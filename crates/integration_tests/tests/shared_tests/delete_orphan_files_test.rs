// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Integration tests for DeleteOrphanFilesAction.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use iceberg::actions::DeleteOrphanFilesAction;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, TableCreation};
use iceberg_catalog_rest::RestCatalogBuilder;
use parquet::file::properties::WriterProperties;

use crate::get_shared_containers;
use crate::shared_tests::{random_ns, test_schema};

/// Helper to create a data file and commit it to the table
async fn write_and_commit_data(
    table: &mut iceberg::table::Table,
    rest_catalog: &impl Catalog,
    suffix: &str,
) -> Vec<String> {
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("test_{}", suffix),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();

    let col1 = StringArray::from(vec![Some("foo"), Some("bar")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();

    data_file_writer.write(batch).await.unwrap();
    let data_files = data_file_writer.close().await.unwrap();

    let file_paths: Vec<String> = data_files.iter().map(|f| f.file_path().to_string()).collect();

    let tx = Transaction::new(table);
    let append_action = tx.fast_append().add_data_files(data_files);
    let tx = append_action.apply(tx).unwrap();
    *table = tx.commit(rest_catalog).await.unwrap();

    file_paths
}

#[tokio::test]
async fn test_delete_orphan_files_dry_run() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("orphan_test_dry_run".to_string())
        .schema(schema.clone())
        .build();

    let mut table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Write some data to create valid table files
    let valid_data_files = write_and_commit_data(&mut table, &rest_catalog, "valid").await;

    // Create orphan files (files that are not referenced by any snapshot)
    let table_location = table.metadata().location();
    let orphan_data_file = format!("{}/data/orphan_data_file.parquet", table_location);
    let orphan_metadata_file = format!("{}/metadata/orphan_metadata.json", table_location);

    table
        .file_io()
        .new_output(&orphan_data_file)
        .unwrap()
        .write("orphan data content".into())
        .await
        .unwrap();

    table
        .file_io()
        .new_output(&orphan_metadata_file)
        .unwrap()
        .write("orphan metadata content".into())
        .await
        .unwrap();

    // Wait a bit to ensure files have older timestamps
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Reload table to get fresh state
    let table = rest_catalog
        .load_table(&iceberg::TableIdent::new(ns.name().clone(), "orphan_test_dry_run".to_string()))
        .await
        .unwrap();

    // Run DeleteOrphanFilesAction in dry_run mode
    // Use older_than_ms far in the future to catch all files regardless of timestamp
    let future_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 1000 * 60 * 60; // 1 hour in the future

    let result = DeleteOrphanFilesAction::new(table.clone())
        .older_than_ms(future_timestamp)
        .dry_run(true)
        .execute()
        .await
        .unwrap();

    // Verify dry run found the orphan files but didn't delete them
    assert!(
        result.deleted_files.is_empty(),
        "Dry run should not delete files"
    );

    let orphan_set: HashSet<String> = result.orphan_files.into_iter().collect();
    assert!(
        orphan_set.contains(&orphan_data_file),
        "Should find orphan data file. Found: {:?}",
        orphan_set
    );
    assert!(
        orphan_set.contains(&orphan_metadata_file),
        "Should find orphan metadata file. Found: {:?}",
        orphan_set
    );

    // Verify valid files are NOT in the orphan list
    for valid_file in &valid_data_files {
        assert!(
            !orphan_set.contains(valid_file),
            "Valid data file should not be marked as orphan: {}",
            valid_file
        );
    }

    // Verify orphan files still exist after dry run
    assert!(
        table.file_io().exists(&orphan_data_file).await.unwrap(),
        "Orphan data file should still exist after dry run"
    );
    assert!(
        table.file_io().exists(&orphan_metadata_file).await.unwrap(),
        "Orphan metadata file should still exist after dry run"
    );
}

#[tokio::test]
async fn test_delete_orphan_files_actual_delete() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("orphan_test_delete".to_string())
        .schema(schema.clone())
        .build();

    let mut table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Write some data to create valid table files
    let valid_data_files = write_and_commit_data(&mut table, &rest_catalog, "valid").await;

    // Create orphan files
    let table_location = table.metadata().location();
    let orphan_file1 = format!("{}/data/orphan1.parquet", table_location);
    let orphan_file2 = format!("{}/data/orphan2.parquet", table_location);

    table
        .file_io()
        .new_output(&orphan_file1)
        .unwrap()
        .write("orphan1 content".into())
        .await
        .unwrap();

    table
        .file_io()
        .new_output(&orphan_file2)
        .unwrap()
        .write("orphan2 content".into())
        .await
        .unwrap();

    // Wait a bit
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Reload table
    let table = rest_catalog
        .load_table(&iceberg::TableIdent::new(ns.name().clone(), "orphan_test_delete".to_string()))
        .await
        .unwrap();

    // Run DeleteOrphanFilesAction (not dry run)
    let future_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 1000 * 60 * 60;

    let result = DeleteOrphanFilesAction::new(table.clone())
        .older_than_ms(future_timestamp)
        .dry_run(false)
        .execute()
        .await
        .unwrap();

    // Verify orphan files were deleted
    let deleted_set: HashSet<String> = result.deleted_files.into_iter().collect();
    assert!(
        deleted_set.contains(&orphan_file1),
        "Should have deleted orphan1. Deleted: {:?}",
        deleted_set
    );
    assert!(
        deleted_set.contains(&orphan_file2),
        "Should have deleted orphan2. Deleted: {:?}",
        deleted_set
    );

    // Verify valid files were NOT deleted
    for valid_file in &valid_data_files {
        assert!(
            !deleted_set.contains(valid_file),
            "Valid data file should not be deleted: {}",
            valid_file
        );
    }

    // Verify orphan files no longer exist
    assert!(
        !table.file_io().exists(&orphan_file1).await.unwrap(),
        "Orphan file 1 should have been deleted"
    );
    assert!(
        !table.file_io().exists(&orphan_file2).await.unwrap(),
        "Orphan file 2 should have been deleted"
    );

    // Verify valid data files still exist
    for valid_file in &valid_data_files {
        assert!(
            table.file_io().exists(valid_file).await.unwrap(),
            "Valid data file should still exist: {}",
            valid_file
        );
    }
}

#[tokio::test]
async fn test_delete_orphan_files_respects_older_than() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("orphan_test_older_than".to_string())
        .schema(schema.clone())
        .build();

    let mut table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Write some data
    let valid_files = write_and_commit_data(&mut table, &rest_catalog, "data").await;

    // Create an orphan file
    let table_location = table.metadata().location();
    let orphan_file = format!("{}/data/new_orphan.parquet", table_location);

    table
        .file_io()
        .new_output(&orphan_file)
        .unwrap()
        .write("new orphan content".into())
        .await
        .unwrap();

    // Reload table
    let table = rest_catalog
        .load_table(&iceberg::TableIdent::new(
            ns.name().clone(),
            "orphan_test_older_than".to_string(),
        ))
        .await
        .unwrap();

    // Run with older_than set to a timestamp in the past (before the file was created)
    // This should NOT find the orphan file as it's too new
    let past_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        - 1000 * 60 * 60; // 1 hour in the past

    let result = DeleteOrphanFilesAction::new(table.clone())
        .older_than_ms(past_timestamp)
        .dry_run(true)
        .execute()
        .await
        .unwrap();

    // The orphan file should NOT be found because it's newer than the threshold
    assert!(
        !result.orphan_files.contains(&orphan_file),
        "New orphan file should not be found with past threshold. Found: {:?}",
        result.orphan_files
    );

    // Now run with a future timestamp - should find the orphan file
    let future_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 1000 * 60 * 60;

    let result = DeleteOrphanFilesAction::new(table.clone())
        .older_than_ms(future_timestamp)
        .dry_run(true)
        .execute()
        .await
        .unwrap();

    assert!(
        result.orphan_files.contains(&orphan_file),
        "Orphan file should be found with future threshold. Found: {:?}",
        result.orphan_files
    );

    // Also verify that valid committed files are NOT in the orphan list
    for valid_file in &valid_files {
        assert!(
            !result.orphan_files.contains(valid_file),
            "Valid committed file should not be orphan: {}",
            valid_file
        );
    }
}

#[tokio::test]
async fn test_delete_orphan_files_preserves_metadata() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("orphan_test_metadata".to_string())
        .schema(schema.clone())
        .build();

    let mut table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Write data multiple times to create multiple snapshots and manifests
    let mut all_data_files: Vec<String> = Vec::new();
    all_data_files.extend(write_and_commit_data(&mut table, &rest_catalog, "snap1").await);
    all_data_files.extend(write_and_commit_data(&mut table, &rest_catalog, "snap2").await);
    all_data_files.extend(write_and_commit_data(&mut table, &rest_catalog, "snap3").await);

    // Reload table to get all metadata
    let table = rest_catalog
        .load_table(&iceberg::TableIdent::new(
            ns.name().clone(),
            "orphan_test_metadata".to_string(),
        ))
        .await
        .unwrap();

    // Collect all metadata file paths that should be preserved
    let mut expected_preserved: HashSet<String> = HashSet::new();

    // version-hint.text
    expected_preserved.insert(format!(
        "{}/metadata/version-hint.text",
        table.metadata().location()
    ));

    // Current metadata file
    if let Some(loc) = table.metadata_location() {
        expected_preserved.insert(loc.to_string());
    }

    // All historical metadata files from metadata log
    for log_entry in table.metadata().metadata_log() {
        expected_preserved.insert(log_entry.metadata_file.clone());
    }

    // All manifest lists and manifests from all snapshots
    for snapshot in table.metadata().snapshots() {
        // Manifest list file
        expected_preserved.insert(snapshot.manifest_list().to_string());
    }

    // Run delete orphan files
    let future_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 1000 * 60 * 60;

    let result = DeleteOrphanFilesAction::new(table.clone())
        .older_than_ms(future_timestamp)
        .dry_run(true)
        .execute()
        .await
        .unwrap();

    // Verify that none of the "expected preserved" files are in the orphan list
    let orphan_set: HashSet<String> = result.orphan_files.into_iter().collect();
    for preserved_file in &expected_preserved {
        assert!(
            !orphan_set.contains(preserved_file),
            "Metadata file should not be marked as orphan: {}. Orphans: {:?}",
            preserved_file,
            orphan_set
        );
    }

    // Additional check: manifest files from snapshots should NOT be orphans
    // We need to read manifest list to get manifest files
    for snapshot in table.metadata().snapshots() {
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        for manifest in manifest_list.entries() {
            assert!(
                !orphan_set.contains(&manifest.manifest_path),
                "Manifest file should not be marked as orphan: {}",
                manifest.manifest_path
            );
        }
    }

    // Also verify that data files are NOT orphans
    for data_file in &all_data_files {
        assert!(
            !orphan_set.contains(data_file),
            "Data file should not be marked as orphan: {}",
            data_file
        );
    }
}

/// Helper to write data files WITHOUT committing them (simulates failed write)
async fn write_data_without_commit(
    table: &iceberg::table::Table,
    suffix: &str,
) -> Vec<String> {
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("uncommitted_{}", suffix),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();

    let col1 = StringArray::from(vec![Some("uncommitted_foo"), Some("uncommitted_bar")]);
    let col2 = Int32Array::from(vec![Some(100), Some(200)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();

    data_file_writer.write(batch).await.unwrap();
    let data_files = data_file_writer.close().await.unwrap();

    // Return file paths but DO NOT COMMIT - simulating a failed write
    data_files.iter().map(|f| f.file_path().to_string()).collect()
}

/// Test: Orphan files from uncommitted writes (simulates write failure)
/// Scenario: Data files are written to storage but the commit/transaction fails
#[tokio::test]
async fn test_delete_orphan_files_from_uncommitted_writes() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("orphan_uncommitted_test".to_string())
        .schema(schema.clone())
        .build();

    let mut table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Step 1: Write and commit valid data
    let valid_files = write_and_commit_data(&mut table, &rest_catalog, "valid").await;

    // Step 2: Write data files but DO NOT commit (simulates failed write)
    let uncommitted_files = write_data_without_commit(&table, "failed_write").await;

    // Verify uncommitted files exist on storage
    for file_path in &uncommitted_files {
        assert!(
            table.file_io().new_input(file_path).unwrap().read().await.is_ok(),
            "Uncommitted file should exist: {}",
            file_path
        );
    }

    // Step 3: Run delete orphan files with future timestamp
    let future_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 1000 * 60 * 60; // 1 hour in future

    let result = DeleteOrphanFilesAction::new(table.clone())
        .older_than_ms(future_timestamp)
        .dry_run(false)
        .execute()
        .await
        .unwrap();

    // When dry_run=false, deleted_files contains what was deleted
    let deleted_set: HashSet<String> = result.deleted_files.iter().cloned().collect();
    
    // Verify: uncommitted files should be deleted
    for uncommitted_file in &uncommitted_files {
        assert!(
            deleted_set.contains(uncommitted_file),
            "Uncommitted file should be identified and deleted as orphan: {}",
            uncommitted_file
        );
    }

    // Verify: valid committed files should NOT be deleted
    for valid_file in &valid_files {
        assert!(
            !deleted_set.contains(valid_file),
            "Valid committed file should not be deleted: {}",
            valid_file
        );
    }

    // Verify: uncommitted files should be actually deleted from storage
    for file_path in &uncommitted_files {
        assert!(
            table.file_io().new_input(file_path).unwrap().read().await.is_err(),
            "Uncommitted file should have been deleted: {}",
            file_path
        );
    }

    // Verify: valid files still exist
    for file_path in &valid_files {
        assert!(
            table.file_io().new_input(file_path).unwrap().read().await.is_ok(),
            "Valid file should still exist: {}",
            file_path
        );
    }
}

/// Test: Orphan manifest/manifest-list files after expire_snapshots
/// Scenario: Create multiple snapshots, then expire old ones
/// The old manifest-list files should become orphans (but data files remain reachable
/// because fast_append snapshots accumulate data file references)
#[tokio::test]
async fn test_delete_orphan_files_after_expire_snapshots() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("orphan_expire_test".to_string())
        .schema(schema.clone())
        .build();

    let mut table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Step 1: Create multiple snapshots with data files
    let mut all_data_files: Vec<String> = Vec::new();
    let mut all_manifest_lists: Vec<String> = Vec::new();

    for i in 0..3 {
        let files = write_and_commit_data(&mut table, &rest_catalog, &format!("batch_{}", i)).await;
        all_data_files.extend(files);

        // Record the manifest list for this snapshot
        if let Some(snap) = table.metadata().current_snapshot() {
            all_manifest_lists.push(snap.manifest_list().to_string());
        }
    }

    // Record snapshot count and manifest lists before expiration
    let snapshot_count_before = table.metadata().snapshots().count();
    assert_eq!(snapshot_count_before, 3, "Should have exactly 3 snapshots");
    assert_eq!(all_manifest_lists.len(), 3, "Should have 3 manifest lists");

    // Keep the last manifest list (from current snapshot)
    let last_manifest_list = all_manifest_lists.last().unwrap().clone();
    let expired_manifest_lists: Vec<String> = all_manifest_lists[..2].to_vec();

    // Step 2: Expire old snapshots, keeping only the latest
    let now = chrono::Utc::now().timestamp_millis();
    let tx = Transaction::new(&table);
    let expire_action = tx.expire_snapshot().retain_last(1).expire_older_than(now);
    let tx = expire_action.apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    // Verify only 1 snapshot remains
    let snapshot_count_after = table.metadata().snapshots().count();
    assert_eq!(snapshot_count_after, 1, "Should have only 1 snapshot after expiration");

    // Step 3: Run delete orphan files
    let future_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 1000 * 60 * 60;

    let result = DeleteOrphanFilesAction::new(table.clone())
        .older_than_ms(future_timestamp)
        .dry_run(true)
        .execute()
        .await
        .unwrap();

    let orphan_set: HashSet<String> = result.orphan_files.iter().cloned().collect();

    // The current snapshot's manifest list should NOT be orphan
    assert!(
        !orphan_set.contains(&last_manifest_list),
        "Current manifest list should not be orphan: {}",
        last_manifest_list
    );

    // The expired snapshots' manifest lists SHOULD be orphans
    // (expire_snapshots removes snapshot refs, making their manifest-lists unreachable)
    for expired_ml in &expired_manifest_lists {
        assert!(
            orphan_set.contains(expired_ml),
            "Expired manifest list should be orphan: {}. Orphans found: {:?}",
            expired_ml,
            orphan_set.iter().filter(|p| p.contains("snap")).collect::<Vec<_>>()
        );
    }

    // NOTE: Data files from expired snapshots are NOT orphans in this test because:
    // - fast_append creates snapshots that accumulate all previous manifest references
    // - The current snapshot still references all previous manifests (and their data files)
    // - To have orphan data files, we would need to use RewriteDataFilesAction
    //   which replaces old data files with new compacted files
    //
    // This test verifies that expired manifest-list files become orphans,
    // which is still a valid orphan file cleanup scenario.
    for data_file in &all_data_files {
        assert!(
            !orphan_set.contains(data_file),
            "Data file should NOT be orphan (still referenced by current snapshot): {}",
            data_file
        );
    }
}

/// Test: Orphan manifest files after RewriteDataFiles operation
/// Scenario: Write data, rewrite (compact) it, expire old snapshot
/// The old manifest-list and manifest files should become orphans.
///
/// NOTE: Original data files are NOT orphans after rewrite + expire because:
/// - rewrite_files creates DELETE manifest entries for removed files
/// - These DELETED entries still reference the old data files in the new snapshot's manifest
/// - This is correct Iceberg behavior for time-travel and audit purposes
/// - To truly orphan old data files, a manifest compaction would be needed to squash DELETED entries
#[tokio::test]
async fn test_delete_orphan_files_after_rewrite() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("orphan_rewrite_test".to_string())
        .schema(schema.clone())
        .build();

    let mut table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Step 1: Write initial data
    let initial_files = write_and_commit_data(&mut table, &rest_catalog, "initial").await;

    // Record the first snapshot's manifest-list
    let first_snapshot = table.metadata().current_snapshot().unwrap();
    let first_manifest_list = first_snapshot.manifest_list().to_string();

    // Also load and record the first manifest file path
    let first_manifest_list_content = first_snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    let first_manifest_paths: Vec<String> = first_manifest_list_content
        .entries()
        .iter()
        .map(|e| e.manifest_path.clone())
        .collect();
    
    // Verify we have initial files and manifest paths
    assert!(!initial_files.is_empty(), "Should have initial files");
    assert!(!first_manifest_paths.is_empty(), "Should have first manifest paths");

    // Step 2: Simulate rewrite by:
    // - Writing new "compacted" data files
    // - Using rewrite operation to replace old files with new files
    let schema_arc: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "rewritten".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();

    let col1 = StringArray::from(vec![Some("rewritten_foo"), Some("rewritten_bar")]);
    let col2 = Int32Array::from(vec![Some(100), Some(200)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false)]);
    let batch = RecordBatch::try_new(schema_arc.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();

    data_file_writer.write(batch).await.unwrap();
    let rewritten_data_files = data_file_writer.close().await.unwrap();
    let rewritten_file_paths: Vec<String> = rewritten_data_files
        .iter()
        .map(|f| f.file_path().to_string())
        .collect();

    // Load original data files to get DataFile objects for removal
    let original_snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = original_snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();

    let mut original_data_files: Vec<iceberg::spec::DataFile> = Vec::new();
    for manifest_entry in manifest_list.entries() {
        let manifest = manifest_entry
            .load_manifest(table.file_io())
            .await
            .unwrap();
        for entry in manifest.entries() {
            if entry.is_alive() {
                original_data_files.push(entry.data_file().clone());
            }
        }
    }

    // Commit the rewrite: remove old files, add new files
    let tx = Transaction::new(&table);
    let rewrite_action = tx
        .rewrite_files()
        .delete_files(original_data_files)
        .add_data_files(rewritten_data_files);
    let tx = rewrite_action.apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    // Verify we now have 2 snapshots
    assert_eq!(table.metadata().snapshots().count(), 2, "Should have 2 snapshots");

    // Step 3: Expire the old snapshot
    let now = chrono::Utc::now().timestamp_millis();
    let tx = Transaction::new(&table);
    let expire_action = tx.expire_snapshot().retain_last(1).expire_older_than(now);
    let tx = expire_action.apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    assert_eq!(table.metadata().snapshots().count(), 1, "Should have 1 snapshot after expiration");

    // Step 4: Run delete orphan files
    let future_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 1000 * 60 * 60;

    let result = DeleteOrphanFilesAction::new(table.clone())
        .older_than_ms(future_timestamp)
        .dry_run(true)
        .execute()
        .await
        .unwrap();

    let orphan_set: HashSet<String> = result.orphan_files.iter().cloned().collect();

    // The rewritten files (from current snapshot) should NOT be orphans
    for rewritten_file in &rewritten_file_paths {
        assert!(
            !orphan_set.contains(rewritten_file),
            "Rewritten file should not be orphan: {}",
            rewritten_file
        );
    }

    // The first snapshot's manifest-list SHOULD be orphan after expire
    assert!(
        orphan_set.contains(&first_manifest_list),
        "First snapshot's manifest-list should be orphan after expire: {}. Orphans: {:?}",
        first_manifest_list,
        orphan_set
    );

    // The first snapshot's manifest files SHOULD also be orphans
    // (rewrite creates a new manifest, so old manifests are no longer referenced)
    for first_manifest in &first_manifest_paths {
        assert!(
            orphan_set.contains(first_manifest),
            "First snapshot's manifest should be orphan after expire: {}. Orphans: {:?}",
            first_manifest,
            orphan_set
        );
    }

    // NOTE: Original data files are NOT orphans after rewrite + expire!
    // This is because rewrite_files creates DELETE manifest entries for removed files,
    // which still reference the old data files in the new snapshot's manifest.
    // This is correct Iceberg behavior for time-travel and audit purposes.
    // To verify this, we check that initial_files are NOT in orphan_set.
    for initial_file in &initial_files {
        assert!(
            !orphan_set.contains(initial_file),
            "Original data file should NOT be orphan (still referenced by DELETE manifest entry): {}",
            initial_file
        );
    }

    // Verify there are orphans (at least the old manifest-list and manifest)
    assert!(
        !orphan_set.is_empty(),
        "Should have found some orphan files after rewrite + expire"
    );

    // The current snapshot's manifest-list should NOT be orphan
    let current_manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .manifest_list()
        .to_string();
    assert!(
        !orphan_set.contains(&current_manifest_list),
        "Current manifest-list should not be orphan: {}",
        current_manifest_list
    );
}


