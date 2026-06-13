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

//! Remove dangling delete files action.
//!
//! Removes position delete files, deletion vectors, and equality delete files
//! from manifests when they no longer apply to any live data file in the
//! current snapshot. This reduces metadata overhead and storage consumption
//! for tables with high CDC throughput.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::spec::{DataContentType, DataFile, MAIN_BRANCH, Struct};
use crate::transaction::{ApplyTransactionAction, Transaction};
use crate::utils::{DEFAULT_LOAD_CONCURRENCY_LIMIT, load_manifests};
use crate::{Catalog, Error, ErrorKind, Result, TableIdent};

/// Action to remove dangling delete files from a table.
///
/// Three categories of dangling deletes are handled:
///
/// - **Position deletes / deletion vectors**: removed when their
///   `referenced_data_file` no longer exists in the current snapshot.
/// - **Equality deletes**: removed when their sequence number is ≤ the
///   minimum data file sequence number in their partition (Iceberg V2 spec:
///   an equality delete at seq S only applies to data files with seq < S).
///
/// # Example
///
/// ```ignore
/// let removed = RemoveDanglingDeleteFilesAction::new(catalog, table_ident)
///     .to_branch("main")
///     .execute()
///     .await?;
/// println!("Removed {} dangling delete files", removed);
/// ```
pub struct RemoveDanglingDeleteFilesAction {
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    to_branch: String,
}

impl RemoveDanglingDeleteFilesAction {
    /// Creates a new action for the given catalog and table.
    pub fn new(catalog: Arc<dyn Catalog>, table_ident: TableIdent) -> Self {
        Self {
            catalog,
            table_ident,
            to_branch: MAIN_BRANCH.to_string(),
        }
    }

    /// Sets the branch to operate on.
    pub fn to_branch(mut self, branch: impl Into<String>) -> Self {
        self.to_branch = branch.into();
        self
    }

    /// Executes the action, returning the number of dangling delete files removed.
    pub async fn execute(self) -> Result<usize> {
        let table = self.catalog.load_table(&self.table_ident).await?;
        let Some(snapshot) = table.metadata().snapshot_for_ref(&self.to_branch) else {
            return Ok(0);
        };

        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await?;

        let mut data_file_paths: HashSet<String> = HashSet::new();
        let mut pos_deletes: Vec<(DataFile, Option<i64>)> = Vec::new();
        let mut eq_deletes: Vec<(DataFile, i64)> = Vec::new();
        let mut partition_min_seq: HashMap<(i32, Struct), i64> = HashMap::new();
        let mut global_min_data_seq: Option<i64> = None;

        let manifest_files: Vec<_> = manifest_list.entries().to_vec();
        let loaded = load_manifests(
            table.file_io(),
            manifest_files,
            DEFAULT_LOAD_CONCURRENCY_LIMIT,
        )
        .await?;

        for (_, manifest) in loaded {
            let (entries, _) = manifest.into_parts();

            for entry in entries {
                if !entry.is_alive() {
                    continue;
                }

                let df = entry.data_file();
                let seq = entry.sequence_number();

                match entry.content_type() {
                    DataContentType::Data => {
                        data_file_paths.insert(df.file_path().to_string());
                        if let Some(s) = seq {
                            let key = (df.partition_spec_id(), df.partition().clone());
                            partition_min_seq
                                .entry(key)
                                .and_modify(|min| *min = (*min).min(s))
                                .or_insert(s);
                            global_min_data_seq = Some(global_min_data_seq.map_or(s, |g| g.min(s)));
                        }
                    }
                    DataContentType::PositionDeletes => {
                        pos_deletes.push((df.clone(), seq));
                    }
                    DataContentType::EqualityDeletes => {
                        if let Some(s) = seq {
                            eq_deletes.push((df.clone(), s));
                        }
                    }
                }
            }
        }

        let mut dangling: Vec<DataFile> = Vec::new();

        dangling.extend(
            pos_deletes
                .into_iter()
                .filter(|(df, df_seq)| {
                    if let Some(ref_path) = df.referenced_data_file() {
                        // Path-based: dangling if the referenced data file no longer exists
                        !data_file_paths.contains(&ref_path)
                    } else if let Some(s) = df_seq {
                        // Sequence-based: dangling if seq < min_data_seq in this partition
                        let key = (df.partition_spec_id(), df.partition().clone());
                        partition_min_seq
                            .get(&key)
                            .map_or(true, |&min_seq| *s < min_seq)
                    } else {
                        // No referenced_data_file and no sequence number — cannot determine
                        false
                    }
                })
                .map(|(df, _)| df),
        );

        dangling.extend(
            eq_deletes
                .into_iter()
                .filter(|(df, seq)| {
                    if df.partition().fields().is_empty() {
                        // Unpartitioned equality deletes are global — they apply to all
                        // data files regardless of partition. Only remove if seq is <=
                        // the global minimum data sequence number.
                        global_min_data_seq.map_or(true, |g| *seq <= g)
                    } else {
                        let key = (df.partition_spec_id(), df.partition().clone());
                        partition_min_seq
                            .get(&key)
                            .map_or(true, |&min_seq| *seq <= min_seq)
                    }
                })
                .map(|(df, _)| df),
        );

        let mut seen: HashSet<String> = HashSet::new();
        dangling.retain(|df| seen.insert(df.file_path().to_string()));

        if dangling.is_empty() {
            return Ok(0);
        }

        let dangling_count = dangling.len();
        let txn = Transaction::new(&table);
        let branch = self.to_branch.clone();
        let action = txn
            .rewrite_files()
            .delete_files(dangling)
            .set_target_branch(branch);

        let txn = action.apply(txn).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to build rewrite action: {e}"),
            )
        })?;

        txn.commit(self.catalog.as_ref()).await?;

        Ok(dangling_count)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::RemoveDanglingDeleteFilesAction;
    use crate::catalog::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use crate::catalog::{Catalog, CatalogBuilder};
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, NestedField,
        PrimitiveType, Schema, Struct, Transform, Type, UnboundPartitionSpec,
    };
    use crate::table::Table;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{NamespaceIdent, TableCreation, TableIdent};

    fn simple_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap()
    }

    async fn create_test_table(
        catalog: &Arc<dyn Catalog>,
        table_name: &str,
    ) -> (TableIdent, Table) {
        let ns = NamespaceIdent::new("test_ns".into());
        catalog.create_namespace(&ns, HashMap::new()).await.ok();

        let table_ident = TableIdent::new(ns, table_name.to_string());
        let table_creation = TableCreation::builder()
            .name(table_ident.name().into())
            .schema(simple_schema())
            .build();

        catalog
            .create_table(&table_ident.namespace, table_creation)
            .await
            .unwrap();

        let table = catalog.load_table(&table_ident).await.unwrap();
        (table_ident, table)
    }

    async fn commit_data_file(catalog: &Arc<dyn Catalog>, table: &Table, file_path: &str) -> Table {
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .build()
            .unwrap();

        let txn = Transaction::new(table);
        let action = txn
            .rewrite_files()
            .add_data_files(vec![data_file])
            .set_target_branch(MAIN_BRANCH.to_string());
        let txn = action.apply(txn).unwrap();
        txn.commit(catalog.as_ref()).await.unwrap()
    }

    /// Creates a table partitioned by `id` (identity transform), so that data
    /// and delete files can be placed into distinct partitions.
    async fn create_partitioned_table(
        catalog: &Arc<dyn Catalog>,
        table_name: &str,
    ) -> (TableIdent, Table) {
        let ns = NamespaceIdent::new("test_ns".into());
        catalog.create_namespace(&ns, HashMap::new()).await.ok();

        let partition_spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(1, "id".to_string(), Transform::Identity)
            .unwrap()
            .build();

        let table_ident = TableIdent::new(ns, table_name.to_string());
        let table_creation = TableCreation::builder()
            .name(table_ident.name().into())
            .schema(simple_schema())
            .partition_spec(partition_spec)
            .build();

        catalog
            .create_table(&table_ident.namespace, table_creation)
            .await
            .unwrap();

        let table = catalog.load_table(&table_ident).await.unwrap();
        (table_ident, table)
    }

    /// Commits a single file (data or delete) into the given identity partition.
    async fn commit_partitioned_file(
        catalog: &Arc<dyn Catalog>,
        table: &Table,
        file_path: &str,
        content: DataContentType,
        partition_val: i32,
    ) -> Table {
        let spec_id = table.metadata().default_partition_spec_id();
        let mut builder = DataFileBuilder::default();
        builder
            .content(content)
            .file_path(file_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([Some(Literal::int(partition_val))]));
        if content == DataContentType::EqualityDeletes {
            builder.equality_ids(Some(vec![1]));
        }
        let data_file = builder.build().unwrap();

        let txn = Transaction::new(table);
        let action = txn
            .rewrite_files()
            .add_data_files(vec![data_file])
            .set_target_branch(MAIN_BRANCH.to_string());
        let txn = action.apply(txn).unwrap();
        txn.commit(catalog.as_ref()).await.unwrap()
    }

    /// Returns whether a live delete-file entry with the given path exists in
    /// the table's current snapshot.
    async fn delete_file_is_live(table: &Table, file_path: &str) -> bool {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        for mf in manifest_list.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() && entry.data_file().file_path() == file_path {
                    return true;
                }
            }
        }
        false
    }

    async fn build_catalog() -> Arc<dyn Catalog> {
        let warehouse = "memory://test/";
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.to_string())]),
            )
            .await
            .unwrap();
        Arc::new(catalog)
    }

    #[tokio::test]
    async fn test_remove_dangling_delete_files() {
        let catalog = build_catalog().await;
        let (table_ident, table) = create_test_table(&catalog, "test_dangling").await;
        let table = commit_data_file(&catalog, &table, "memory://test/data-1.parquet").await;

        let pos_delete = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("memory://test/pos-del-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .referenced_data_file(Some("memory://test/nonexistent.parquet".to_string()))
            .build()
            .unwrap();

        let txn = Transaction::new(&table);
        let action = txn
            .rewrite_files()
            .add_data_files(vec![pos_delete])
            .set_target_branch(MAIN_BRANCH.to_string());
        let txn = action.apply(txn).unwrap();
        txn.commit(catalog.as_ref()).await.unwrap();

        let removed = RemoveDanglingDeleteFilesAction::new(catalog.clone(), table_ident)
            .execute()
            .await
            .unwrap();
        assert_eq!(removed, 1);
    }

    #[tokio::test]
    async fn test_remove_dangling_delete_files_none_when_referenced() {
        let catalog = build_catalog().await;
        let (table_ident, table) = create_test_table(&catalog, "test_referenced").await;

        let data_file_path = "memory://test/data-1.parquet";
        let table = commit_data_file(&catalog, &table, data_file_path).await;

        let pos_delete = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("memory://test/pos-del-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .referenced_data_file(Some(data_file_path.to_string()))
            .build()
            .unwrap();

        let txn = Transaction::new(&table);
        let action = txn
            .rewrite_files()
            .add_data_files(vec![pos_delete])
            .set_target_branch(MAIN_BRANCH.to_string());
        let txn = action.apply(txn).unwrap();
        txn.commit(catalog.as_ref()).await.unwrap();

        let removed = RemoveDanglingDeleteFilesAction::new(catalog.clone(), table_ident)
            .execute()
            .await
            .unwrap();
        assert_eq!(removed, 0);
    }

    #[tokio::test]
    async fn test_remove_dangling_equality_delete() {
        let catalog = build_catalog().await;
        let (table_ident, table) = create_test_table(&catalog, "test_eq_dangling").await;

        // Commit equality delete first → gets lower sequence number
        let eq_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("memory://test/eq-del-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .equality_ids(Some(vec![1]))
            .build()
            .unwrap();

        let txn = Transaction::new(&table);
        let action = txn
            .rewrite_files()
            .add_data_files(vec![eq_delete])
            .set_target_branch(MAIN_BRANCH.to_string());
        let txn = action.apply(txn).unwrap();
        let table = txn.commit(catalog.as_ref()).await.unwrap();

        // Commit data file second → gets higher sequence number.
        // The equality delete at lower seq applies to files with seq < its seq.
        // All data files have higher seq, so the equality delete is dangling.
        let _table = commit_data_file(&catalog, &table, "memory://test/data-1.parquet").await;

        let removed = RemoveDanglingDeleteFilesAction::new(catalog.clone(), table_ident)
            .execute()
            .await
            .unwrap();
        assert_eq!(removed, 1);
    }

    #[tokio::test]
    async fn test_keep_equality_delete_when_data_has_lower_seq() {
        let catalog = build_catalog().await;
        let (table_ident, table) = create_test_table(&catalog, "test_eq_kept").await;

        // Commit data file first → gets lower sequence number
        let table = commit_data_file(&catalog, &table, "memory://test/data-1.parquet").await;

        // Commit equality delete second → gets higher sequence number.
        // The equality delete at higher seq applies to the data file at lower seq.
        let eq_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("memory://test/eq-del-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .equality_ids(Some(vec![1]))
            .build()
            .unwrap();

        let txn = Transaction::new(&table);
        let action = txn
            .rewrite_files()
            .add_data_files(vec![eq_delete])
            .set_target_branch(MAIN_BRANCH.to_string());
        let txn = action.apply(txn).unwrap();
        txn.commit(catalog.as_ref()).await.unwrap();

        let removed = RemoveDanglingDeleteFilesAction::new(catalog.clone(), table_ident)
            .execute()
            .await
            .unwrap();
        assert_eq!(removed, 0);
    }

    #[tokio::test]
    async fn test_position_delete_without_ref_removed_by_seq() {
        let catalog = build_catalog().await;
        let (table_ident, table) = create_test_table(&catalog, "test_pos_seq").await;

        let pos_delete = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("memory://test/pos-del-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .build()
            .unwrap();

        let txn = Transaction::new(&table);
        let action = txn
            .rewrite_files()
            .add_data_files(vec![pos_delete])
            .set_target_branch(MAIN_BRANCH.to_string());
        let txn = action.apply(txn).unwrap();
        let table = txn.commit(catalog.as_ref()).await.unwrap();

        let _ = commit_data_file(&catalog, &table, "memory://test/data-1.parquet").await;

        let removed = RemoveDanglingDeleteFilesAction::new(catalog.clone(), table_ident)
            .execute()
            .await
            .unwrap();
        assert_eq!(removed, 1);
    }

    #[tokio::test]
    async fn test_position_delete_without_ref_kept_by_seq() {
        let catalog = build_catalog().await;
        let (table_ident, table) = create_test_table(&catalog, "test_pos_seq_kept").await;

        let table = commit_data_file(&catalog, &table, "memory://test/data-1.parquet").await;

        let pos_delete = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("memory://test/pos-del-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .build()
            .unwrap();

        let txn = Transaction::new(&table);
        let action = txn
            .rewrite_files()
            .add_data_files(vec![pos_delete])
            .set_target_branch(MAIN_BRANCH.to_string());
        let txn = action.apply(txn).unwrap();
        txn.commit(catalog.as_ref()).await.unwrap();

        let removed = RemoveDanglingDeleteFilesAction::new(catalog.clone(), table_ident)
            .execute()
            .await
            .unwrap();
        assert_eq!(removed, 0);
    }

    #[tokio::test]
    async fn test_remove_global_equality_delete() {
        let catalog = build_catalog().await;
        let (table_ident, table) = create_test_table(&catalog, "test_global_eq").await;

        let eq_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("memory://test/eq-del-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .equality_ids(Some(vec![1]))
            .build()
            .unwrap();

        let txn = Transaction::new(&table);
        let action = txn
            .rewrite_files()
            .add_data_files(vec![eq_delete])
            .set_target_branch(MAIN_BRANCH.to_string());
        let txn = action.apply(txn).unwrap();
        let table = txn.commit(catalog.as_ref()).await.unwrap();

        let _ = commit_data_file(&catalog, &table, "memory://test/data-1.parquet").await;

        let removed = RemoveDanglingDeleteFilesAction::new(catalog.clone(), table_ident)
            .execute()
            .await
            .unwrap();
        assert_eq!(removed, 1);
    }

    #[tokio::test]
    async fn test_partial_delete_manifest_removes_dangling_only() {
        let catalog = build_catalog().await;
        let (table_ident, table) = create_test_table(&catalog, "test_partial").await;

        let data_file_path = "memory://test/data-1.parquet";
        let table = commit_data_file(&catalog, &table, data_file_path).await;

        let dangling = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("memory://test/dangling.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .referenced_data_file(Some("memory://test/nonexistent.parquet".to_string()))
            .build()
            .unwrap();

        let kept = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("memory://test/kept.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .referenced_data_file(Some(data_file_path.to_string()))
            .build()
            .unwrap();

        let txn = Transaction::new(&table);
        let action = txn
            .rewrite_files()
            .add_data_files(vec![dangling, kept])
            .set_target_branch(MAIN_BRANCH.to_string());
        let txn = action.apply(txn).unwrap();
        txn.commit(catalog.as_ref()).await.unwrap();

        let removed = RemoveDanglingDeleteFilesAction::new(catalog.clone(), table_ident.clone())
            .execute()
            .await
            .unwrap();
        assert_eq!(removed, 1);

        let table = catalog.load_table(&table_ident).await.unwrap();
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        let mut found_kept = false;
        let mut found_dangling = false;
        for mf in manifest_list.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let path = entry.data_file().file_path();
                if path == "memory://test/kept.parquet" {
                    found_kept = true;
                }
                if path == "memory://test/dangling.parquet" {
                    found_dangling = true;
                }
            }
        }
        assert!(found_kept, "kept delete should still be live");
        assert!(!found_dangling, "dangling delete should be removed");
    }

    /// Equality deletes are isolated per partition: a delete is dangling only
    /// when its sequence number is <= the minimum data sequence number *within
    /// its own partition*.
    ///
    /// The commit ordering is chosen so the result diverges from a naive
    /// global-minimum implementation: the dangling delete (`eq-p2`, seq 3)
    /// sits in partition 2 whose min data seq is 4, but the global min data
    /// seq is 1 (from partition 1). Correct per-partition logic removes it
    /// (3 <= 4); a global-min implementation would wrongly keep it (3 > 1).
    /// `eq-p1` is kept under both, proving cross-partition isolation.
    ///
    /// This exercises the partitioned branch of the equality-delete filter,
    /// which the unpartitioned tests never reach.
    #[tokio::test]
    async fn test_partitioned_equality_delete_isolated_per_partition() {
        let catalog = build_catalog().await;
        let (table_ident, table) = create_partitioned_table(&catalog, "test_part_eq").await;

        // seq 1: data in p1 (sets global min data seq to 1, p1 min to 1).
        let table = commit_partitioned_file(
            &catalog,
            &table,
            "memory://test/data-p1.parquet",
            DataContentType::Data,
            1,
        )
        .await;
        // seq 2: eq delete in p1. min_data_seq(p1) = 1, 2 <= 1 is false -> kept.
        let table = commit_partitioned_file(
            &catalog,
            &table,
            "memory://test/eq-p1.parquet",
            DataContentType::EqualityDeletes,
            1,
        )
        .await;
        // seq 3: eq delete in p2 (the discriminating file).
        let table = commit_partitioned_file(
            &catalog,
            &table,
            "memory://test/eq-p2.parquet",
            DataContentType::EqualityDeletes,
            2,
        )
        .await;
        // seq 4: data in p2. min_data_seq(p2) = 4, eq-p2 seq 3 <= 4 -> dangling.
        let _table = commit_partitioned_file(
            &catalog,
            &table,
            "memory://test/data-p2.parquet",
            DataContentType::Data,
            2,
        )
        .await;

        let removed = RemoveDanglingDeleteFilesAction::new(catalog.clone(), table_ident.clone())
            .execute()
            .await
            .unwrap();
        assert_eq!(
            removed, 1,
            "only the partition-2 equality delete is dangling"
        );

        let table = catalog.load_table(&table_ident).await.unwrap();
        assert!(
            delete_file_is_live(&table, "memory://test/eq-p1.parquet").await,
            "partition-1 equality delete should be kept"
        );
        assert!(
            !delete_file_is_live(&table, "memory://test/eq-p2.parquet").await,
            "partition-2 equality delete should be removed"
        );
    }

    /// Position deletes without a `referenced_data_file` fall back to the
    /// sequence-based, per-partition check (dangling when seq < min data seq in
    /// the same partition).
    ///
    /// As with the equality test, ordering is chosen to diverge from a naive
    /// global minimum: the dangling delete (`pos-p2`, seq 3) is in partition 2
    /// (min data seq 4) while the global min is 1. Correct per-partition logic
    /// removes it (3 < 4); a global-min implementation keeps it (3 > 1).
    #[tokio::test]
    async fn test_partitioned_position_delete_isolated_per_partition() {
        let catalog = build_catalog().await;
        let (table_ident, table) = create_partitioned_table(&catalog, "test_part_pos").await;

        // seq 1: data in p1 (global min = 1, p1 min = 1).
        let table = commit_partitioned_file(
            &catalog,
            &table,
            "memory://test/data-p1.parquet",
            DataContentType::Data,
            1,
        )
        .await;
        // seq 2: pos delete in p1. min_data_seq(p1) = 1, 2 < 1 is false -> kept.
        let table = commit_partitioned_file(
            &catalog,
            &table,
            "memory://test/pos-p1.parquet",
            DataContentType::PositionDeletes,
            1,
        )
        .await;
        // seq 3: pos delete in p2 (the discriminating file).
        let table = commit_partitioned_file(
            &catalog,
            &table,
            "memory://test/pos-p2.parquet",
            DataContentType::PositionDeletes,
            2,
        )
        .await;
        // seq 4: data in p2. min_data_seq(p2) = 4, pos-p2 seq 3 < 4 -> dangling.
        let _table = commit_partitioned_file(
            &catalog,
            &table,
            "memory://test/data-p2.parquet",
            DataContentType::Data,
            2,
        )
        .await;

        let removed = RemoveDanglingDeleteFilesAction::new(catalog.clone(), table_ident.clone())
            .execute()
            .await
            .unwrap();
        assert_eq!(
            removed, 1,
            "only the partition-2 position delete is dangling"
        );

        let table = catalog.load_table(&table_ident).await.unwrap();
        assert!(
            delete_file_is_live(&table, "memory://test/pos-p1.parquet").await,
            "partition-1 position delete should be kept"
        );
        assert!(
            !delete_file_is_live(&table, "memory://test/pos-p2.parquet").await,
            "partition-2 position delete should be removed"
        );
    }
}
