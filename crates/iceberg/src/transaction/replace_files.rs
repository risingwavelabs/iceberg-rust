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

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;

use uuid::Uuid;

use super::snapshot::{DefaultManifestProcess, SnapshotProducer, data_file_identity};
use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, ManifestEntry, ManifestFile, ManifestStatus, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::SnapshotProduceOperation;
use crate::transaction::{ActionCommit, TransactionAction};

/// Which snapshot [`Operation`] a file replacement records.
///
/// `rewrite_files` and `overwrite_files` differ only in this value
pub(crate) trait ReplaceFilesMode: Send + Sync + 'static {
    const OPERATION: Operation;
}

/// Files were added and removed without changing table data (compaction,
/// changing file format, relocating files).
pub struct Rewrite;

/// Files were added and removed in a logical overwrite.
pub struct Overwrite;

impl ReplaceFilesMode for Rewrite {
    const OPERATION: Operation = Operation::Replace;
}

impl ReplaceFilesMode for Overwrite {
    const OPERATION: Operation = Operation::Overwrite;
}

/// A blanket `impl<M: ReplaceFilesMode> SnapshotProduceOperation for M` would
/// collide with `impl SnapshotProduceOperation for FastAppendOperation`: the
/// compiler cannot prove `FastAppendOperation` will never implement
/// `ReplaceFilesMode`. This wrapper carries the shared implementation instead.
pub(crate) struct ReplaceFilesOperation<M: ReplaceFilesMode>(PhantomData<M>);

impl<M: ReplaceFilesMode> ReplaceFilesOperation<M> {
    pub(crate) fn new() -> Self {
        Self(PhantomData)
    }
}

impl<M: ReplaceFilesMode> SnapshotProduceOperation for ReplaceFilesOperation<M> {
    fn operation(&self) -> Operation {
        M::OPERATION
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        if snapshot_produce.removed_data_file_identities.is_empty()
            && snapshot_produce.removed_delete_file_identities.is_empty()
        {
            return Ok(vec![]);
        }

        // generate delete manifest entries from removed files
        let snapshot = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch());

        if let Some(snapshot) = snapshot {
            let gen_manifest_entry = |old_entry: &Arc<ManifestEntry>| {
                let mut entry = old_entry.as_ref().clone();
                entry.status = ManifestStatus::Deleted;
                entry
            };

            let manifest_list = snapshot_produce
                .table
                .manifest_list_reader(snapshot)
                .load()
                .await?;

            let mut deleted_entries = Vec::new();

            for manifest_file in manifest_list.entries() {
                let manifest = manifest_file
                    .load_manifest(snapshot_produce.table.file_io())
                    .await?;

                for entry in manifest.entries() {
                    if entry.is_alive()
                        && entry.content_type() == DataContentType::Data
                        && snapshot_produce
                            .removed_data_file_identities
                            .contains(&data_file_identity(entry.data_file()))
                    {
                        deleted_entries.push(gen_manifest_entry(entry));
                    }

                    if entry.is_alive()
                        && (entry.content_type() == DataContentType::PositionDeletes
                            || entry.content_type() == DataContentType::EqualityDeletes)
                        && snapshot_produce
                            .removed_delete_file_identities
                            .contains(&data_file_identity(entry.data_file()))
                    {
                        deleted_entries.push(gen_manifest_entry(entry));
                    }
                }
            }

            Ok(deleted_entries)
        } else {
            Ok(vec![])
        }
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch())
        else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot_produce
            .table
            .manifest_list_reader(snapshot)
            .load()
            .await?;

        if snapshot_produce.removed_data_file_identities.is_empty()
            && snapshot_produce.removed_delete_file_identities.is_empty()
        {
            return Ok(manifest_list.entries().to_vec());
        }

        let mut existing_files = Vec::new();

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(snapshot_produce.table.file_io())
                .await?;

            let found_deleted_files: HashSet<_> = manifest
                .entries()
                .iter()
                .filter_map(|entry| {
                    let identity = data_file_identity(entry.data_file());
                    if entry.is_alive()
                        && (snapshot_produce
                            .removed_data_file_identities
                            .contains(&identity)
                            || snapshot_produce
                                .removed_delete_file_identities
                                .contains(&identity))
                    {
                        Some(identity)
                    } else {
                        None
                    }
                })
                .collect();

            if found_deleted_files.is_empty() {
                existing_files.push(manifest_file.clone());
            } else {
                // Rewrite the manifest file without the deleted data files
                let survives = |entry: &ManifestEntry| {
                    entry.is_alive()
                        && !found_deleted_files.contains(&data_file_identity(entry.data_file()))
                };

                if manifest.entries().iter().any(|entry| survives(entry)) {
                    let mut manifest_writer = snapshot_produce.new_manifest_writer(
                        manifest_file.content,
                        manifest_file.partition_spec_id,
                    )?;

                    for entry in manifest.entries() {
                        // Carry survivors forward as `Existing`: `add_entry` would
                        // restamp them as `Added` under the new snapshot and drop
                        // their file sequence number.
                        if survives(entry) {
                            manifest_writer.add_existing_entry(entry.as_ref().clone())?;
                        }
                    }

                    existing_files.push(manifest_writer.write_manifest_file().await?);
                }
            }
        }

        Ok(existing_files)
    }
}

/// Transaction action that replaces one set of files with another.
///
/// `M` is sealed to [`Rewrite`] and [`Overwrite`] via the [`RewriteFilesAction`] /
/// [`OverwriteFilesAction`] type aliases below; `ReplaceFilesMode` itself stays
/// `pub(crate)` so no other type can be substituted for `M`.
#[allow(private_bounds)]
pub struct ReplaceFilesAction<M: ReplaceFilesMode> {
    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,
    snapshot_id: Option<i64>,
    new_data_file_sequence_number: Option<i64>,
    delete_file_cleanup_min_data_sequence_number: Option<i64>,
    target_branch: Option<String>,
    enable_delete_filter_manager: bool,
    check_file_existence: bool,

    _mode: PhantomData<M>,
}

/// Rewrites files without changing table data — compaction and friends.
pub type RewriteFilesAction = ReplaceFilesAction<Rewrite>;

/// Rewrites files as a logical overwrite.
pub type OverwriteFilesAction = ReplaceFilesAction<Overwrite>;

#[allow(private_bounds)]
impl<M: ReplaceFilesMode> ReplaceFilesAction<M> {
    pub fn new() -> Self {
        Self {
            commit_uuid: None,
            snapshot_properties: HashMap::new(),
            added_data_files: Vec::new(),
            added_delete_files: Vec::new(),
            removed_data_files: Vec::new(),
            removed_delete_files: Vec::new(),
            snapshot_id: None,
            new_data_file_sequence_number: None,
            delete_file_cleanup_min_data_sequence_number: None,
            target_branch: None,
            enable_delete_filter_manager: true,
            check_file_existence: false,
            _mode: PhantomData,
        }
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in data_files {
            match file.content_type() {
                DataContentType::Data => self.added_data_files.push(file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.added_delete_files.push(file)
                }
            }
        }

        self
    }

    /// Add remove files to the snapshot.
    pub fn delete_files(mut self, remove_data_files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in remove_data_files {
            match file.content_type() {
                DataContentType::Data => self.removed_data_files.push(file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.removed_delete_files.push(file)
                }
            }
        }

        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(&mut self, properties: HashMap<String, String>) -> &mut Self {
        self.snapshot_properties = properties;

        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(&mut self, commit_uuid: Uuid) -> &mut Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set snapshot id
    pub fn set_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Enable or disable filtering obsolete delete entries for this snapshot.
    ///
    /// Filtering is enabled by default so replacing a data file also drops
    /// deletion vectors that can no longer apply to any live file.
    pub fn set_enable_delete_filter_manager(mut self, enable_delete_filter_manager: bool) -> Self {
        self.enable_delete_filter_manager = enable_delete_filter_manager;
        self
    }

    pub fn set_target_branch(mut self, target_branch: String) -> Self {
        self.target_branch = Some(target_branch);
        self
    }

    // If the compaction should use the sequence number of the snapshot at compaction start time for
    // new data files, instead of using the sequence number of the newly produced snapshot.
    // This avoids commit conflicts with updates that add newer equality deletes at a higher sequence number.
    pub fn set_new_data_file_sequence_number(mut self, seq: i64) -> Self {
        self.new_data_file_sequence_number = Some(seq);
        self
    }

    /// Set the minimum data sequence used to retire older delete files.
    ///
    /// If omitted, the minimum sequence from all data manifests is used. An
    /// override must not exceed the data sequence of any live data file to which
    /// an existing delete may apply.
    pub fn set_delete_file_cleanup_min_data_sequence_number(mut self, seq: i64) -> Self {
        self.delete_file_cleanup_min_data_sequence_number = Some(seq);
        self
    }

    pub fn set_check_file_existence(mut self, check: bool) -> Self {
        self.check_file_existence = check;
        self
    }
}

#[async_trait::async_trait]
impl<M: ReplaceFilesMode> TransactionAction for ReplaceFilesAction<M> {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if let Some(snapshot_id) = self.snapshot_id
            && table.metadata().snapshot_by_id(snapshot_id).is_some()
        {
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                format!("Snapshot id {snapshot_id} already exists"),
            ));
        }

        if let Some(sequence_number) = self.new_data_file_sequence_number {
            let next_sequence_number = table.metadata().next_sequence_number();
            if sequence_number < 0 || sequence_number > next_sequence_number {
                return Err(crate::Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!(
                        "New data file sequence number {sequence_number} must be between 0 and \
                         the new snapshot sequence number {next_sequence_number}"
                    ),
                ));
            }
        }

        if let Some(sequence_number) = self.delete_file_cleanup_min_data_sequence_number {
            let last_sequence_number = table.metadata().last_sequence_number();
            if sequence_number < 0 || sequence_number > last_sequence_number {
                return Err(crate::Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!(
                        "Delete file cleanup minimum data sequence number {sequence_number} must \
                         be between 0 and the table's last sequence number {last_sequence_number}"
                    ),
                ));
            }

            if !self.added_data_files.is_empty() {
                let added_data_sequence_number = self
                    .new_data_file_sequence_number
                    .unwrap_or(last_sequence_number);
                if sequence_number > added_data_sequence_number {
                    return Err(crate::Error::new(
                        crate::ErrorKind::DataInvalid,
                        format!(
                            "Delete file cleanup minimum data sequence number {sequence_number} \
                             must not exceed the added data file sequence number \
                             {added_data_sequence_number}"
                        ),
                    ));
                }
            }
        }

        let mut snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.snapshot_id,
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
            self.removed_data_files.clone(),
            self.removed_delete_files.clone(),
        );

        if let Some(seq) = self.new_data_file_sequence_number {
            snapshot_producer.set_new_data_file_sequence_number(seq);
        }

        if let Some(seq) = self.delete_file_cleanup_min_data_sequence_number {
            snapshot_producer.set_delete_file_cleanup_min_data_sequence_number(seq);
        }

        if let Some(branch) = &self.target_branch {
            snapshot_producer.set_target_branch(branch.clone());
        }

        if self.enable_delete_filter_manager {
            snapshot_producer.enable_delete_filter_manager()?;
        }

        snapshot_producer.validate_added_files(&self.added_data_files)?;
        snapshot_producer.validate_added_files(&self.added_delete_files)?;

        if self.check_file_existence {
            snapshot_producer.validate_data_file_changes().await?;
        }

        snapshot_producer
            .commit(ReplaceFilesOperation::<M>::new(), DefaultManifestProcess)
            .await
    }
}

impl<M: ReplaceFilesMode> Default for ReplaceFilesAction<M> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use uuid::Uuid;

    use super::{Overwrite, ReplaceFilesMode, ReplaceFilesOperation, Rewrite};
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH,
        ManifestContentType, ManifestEntry, ManifestListWriter, ManifestStatus,
        ManifestWriterBuilder, Operation, Snapshot, SnapshotRef, SnapshotReference,
        SnapshotRetention, Struct, Summary, UnboundPartitionSpec,
    };
    use crate::table::Table;
    use crate::test_utils::make_encrypted_table;
    use crate::transaction::snapshot::{SnapshotProduceOperation, SnapshotProducer};
    use crate::transaction::tests::{
        PARENT_SEQUENCE_NUMBER, PARENT_SNAPSHOT_ID, REMOVED_DELETE_FILE, RETAINED_DELETE_FILE,
        make_v2_minimal_table, make_v2_table_with_delete_manifest, make_v3_minimal_table,
        make_v3_minimal_table_in_catalog, position_delete_file,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};
    use crate::{ErrorKind, TableRequirement, TableUpdate};

    async fn delete_file_statuses(table: &Table, snapshot: &Snapshot) -> Vec<ManifestStatus> {
        let manifest_list = table
            .manifest_list_reader(&SnapshotRef::new(snapshot.clone()))
            .load()
            .await
            .unwrap();
        let mut statuses = Vec::new();
        for manifest_file in manifest_list
            .entries()
            .iter()
            .filter(|manifest| manifest.content == ManifestContentType::Deletes)
        {
            statuses.extend(
                manifest_file
                    .load_manifest(table.file_io())
                    .await
                    .unwrap()
                    .entries()
                    .iter()
                    .map(|entry| entry.status()),
            );
        }
        statuses
    }

    #[test]
    fn test_modes_map_to_their_operations() {
        assert_eq!(Rewrite::OPERATION, Operation::Replace);
        assert_eq!(Overwrite::OPERATION, Operation::Overwrite);
        assert_eq!(
            ReplaceFilesOperation::<Rewrite>::new().operation(),
            Operation::Replace
        );
        assert_eq!(
            ReplaceFilesOperation::<Overwrite>::new().operation(),
            Operation::Overwrite
        );
    }

    /// A logical overwrite may replace only part of a table. Its summary must account for the
    /// files actually removed instead of treating every overwrite as a full-table truncate.
    #[tokio::test]
    async fn test_partial_overwrite_summary_rolls_totals_forward() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let make_file = |path: String, record_count: u64, file_size_in_bytes: u64| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(path)
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(file_size_in_bytes)
                .record_count(record_count)
                .partition_spec_id(table.metadata().default_partition_spec_id())
                .partition(Struct::from_iter([Some(Literal::long(300))]))
                .build()
                .unwrap()
        };

        let parent_files = (0..5)
            .map(|index| make_file(format!("test/old-{index}.parquet"), 20, 200))
            .collect::<Vec<_>>();
        let removed_file = parent_files[0].clone();

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files(parent_files)
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let replacement = make_file("test/replacement.parquet".to_string(), 10, 100);
        let tx = Transaction::new(&table);
        let tx = tx
            .overwrite_files()
            .add_data_files([replacement])
            .delete_files([removed_file])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let summary = table.metadata().current_snapshot().unwrap().summary();
        assert_eq!(summary.operation, Operation::Overwrite);
        let properties = &summary.additional_properties;

        assert_eq!(
            properties.get("added-data-files").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            properties.get("deleted-data-files").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            properties.get("deleted-records").map(String::as_str),
            Some("20")
        );
        assert_eq!(
            properties.get("removed-files-size").map(String::as_str),
            Some("200")
        );
        assert_eq!(
            properties.get("total-data-files").map(String::as_str),
            Some("5")
        );
        assert_eq!(
            properties.get("total-records").map(String::as_str),
            Some("90")
        );
        assert_eq!(
            properties.get("total-files-size").map(String::as_str),
            Some("900")
        );
    }

    /// Regression test: a rewrite/overwrite that removes one delete file must not
    /// mark *unrelated* delete files as deleted.
    ///
    /// `delete_entries` once guarded the delete-file branch with
    ///   `content == PositionDeletes || content == EqualityDeletes && removed.contains(path)`
    /// and because `&&` binds tighter than `||`, every `PositionDeletes` entry in
    /// the parent snapshot matched regardless of the requested delete-file identities.
    async fn assert_only_removed_delete_files_marked<M: ReplaceFilesMode>() {
        let table = make_v2_table_with_delete_manifest().await;
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);

        let producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![],
            vec![removed],
        );

        let deleted_entries = ReplaceFilesOperation::<M>::new()
            .delete_entries(&producer)
            .await
            .unwrap();
        let deleted_paths: Vec<&str> = deleted_entries
            .iter()
            .map(|entry| entry.data_file().file_path())
            .collect();

        assert_eq!(
            deleted_paths,
            vec![REMOVED_DELETE_FILE],
            "only the removed delete file should be marked deleted; \
             {RETAINED_DELETE_FILE} must stay live"
        );
    }

    /// Regression test: rewriting a partially-deleted *delete* manifest must
    /// preserve its `Deletes` content type, and must carry survivors forward as
    /// `Existing` rather than restamping them as `Added`.
    async fn assert_delete_manifest_carried_forward_intact<M: ReplaceFilesMode>() {
        let table = make_v2_table_with_delete_manifest().await;
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);

        let mut producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![],
            vec![removed],
        );

        let existing = ReplaceFilesOperation::<M>::new()
            .existing_manifest(&mut producer)
            .await
            .unwrap();

        assert_eq!(existing.len(), 1, "the delete manifest should be rewritten");
        assert_eq!(
            existing[0].content,
            ManifestContentType::Deletes,
            "a rewritten delete manifest must stay a Deletes manifest"
        );

        let entries = existing[0].load_manifest(table.file_io()).await.unwrap();
        let paths: Vec<&str> = entries
            .entries()
            .iter()
            .map(|entry| entry.data_file().file_path())
            .collect();
        assert_eq!(paths, vec![RETAINED_DELETE_FILE]);

        let retained = &entries.entries()[0];
        assert_eq!(retained.status(), ManifestStatus::Existing);
        assert_eq!(retained.snapshot_id(), Some(PARENT_SNAPSHOT_ID));
        assert_eq!(retained.sequence_number(), Some(PARENT_SEQUENCE_NUMBER));
        assert_eq!(retained.file_sequence_number, Some(PARENT_SEQUENCE_NUMBER));
    }

    #[tokio::test]
    async fn test_overwrite_only_marks_removed_delete_files() {
        assert_only_removed_delete_files_marked::<Overwrite>().await;
    }

    #[tokio::test]
    async fn test_rewrite_only_marks_removed_delete_files() {
        assert_only_removed_delete_files_marked::<Rewrite>().await;
    }

    #[tokio::test]
    async fn test_overwrite_preserves_delete_manifest_content_type() {
        assert_delete_manifest_carried_forward_intact::<Overwrite>().await;
    }

    #[tokio::test]
    async fn test_rewrite_preserves_delete_manifest_content_type() {
        assert_delete_manifest_carried_forward_intact::<Rewrite>().await;
    }

    #[tokio::test]
    async fn test_replace_commit_preserves_delete_manifest_semantics() {
        let table = make_v2_table_with_delete_manifest().await;
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);
        let snapshot_id = SnapshotProducer::generate_unique_snapshot_id(&table);
        let action = Transaction::new(&table)
            .overwrite_files()
            .set_snapshot_id(snapshot_id)
            .set_check_file_existence(true)
            .delete_files([removed]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            unreachable!()
        };

        assert_eq!(snapshot.snapshot_id(), snapshot_id);
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("total-delete-files")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("total-position-deletes")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("total-files-size")
                .map(String::as_str),
            Some("100")
        );
        let manifest_list = table
            .manifest_list_reader(&SnapshotRef::new(snapshot.clone()))
            .load()
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .all(|manifest| manifest.content == ManifestContentType::Deletes)
        );

        let mut entries = Vec::new();
        for manifest_file in manifest_list.entries() {
            entries.extend(
                manifest_file
                    .load_manifest(table.file_io())
                    .await
                    .unwrap()
                    .entries()
                    .iter()
                    .map(|entry| (entry.file_path().to_string(), entry.status())),
            );
        }
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(entries, vec![
            (REMOVED_DELETE_FILE.to_string(), ManifestStatus::Deleted),
            (RETAINED_DELETE_FILE.to_string(), ManifestStatus::Existing),
        ]);
    }

    #[tokio::test]
    async fn test_replace_uses_requested_data_sequence_number() {
        let table = make_v2_table_with_delete_manifest().await;
        let added_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/replacement.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        let added_delete_file = position_delete_file(&table, "test/new-position-delete.parquet");
        let action = Transaction::new(&table)
            .rewrite_files()
            .set_new_data_file_sequence_number(PARENT_SEQUENCE_NUMBER)
            .add_data_files([added_file, added_delete_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            unreachable!()
        };
        let manifest_list = table
            .manifest_list_reader(&SnapshotRef::new(snapshot.clone()))
            .load()
            .await
            .unwrap();
        let added_manifest = manifest_list
            .entries()
            .iter()
            .find(|manifest| manifest.content == ManifestContentType::Data)
            .unwrap();
        let manifest = added_manifest.load_manifest(table.file_io()).await.unwrap();
        let entry = &manifest.entries()[0];

        assert_eq!(entry.sequence_number(), Some(PARENT_SEQUENCE_NUMBER));
        assert_eq!(entry.file_sequence_number, Some(snapshot.sequence_number()));

        let added_delete_manifest = manifest_list
            .entries()
            .iter()
            .find(|manifest| {
                manifest.content == ManifestContentType::Deletes
                    && manifest.added_snapshot_id == snapshot.snapshot_id()
            })
            .unwrap();
        let manifest = added_delete_manifest
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(
            manifest.entries()[0].sequence_number(),
            Some(snapshot.sequence_number()),
            "the data-file sequence override must not backdate newly written delete files"
        );
    }

    #[tokio::test]
    async fn test_delete_cleanup_sequence_override() {
        let table = make_v2_table_with_delete_manifest().await;
        let parent = table.metadata().current_snapshot().unwrap();
        let later_snapshot_id = PARENT_SNAPSHOT_ID + 1;
        let later_snapshot = Snapshot::builder()
            .with_snapshot_id(later_snapshot_id)
            .with_parent_snapshot_id(Some(PARENT_SNAPSHOT_ID))
            .with_timestamp_ms(parent.timestamp_ms() + 1)
            .with_sequence_number(PARENT_SEQUENCE_NUMBER + 2)
            .with_schema_id(parent.schema_id().unwrap())
            .with_manifest_list(parent.manifest_list())
            .with_summary(parent.summary().clone())
            .build();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(Some("memory:///test/location/metadata/v2.json".to_string()))
            .add_snapshot(later_snapshot)
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(
                    later_snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));
        let added_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/compacted.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = Transaction::new(&table)
            .rewrite_files()
            .set_new_data_file_sequence_number(PARENT_SEQUENCE_NUMBER)
            .add_data_files([added_file.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            unreachable!()
        };
        let delete_statuses = delete_file_statuses(&table, snapshot).await;

        assert_eq!(delete_statuses.len(), 2);
        assert!(
            delete_statuses
                .iter()
                .all(|status| *status != ManifestStatus::Deleted),
            "delete files newer than the explicitly retained data sequence must stay live"
        );

        let action = Transaction::new(&table)
            .rewrite_files()
            .set_new_data_file_sequence_number(PARENT_SEQUENCE_NUMBER)
            .set_delete_file_cleanup_min_data_sequence_number(PARENT_SEQUENCE_NUMBER + 1)
            .add_data_files([added_file.clone()]);
        let err = match Arc::new(action).commit(&table).await {
            Ok(_) => panic!("cleanup sequence newer than added data should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("must not exceed"));

        let action = Transaction::new(&table)
            .rewrite_files()
            .set_new_data_file_sequence_number(PARENT_SEQUENCE_NUMBER + 1)
            .set_delete_file_cleanup_min_data_sequence_number(PARENT_SEQUENCE_NUMBER + 1)
            .add_data_files([added_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            unreachable!()
        };
        let delete_statuses = delete_file_statuses(&table, snapshot).await;
        assert!(
            delete_statuses
                .iter()
                .all(|status| *status == ManifestStatus::Deleted)
        );
    }

    #[tokio::test]
    async fn test_replace_rejects_invalid_data_sequence_number() {
        let table = make_v2_minimal_table();
        let next_sequence_number = table.metadata().next_sequence_number();

        for sequence_number in [-1, next_sequence_number + 1] {
            let action = Transaction::new(&table)
                .rewrite_files()
                .set_new_data_file_sequence_number(sequence_number);
            let err = match Arc::new(action).commit(&table).await {
                Ok(_) => panic!("invalid data sequence number should fail"),
                Err(err) => err,
            };

            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(err.message().contains("must be between 0"));
        }
    }

    #[tokio::test]
    async fn test_replace_preserves_non_default_partition_spec_id() {
        let base = make_v2_minimal_table();
        let metadata = base
            .metadata()
            .clone()
            .into_builder(Some("memory:///test/location/metadata/v1.json".to_string()))
            .set_location("memory:///test/location".to_string())
            .add_partition_spec(UnboundPartitionSpec::builder().with_spec_id(1).build())
            .unwrap()
            .set_default_partition_spec(-1)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = base.with_metadata(Arc::new(metadata));
        assert_eq!(table.metadata().default_partition_spec_id(), 1);

        let old_spec = table.metadata().partition_spec_by_id(0).unwrap().clone();
        let make_old_file = |path: &str| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(path.to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(1)
                .partition_spec_id(0)
                .partition(Struct::from_iter([Some(Literal::long(300))]))
                .build()
                .unwrap()
        };
        let removed = make_old_file("test/old-spec-removed.parquet");
        let survivor = make_old_file("test/old-spec-survivor.parquet");
        let manifest_path = "memory:///test/location/metadata/old-spec-data.avro";
        let mut manifest_writer = ManifestWriterBuilder::new(
            table.file_io().new_output(manifest_path).unwrap(),
            Some(PARENT_SNAPSHOT_ID),
            table.metadata().current_schema().clone(),
            old_spec.as_ref().clone(),
        )
        .build_v2_data();
        for file in [removed.clone(), survivor.clone()] {
            manifest_writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(file)
                        .build(),
                )
                .unwrap();
        }
        let manifest_file = manifest_writer.write_manifest_file().await.unwrap();

        let manifest_list_path = "memory:///test/location/metadata/old-spec-manifest-list.avro";
        let output = table
            .file_io()
            .new_output(manifest_list_path)
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut manifest_list_writer =
            ManifestListWriter::v2(output, PARENT_SNAPSHOT_ID, None, PARENT_SEQUENCE_NUMBER);
        manifest_list_writer
            .add_manifests([manifest_file].into_iter())
            .unwrap();
        manifest_list_writer.close().await.unwrap();

        let parent_snapshot = Snapshot::builder()
            .with_snapshot_id(PARENT_SNAPSHOT_ID)
            .with_timestamp_ms(table.metadata().last_updated_ms() + 1)
            .with_sequence_number(PARENT_SEQUENCE_NUMBER)
            .with_schema_id(table.metadata().current_schema_id())
            .with_manifest_list(manifest_list_path)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: [
                    ("total-data-files".to_string(), "2".to_string()),
                    ("total-records".to_string(), "2".to_string()),
                    ("total-files-size".to_string(), "200".to_string()),
                ]
                .into_iter()
                .collect(),
            })
            .build();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(Some("memory:///test/location/metadata/v2.json".to_string()))
            .add_snapshot(parent_snapshot)
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(
                    PARENT_SNAPSHOT_ID,
                    SnapshotRetention::branch(None, None, None),
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));

        let action = Transaction::new(&table)
            .rewrite_files()
            .delete_files([removed]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            unreachable!()
        };
        let manifest_list = table
            .manifest_list_reader(&SnapshotRef::new(snapshot.clone()))
            .load()
            .await
            .unwrap();

        assert!(
            manifest_list
                .entries()
                .iter()
                .all(|manifest| manifest.partition_spec_id == 0),
            "rewritten and deleted-entry manifests must use the source files' spec, not default spec 1"
        );
        let mut statuses = Vec::new();
        for manifest_file in manifest_list.entries() {
            statuses.extend(
                manifest_file
                    .load_manifest(table.file_io())
                    .await
                    .unwrap()
                    .entries()
                    .iter()
                    .map(|entry| (entry.file_path().to_string(), entry.status())),
            );
        }
        statuses.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(statuses, vec![
            (
                "test/old-spec-removed.parquet".to_string(),
                ManifestStatus::Deleted,
            ),
            (
                "test/old-spec-survivor.parquet".to_string(),
                ManifestStatus::Existing,
            ),
        ]);
    }

    #[tokio::test]
    async fn test_replace_existence_validation_rejects_missing_file() {
        let table = make_v2_minimal_table();
        let missing_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/missing.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        let action = Transaction::new(&table)
            .rewrite_files()
            .set_check_file_existence(true)
            .delete_files([missing_file]);
        let err = match Arc::new(action).commit(&table).await {
            Ok(_) => panic!("missing delete file should fail validation"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("branch with no snapshot"));
    }

    #[tokio::test]
    async fn test_replace_uses_target_branch_parent_and_requirement() {
        let table = make_v2_table_with_delete_manifest().await;
        let main_snapshot_id = PARENT_SNAPSHOT_ID + 1;
        let parent = table.metadata().snapshot_by_id(PARENT_SNAPSHOT_ID).unwrap();
        let main_snapshot = Snapshot::builder()
            .with_snapshot_id(main_snapshot_id)
            .with_parent_snapshot_id(Some(PARENT_SNAPSHOT_ID))
            .with_timestamp_ms(parent.timestamp_ms() + 1)
            .with_sequence_number(PARENT_SEQUENCE_NUMBER + 1)
            .with_schema_id(parent.schema_id().unwrap())
            .with_manifest_list(parent.manifest_list())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: parent.summary().additional_properties.clone(),
            })
            .build();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(Some("memory:///test/location/metadata/v2.json".to_string()))
            .add_snapshot(main_snapshot)
            .unwrap()
            .set_ref(
                "staging",
                SnapshotReference::new(
                    PARENT_SNAPSHOT_ID,
                    SnapshotRetention::branch(None, None, None),
                ),
            )
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(
                    main_snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);
        let action = Transaction::new(&table)
            .rewrite_files()
            .set_target_branch("staging".to_string())
            .delete_files([removed]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            unreachable!()
        };

        assert_eq!(snapshot.parent_snapshot_id(), Some(PARENT_SNAPSHOT_ID));
        assert!(matches!(
            &updates[1],
            TableUpdate::SetSnapshotRef { ref_name, .. } if ref_name == "staging"
        ));
        assert_eq!(requirements[1], TableRequirement::RefSnapshotIdMatch {
            r#ref: "staging".to_string(),
            snapshot_id: Some(PARENT_SNAPSHOT_ID),
        });
    }

    #[tokio::test]
    async fn test_replace_writes_encrypted_manifest_and_manifest_list() {
        let table = make_encrypted_table().await;
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/encrypted-replacement.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::empty())
            .build()
            .unwrap();
        let action = Transaction::new(&table)
            .rewrite_files()
            .add_data_files([data_file.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let snapshot = updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                _ => None,
            })
            .unwrap();
        assert!(snapshot.encryption_key_id().is_some());

        let manifest_list = table
            .manifest_list_reader(&SnapshotRef::new(snapshot.clone()))
            .load()
            .await
            .unwrap();
        let data_manifest = manifest_list
            .entries()
            .iter()
            .find(|manifest| manifest.content == ManifestContentType::Data)
            .unwrap();
        assert!(data_manifest.key_metadata.is_some());
        let manifest = data_manifest.load_manifest(table.file_io()).await.unwrap();
        let mut expected_data_file = data_file;
        expected_data_file.first_row_id = Some(0);
        assert_eq!(manifest.entries()[0].data_file(), &expected_data_file);
    }

    #[tokio::test]
    async fn test_replace_drops_only_dangling_dv_from_shared_puffin() {
        const DATA_A: &str = "test/data-a.parquet";
        const DATA_B: &str = "test/data-b.parquet";
        const PUFFIN: &str = "test/shared.puffin";
        const DATA_MANIFEST: &str = "memory:///test/location/metadata/data-manifest.avro";
        const DELETE_MANIFEST: &str = "memory:///test/location/metadata/delete-manifest.avro";
        const MANIFEST_LIST: &str = "memory:///test/location/metadata/manifest-list.avro";

        let base = make_v3_minimal_table();
        let metadata = base
            .metadata()
            .clone()
            .into_builder(Some("memory:///test/location/metadata/v1.json".to_string()))
            .set_location("memory:///test/location".to_string())
            .build()
            .unwrap()
            .metadata;
        let table = base.with_metadata(Arc::new(metadata));
        let make_data_file = |path: &str, first_row_id: i64| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(path.to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(10)
                .partition_spec_id(table.metadata().default_partition_spec_id())
                .partition(Struct::from_iter([Some(Literal::long(300))]))
                .first_row_id(Some(first_row_id))
                .build()
                .unwrap()
        };
        let data_a = make_data_file(DATA_A, 0);
        let data_b = make_data_file(DATA_B, 10);
        let make_dv = |referenced_data_file: &str, offset: i64| {
            DataFileBuilder::default()
                .content(DataContentType::PositionDeletes)
                .file_path(PUFFIN.to_string())
                .file_format(DataFileFormat::Puffin)
                .file_size_in_bytes(256)
                .record_count(1)
                .partition_spec_id(table.metadata().default_partition_spec_id())
                .partition(Struct::from_iter([Some(Literal::long(300))]))
                .referenced_data_file(Some(referenced_data_file.to_string()))
                .content_offset(Some(offset))
                .content_size_in_bytes(Some(64))
                .build()
                .unwrap()
        };
        let dv_a = make_dv(DATA_A, 4);
        let dv_b = make_dv(DATA_B, 68);

        let mut data_writer = ManifestWriterBuilder::new(
            table.file_io().new_output(DATA_MANIFEST).unwrap(),
            Some(PARENT_SNAPSHOT_ID),
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v3_data();
        for file in [data_a.clone(), data_b] {
            data_writer
                .add_existing_file(
                    file,
                    PARENT_SNAPSHOT_ID,
                    PARENT_SEQUENCE_NUMBER,
                    Some(PARENT_SEQUENCE_NUMBER),
                )
                .unwrap();
        }
        let data_manifest = data_writer.write_manifest_file().await.unwrap();

        let mut delete_writer = ManifestWriterBuilder::new(
            table.file_io().new_output(DELETE_MANIFEST).unwrap(),
            Some(PARENT_SNAPSHOT_ID),
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v3_deletes();
        for file in [dv_a.clone(), dv_b] {
            delete_writer
                .add_existing_file(
                    file,
                    PARENT_SNAPSHOT_ID,
                    PARENT_SEQUENCE_NUMBER,
                    Some(PARENT_SEQUENCE_NUMBER),
                )
                .unwrap();
        }
        let delete_manifest = delete_writer.write_manifest_file().await.unwrap();

        let manifest_list_output = table
            .file_io()
            .new_output(MANIFEST_LIST)
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut manifest_list_writer = ManifestListWriter::v3(
            manifest_list_output,
            PARENT_SNAPSHOT_ID,
            None,
            PARENT_SEQUENCE_NUMBER,
            Some(0),
        );
        manifest_list_writer
            .add_manifests([data_manifest, delete_manifest].into_iter())
            .unwrap();
        manifest_list_writer.close().await.unwrap();

        let parent_snapshot = Snapshot::builder()
            .with_snapshot_id(PARENT_SNAPSHOT_ID)
            .with_timestamp_ms(table.metadata().last_updated_ms() + 1)
            .with_sequence_number(PARENT_SEQUENCE_NUMBER)
            .with_schema_id(table.metadata().current_schema_id())
            .with_manifest_list(MANIFEST_LIST)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: [
                    ("total-data-files".to_string(), "2".to_string()),
                    ("total-delete-files".to_string(), "2".to_string()),
                    ("total-records".to_string(), "20".to_string()),
                    ("total-files-size".to_string(), "712".to_string()),
                    ("total-position-deletes".to_string(), "2".to_string()),
                ]
                .into_iter()
                .collect(),
            })
            .with_row_range(0, 20)
            .build();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(Some("memory:///test/location/metadata/v1.json".to_string()))
            .add_snapshot(parent_snapshot)
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(
                    PARENT_SNAPSHOT_ID,
                    SnapshotRetention::branch(None, None, None),
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));

        let replacement = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/replacement.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(200)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        let action = Transaction::new(&table)
            .rewrite_files()
            .add_data_files([replacement])
            .delete_files([data_a]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let snapshot = updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                _ => None,
            })
            .unwrap();
        let manifest_list = table
            .manifest_list_reader(&SnapshotRef::new(snapshot.clone()))
            .load()
            .await
            .unwrap();

        let mut dv_entries = Vec::new();
        for manifest_file in manifest_list
            .entries()
            .iter()
            .filter(|manifest| manifest.content == ManifestContentType::Deletes)
        {
            dv_entries.extend(
                manifest_file
                    .load_manifest(table.file_io())
                    .await
                    .unwrap()
                    .entries()
                    .iter()
                    .map(|entry| {
                        (
                            entry.data_file().referenced_data_file(),
                            entry.data_file().content_offset(),
                            entry.status(),
                        )
                    }),
            );
        }
        dv_entries.sort_unstable_by_key(|entry| entry.1);

        assert_eq!(dv_entries, vec![
            (Some(DATA_A.to_string()), Some(4), ManifestStatus::Deleted),
            (Some(DATA_B.to_string()), Some(68), ManifestStatus::Existing),
        ]);

        let action = Transaction::new(&table)
            .rewrite_files()
            .delete_files([dv_a]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let snapshot = updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                _ => None,
            })
            .unwrap();
        let manifest_list = table
            .manifest_list_reader(&SnapshotRef::new(snapshot.clone()))
            .load()
            .await
            .unwrap();
        let mut offsets_and_statuses = Vec::new();
        for manifest_file in manifest_list
            .entries()
            .iter()
            .filter(|manifest| manifest.content == ManifestContentType::Deletes)
        {
            offsets_and_statuses.extend(
                manifest_file
                    .load_manifest(table.file_io())
                    .await
                    .unwrap()
                    .entries()
                    .iter()
                    .map(|entry| (entry.data_file().content_offset().unwrap(), entry.status())),
            );
        }
        offsets_and_statuses.sort_unstable_by_key(|entry| entry.0);

        assert_eq!(offsets_and_statuses, vec![
            (4, ManifestStatus::Deleted),
            (68, ManifestStatus::Existing),
        ]);
    }
}
