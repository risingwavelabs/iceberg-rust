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
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;

use crate::actions::maintenance::{DEFAULT_LOAD_CONCURRENCY, for_each_manifest};
use crate::delete_file_index::try_infer_single_referenced_data_file_from_bounds;
use crate::spec::{DataContentType, DataFile, MAIN_BRANCH, SnapshotRef, Struct};
use crate::table::Table;
use crate::transaction::{
    ActionCommit, ApplyTransactionAction, RewriteFilesAction, Transaction, TransactionAction,
    data_file_identity,
};
use crate::{Catalog, Error, ErrorKind, Result, TableIdent, TableRequirement};

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
    ///
    /// The dangling set is computed against the branch snapshot the commit is
    /// actually built on: if a concurrent commit moves the branch, the
    /// transaction's retry refreshes the table and rescans instead of replaying
    /// a stale decision.
    pub async fn execute(self) -> Result<usize> {
        let table = self.catalog.load_table(&self.table_ident).await?;
        let Some(snapshot) = table.metadata().snapshot_for_ref(&self.to_branch) else {
            return Ok(0);
        };

        // Scan once up front so a table with nothing to clean up skips the
        // commit entirely. The result is reused by the first commit attempt as
        // long as the branch still points at this snapshot.
        let dangling = find_dangling_delete_files(&table, snapshot).await?;
        if dangling.is_empty() {
            return Ok(0);
        }

        let action = RemoveDanglingDeleteFilesCommit {
            branch: self.to_branch.clone(),
            prescan: (snapshot.snapshot_id(), dangling),
            removed_count: Arc::new(AtomicUsize::new(0)),
        };
        let removed_count = Arc::clone(&action.removed_count);

        let txn = action.apply(Transaction::new(&table)).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to build remove-dangling-deletes action: {e}"),
            )
        })?;
        txn.commit(self.catalog.as_ref()).await?;

        Ok(removed_count.load(Ordering::SeqCst))
    }
}

/// Collects the delete files in `snapshot` that no longer apply to any live
/// data file, deduplicated by delete-file identity.
async fn find_dangling_delete_files(
    table: &Table,
    snapshot: &SnapshotRef,
) -> Result<Vec<DataFile>> {
    let manifest_list = table.manifest_list_reader(snapshot).load().await?;

    let mut data_file_paths: HashSet<String> = HashSet::new();
    let mut pos_deletes: Vec<(DataFile, Option<i64>)> = Vec::new();
    let mut eq_deletes: Vec<(DataFile, i64)> = Vec::new();
    let mut partition_min_seq: HashMap<(i32, Struct), i64> = HashMap::new();
    let mut global_min_data_seq: Option<i64> = None;

    let manifest_files: Vec<_> = manifest_list.entries().to_vec();
    for_each_manifest(
        table,
        manifest_files,
        DEFAULT_LOAD_CONCURRENCY,
        |_, manifest| {
            for entry in manifest.entries() {
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
        },
    )
    .await?;

    let mut dangling: Vec<DataFile> = Vec::new();

    dangling.extend(
        pos_deletes
            .into_iter()
            .filter(|(df, df_seq)| {
                if let Some(ref_path) = df.referenced_data_file() {
                    // Path-based: dangling if the referenced data file no longer exists
                    !data_file_paths.contains(&ref_path)
                } else if let Some(inferred_path) =
                    try_infer_single_referenced_data_file_from_bounds(df)
                {
                    // V2 position deletes may omit `referenced_data_file` while their
                    // `file_path` column's lower/upper bounds still prove a single
                    // target (the same heuristic the reader uses to scope deletes).
                    !data_file_paths.contains(&inferred_path)
                } else if let Some(s) = df_seq {
                    // Sequence-based: dangling if seq < min_data_seq in this partition
                    let key = (df.partition_spec_id(), df.partition().clone());
                    partition_min_seq
                        .get(&key)
                        .is_none_or(|&min_seq| *s < min_seq)
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
                    global_min_data_seq.is_none_or(|g| *seq <= g)
                } else {
                    let key = (df.partition_spec_id(), df.partition().clone());
                    partition_min_seq
                        .get(&key)
                        .is_none_or(|&min_seq| *seq <= min_seq)
                }
            })
            .map(|(df, _)| df),
    );

    // Dedup by `(file_path, content_offset, content_size_in_bytes)`, not
    // `file_path` alone: several deletion vectors can share one Puffin
    // file, so a path-only key would collapse a dangling DV and a
    // still-live DV in the same file into one entry.
    let mut seen = HashSet::new();
    dangling.retain(|df| seen.insert(data_file_identity(df)));

    Ok(dangling)
}

/// [`TransactionAction`] that computes the dangling set against the table state
/// it is committed on, then removes it with a [`RewriteFilesAction`].
///
/// [`Transaction::commit`] refreshes the table before every attempt and
/// re-applies its actions, so computing the dangling set here (rather than
/// once before the transaction) means a retry after a concurrent commit
/// rescans the refreshed snapshot instead of replaying a stale decision. The
/// rewrite's snapshot producer is built on the same `table` and requires the
/// branch to still point at its parent snapshot, so the scan and the commit
/// can never diverge: a commit that races in between fails the requirement
/// check with a retryable conflict and triggers a rescan.
struct RemoveDanglingDeleteFilesCommit {
    branch: String,
    /// Snapshot id and dangling set computed by `execute()`, reused when the
    /// branch has not moved since, to avoid scanning twice.
    prescan: (i64, Vec<DataFile>),
    /// Number of delete files removed by the most recent commit attempt.
    removed_count: Arc<AtomicUsize>,
}

#[async_trait]
impl TransactionAction for RemoveDanglingDeleteFilesCommit {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let Some(snapshot) = table.metadata().snapshot_for_ref(&self.branch) else {
            // The branch was removed concurrently; nothing left to clean up.
            self.removed_count.store(0, Ordering::SeqCst);
            return Ok(ActionCommit::new(vec![], vec![
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: self.branch.clone(),
                    snapshot_id: None,
                },
            ]));
        };

        let (prescan_snapshot_id, prescan_dangling) = &self.prescan;
        let dangling = if snapshot.snapshot_id() == *prescan_snapshot_id {
            prescan_dangling.clone()
        } else {
            find_dangling_delete_files(table, snapshot).await?
        };
        self.removed_count.store(dangling.len(), Ordering::SeqCst);

        if dangling.is_empty() {
            // The rescan found nothing: commit no updates, only pin the branch
            // to the snapshot that was just checked.
            return Ok(ActionCommit::new(vec![], vec![
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: self.branch.clone(),
                    snapshot_id: Some(snapshot.snapshot_id()),
                },
            ]));
        }

        let rewrite = RewriteFilesAction::new()
            .delete_files(dangling)
            .set_target_branch(self.branch.clone());
        Arc::new(rewrite).commit(table).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{
        RemoveDanglingDeleteFilesAction, RemoveDanglingDeleteFilesCommit,
        find_dangling_delete_files,
    };
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
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
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

    /// Like [`delete_file_is_live`], but also matches on `content_offset` — the
    /// identity of a specific deletion-vector blob within a shared Puffin file.
    async fn dv_entry_is_live(table: &Table, file_path: &str, content_offset: i64) -> bool {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        for mf in manifest_list.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive()
                    && entry.data_file().file_path() == file_path
                    && entry.data_file().content_offset() == Some(content_offset)
                {
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
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();

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

    /// Iceberg identifies a deletion-vector blob by
    /// `(file_path, content_offset, content_size_in_bytes)`, because several DVs
    /// can share one physical Puffin file. A dangling DV for one data file must
    /// not drop a still-live DV for another data file that happens to share the
    /// same Puffin `file_path`.
    #[tokio::test]
    async fn test_shared_puffin_file_keeps_live_dv_removes_dangling_dv() {
        let catalog = build_catalog().await;
        let (table_ident, table) = create_test_table(&catalog, "test_shared_puffin").await;

        let live_data_path = "memory://test/data-a.parquet";
        let table = commit_data_file(&catalog, &table, live_data_path).await;

        let shared_puffin_path = "memory://test/shared.puffin";

        let live_dv = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(shared_puffin_path.to_string())
            .file_format(DataFileFormat::Puffin)
            .record_count(1)
            .file_size_in_bytes(200)
            .referenced_data_file(Some(live_data_path.to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(64))
            .build()
            .unwrap();

        let dangling_dv = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(shared_puffin_path.to_string())
            .file_format(DataFileFormat::Puffin)
            .record_count(1)
            .file_size_in_bytes(200)
            .referenced_data_file(Some("memory://test/nonexistent.parquet".to_string()))
            .content_offset(Some(200))
            .content_size_in_bytes(Some(64))
            .build()
            .unwrap();

        let txn = Transaction::new(&table);
        let action = txn
            .rewrite_files()
            .add_data_files(vec![live_dv, dangling_dv])
            .set_target_branch(MAIN_BRANCH.to_string());
        let txn = action.apply(txn).unwrap();
        txn.commit(catalog.as_ref()).await.unwrap();

        let removed = RemoveDanglingDeleteFilesAction::new(catalog.clone(), table_ident.clone())
            .execute()
            .await
            .unwrap();
        assert_eq!(removed, 1, "only the dangling DV blob should be removed");

        let table = catalog.load_table(&table_ident).await.unwrap();
        assert!(
            dv_entry_is_live(&table, shared_puffin_path, 4).await,
            "the live DV blob sharing the Puffin file must survive"
        );
        assert!(
            !dv_entry_is_live(&table, shared_puffin_path, 200).await,
            "the dangling DV blob should be removed"
        );
    }

    /// Two *dangling* DVs sharing one Puffin file must both be identified and
    /// removed as distinct entries. Before deduping by identity instead of
    /// path, the scan would collapse them into a single `DataFile`, so only
    /// one of the two manifest entries would end up in the removal set —
    /// leaving the other to survive even though it is dangling too.
    #[tokio::test]
    async fn test_shared_puffin_file_removes_both_dangling_dvs() {
        let catalog = build_catalog().await;
        let (table_ident, table) = create_test_table(&catalog, "test_shared_puffin_both").await;

        let shared_puffin_path = "memory://test/shared.puffin";

        let dangling_dv_a = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(shared_puffin_path.to_string())
            .file_format(DataFileFormat::Puffin)
            .record_count(1)
            .file_size_in_bytes(200)
            .referenced_data_file(Some("memory://test/nonexistent-a.parquet".to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(64))
            .build()
            .unwrap();

        let dangling_dv_b = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(shared_puffin_path.to_string())
            .file_format(DataFileFormat::Puffin)
            .record_count(1)
            .file_size_in_bytes(200)
            .referenced_data_file(Some("memory://test/nonexistent-b.parquet".to_string()))
            .content_offset(Some(200))
            .content_size_in_bytes(Some(64))
            .build()
            .unwrap();

        let txn = Transaction::new(&table);
        let action = txn
            .rewrite_files()
            .add_data_files(vec![dangling_dv_a, dangling_dv_b])
            .set_target_branch(MAIN_BRANCH.to_string());
        let txn = action.apply(txn).unwrap();
        txn.commit(catalog.as_ref()).await.unwrap();

        let removed = RemoveDanglingDeleteFilesAction::new(catalog.clone(), table_ident.clone())
            .execute()
            .await
            .unwrap();
        assert_eq!(
            removed, 2,
            "both dangling DV blobs must be counted and removed, not collapsed into one"
        );

        let table = catalog.load_table(&table_ident).await.unwrap();
        assert!(
            !dv_entry_is_live(&table, shared_puffin_path, 4).await,
            "dangling DV blob A should be removed"
        );
        assert!(
            !dv_entry_is_live(&table, shared_puffin_path, 200).await,
            "dangling DV blob B should be removed"
        );
    }

    /// V2 position deletes may omit `referenced_data_file` while their
    /// `file_path` column's lower/upper bounds still prove a single target (the
    /// same heuristic the reader uses, see `delete_file_index`). A delete
    /// scoped this way to an already-removed data file is provably dangling
    /// even when a partition-wide sequence check alone would keep it, because
    /// another still-live data file with a lower sequence number sits in the
    /// same partition.
    #[tokio::test]
    async fn test_position_delete_without_ref_removed_by_bounds_inference() {
        use crate::delete_file_index::POSITION_DELETE_FILE_PATH_FIELD_ID;
        use crate::spec::Datum;

        let catalog = build_catalog().await;
        let (table_ident, table) = create_partitioned_table(&catalog, "test_pos_bounds").await;

        // seq 1: data-a in partition 1, stays alive with the lowest sequence
        // number in the partition.
        let table = commit_partitioned_file(
            &catalog,
            &table,
            "memory://test/data-a.parquet",
            DataContentType::Data,
            1,
        )
        .await;

        // seq 2: position delete in partition 1 whose bounds prove it targets
        // only "nonexistent-data-b.parquet" (a data file that was already
        // compacted away and never re-appears). Its sequence (2) is not below
        // the partition's min data sequence (1), so a sequence-only check
        // would wrongly keep it.
        let target_path = "memory://test/nonexistent-data-b.parquet";
        let spec_id = table.metadata().default_partition_spec_id();
        let pos_delete = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("memory://test/pos-bounds.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([Some(Literal::int(1))]))
            .lower_bounds(HashMap::from([(
                POSITION_DELETE_FILE_PATH_FIELD_ID,
                Datum::string(target_path),
            )]))
            .upper_bounds(HashMap::from([(
                POSITION_DELETE_FILE_PATH_FIELD_ID,
                Datum::string(target_path),
            )]))
            .build()
            .unwrap();

        let txn = Transaction::new(&table);
        let action = txn
            .rewrite_files()
            .add_data_files(vec![pos_delete])
            .set_target_branch(MAIN_BRANCH.to_string());
        let txn = action.apply(txn).unwrap();
        txn.commit(catalog.as_ref()).await.unwrap();

        let removed = RemoveDanglingDeleteFilesAction::new(catalog.clone(), table_ident.clone())
            .execute()
            .await
            .unwrap();
        assert_eq!(
            removed, 1,
            "bounds prove the delete targets an already-removed data file, so it is dangling \
             even though the partition-wide sequence check alone would keep it"
        );

        let table = catalog.load_table(&table_ident).await.unwrap();
        assert!(
            !delete_file_is_live(&table, "memory://test/pos-bounds.parquet").await,
            "the bounds-scoped dangling position delete should be removed"
        );
    }

    /// Commits a position delete that references `referenced_data_file`.
    async fn commit_pos_delete(
        catalog: &Arc<dyn Catalog>,
        table: &Table,
        file_path: &str,
        referenced_data_file: &str,
    ) -> Table {
        let pos_delete = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(file_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .referenced_data_file(Some(referenced_data_file.to_string()))
            .build()
            .unwrap();

        let txn = Transaction::new(table);
        let action = txn
            .rewrite_files()
            .add_data_files(vec![pos_delete])
            .set_target_branch(MAIN_BRANCH.to_string());
        let txn = action.apply(txn).unwrap();
        txn.commit(catalog.as_ref()).await.unwrap()
    }

    /// Simulates `execute()` racing a concurrent commit: the dangling set is
    /// scanned at `table_at_scan`, then `concurrent` moves the branch before the
    /// transaction commits. Returns the number of files the commit removed.
    async fn commit_with_stale_prescan(catalog: &Arc<dyn Catalog>, table_at_scan: &Table) -> usize {
        let snapshot = table_at_scan.metadata().current_snapshot().unwrap();
        let dangling = find_dangling_delete_files(table_at_scan, snapshot)
            .await
            .unwrap();
        assert_eq!(dangling.len(), 1, "the delete is dangling at scan time");

        let action = RemoveDanglingDeleteFilesCommit {
            branch: MAIN_BRANCH.to_string(),
            prescan: (snapshot.snapshot_id(), dangling),
            removed_count: Arc::new(AtomicUsize::new(0)),
        };
        let removed_count = Arc::clone(&action.removed_count);
        let txn = action.apply(Transaction::new(table_at_scan)).unwrap();
        txn.commit(catalog.as_ref()).await.unwrap();
        removed_count.load(Ordering::SeqCst)
    }

    /// Regression test for review of #168: a dangling-file decision computed
    /// against snapshot A must not be replayed against snapshot B when a
    /// concurrent commit made the delete applicable again. The commit rescans B
    /// and keeps the delete.
    #[tokio::test]
    async fn test_concurrent_commit_reviving_delete_is_rescanned() {
        let catalog = build_catalog().await;
        let (table_ident, table) = create_test_table(&catalog, "test_revived").await;
        let table = commit_data_file(&catalog, &table, "memory://test/data-1.parquet").await;
        let table_at_scan = commit_pos_delete(
            &catalog,
            &table,
            "memory://test/pos-del-2.parquet",
            "memory://test/data-2.parquet",
        )
        .await;

        // Concurrently, the data file the delete targets appears.
        let _ = commit_data_file(&catalog, &table_at_scan, "memory://test/data-2.parquet").await;

        let removed = commit_with_stale_prescan(&catalog, &table_at_scan).await;
        assert_eq!(removed, 0, "the rescan sees data-2, so nothing is dangling");

        let table = catalog.load_table(&table_ident).await.unwrap();
        assert!(
            delete_file_is_live(&table, "memory://test/pos-del-2.parquet").await,
            "a delete revived by a concurrent commit must be kept"
        );
    }

    /// A concurrent commit that does not affect the dangling set no longer
    /// fails the action: the commit rescans the refreshed snapshot and still
    /// removes the dangling delete.
    #[tokio::test]
    async fn test_concurrent_unrelated_commit_still_removes_dangling() {
        let catalog = build_catalog().await;
        let (table_ident, table) = create_test_table(&catalog, "test_unrelated").await;
        let table = commit_data_file(&catalog, &table, "memory://test/data-1.parquet").await;
        let table_at_scan = commit_pos_delete(
            &catalog,
            &table,
            "memory://test/pos-del-2.parquet",
            "memory://test/data-2.parquet",
        )
        .await;

        // Concurrently, an unrelated data file is committed.
        let concurrent =
            commit_data_file(&catalog, &table_at_scan, "memory://test/data-3.parquet").await;
        let concurrent_snapshot_id = concurrent.metadata().current_snapshot_id();

        let removed = commit_with_stale_prescan(&catalog, &table_at_scan).await;
        assert_eq!(removed, 1);

        let table = catalog.load_table(&table_ident).await.unwrap();
        assert!(!delete_file_is_live(&table, "memory://test/pos-del-2.parquet").await);
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .parent_snapshot_id(),
            concurrent_snapshot_id,
            "the removal is committed on top of the concurrent snapshot"
        );
    }
}
