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

//! Transaction action for removing snapshot.

use std::collections::{HashMap, HashSet};
use std::i32;

use itertools::Itertools;

use crate::error::Result;
use crate::spec::{
    SnapshotReference, SnapshotRetention, MAIN_BRANCH, MAX_REF_AGE_MS, MAX_REF_AGE_MS_DEFAULT,
    MAX_SNAPSHOT_AGE_MS, MAX_SNAPSHOT_AGE_MS_DEFAULT, MIN_SNAPSHOTS_TO_KEEP,
    MIN_SNAPSHOTS_TO_KEEP_DEFAULT,
};
use crate::transaction::Transaction;
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// RemoveSnapshotAction is a transaction action for removing snapshot.
pub struct RemoveSnapshotAction<'a> {
    tx: Transaction<'a>,
    clear_expire_files: bool,
    ids_to_remove: HashSet<i64>,
    default_expired_older_than: i64,
    default_min_num_snapshots: i32,
    default_max_ref_age_ms: i64,
    clear_expired_meta_data: bool,

    now: i64,
}

impl<'a> RemoveSnapshotAction<'a> {
    /// Creates a new action.
    pub fn new(tx: Transaction<'a>) -> Self {
        let table = tx.table;
        let properties = table.metadata().properties();

        let now = chrono::Utc::now().timestamp_millis();

        let default_max_snapshot_age_ms = properties
            .get(MAX_SNAPSHOT_AGE_MS)
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(MAX_SNAPSHOT_AGE_MS_DEFAULT);

        let default_min_num_snapshots = properties
            .get(MIN_SNAPSHOTS_TO_KEEP)
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(MIN_SNAPSHOTS_TO_KEEP_DEFAULT);

        let default_max_ref_age_ms = properties
            .get(MAX_REF_AGE_MS)
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(MAX_REF_AGE_MS_DEFAULT);

        Self {
            tx,
            clear_expire_files: false,
            ids_to_remove: HashSet::new(),
            default_expired_older_than: now - default_max_snapshot_age_ms,
            default_min_num_snapshots,
            default_max_ref_age_ms,
            now,
            clear_expired_meta_data: false,
        }
    }

    /// Finished building the action and apply it to the transaction.
    pub fn clear_expire_files(mut self, clear_expire_files: bool) -> Self {
        self.clear_expire_files = clear_expire_files;
        self
    }

    /// Finished building the action and apply it to the transaction.
    pub fn expire_snapshot_id(mut self, expire_snapshot_id: i64) -> Self {
        self.ids_to_remove.insert(expire_snapshot_id);
        self
    }

    /// Finished building the action and apply it to the transaction.
    pub fn expire_older_than(mut self, timestamp_ms: i64) -> Self {
        self.default_expired_older_than = timestamp_ms;
        self
    }

    /// Finished building the action and apply it to the transaction.
    pub fn retain_last(mut self, min_num_snapshots: i32) -> Self {
        self.default_min_num_snapshots = min_num_snapshots;
        self
    }

    /// Finished building the action and apply it to the transaction.
    pub fn clear_expired_meta_data(mut self, clear_expired_meta_data: bool) -> Self {
        self.clear_expired_meta_data = clear_expired_meta_data;
        self
    }

    /// Finished building the action and apply it to the transaction.
    async fn apply_impl(mut self) -> Result<Transaction<'a>> {
        if self.tx.table.metadata().refs.is_empty() {
            return Ok(self.tx);
        }

        let table_meta = self.tx.table.metadata().clone();

        let mut ids_to_retain = HashSet::new();
        let retained_refs = self.compute_retained_refs(&table_meta.refs);
        let mut retained_id_to_refs = HashMap::new();
        for (ref_name, snapshot_ref) in &retained_refs {
            let snapshot_id = snapshot_ref.snapshot_id;
            retained_id_to_refs
                .entry(snapshot_id)
                .or_insert_with(Vec::new)
                .push(ref_name.clone());

            ids_to_retain.insert(snapshot_id);
        }

        for id_to_remove in &self.ids_to_remove {
            let refs_for_id = retained_id_to_refs.get(id_to_remove);
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot remove snapshot {:?} with retained references: {:?}",
                    id_to_remove, refs_for_id
                ),
            ));
        }

        ids_to_retain.extend(self.compute_all_branch_snapshots_to_retain(table_meta.refs.values()));
        ids_to_retain.extend(self.unreferenced_snapshots_to_retain(table_meta.refs.values()));

        for ref_name in table_meta.refs.keys() {
            if !retained_refs.contains_key(ref_name) {
                self.tx
                    .append_updates(vec![TableUpdate::RemoveSnapshotRef {
                        ref_name: ref_name.clone(),
                    }])?;
            }
        }

        let mut snapshot_to_remove = Vec::from_iter(self.ids_to_remove.iter().cloned());
        for snapshot in table_meta.snapshots() {
            if !ids_to_retain.contains(&snapshot.snapshot_id()) {
                snapshot_to_remove.push(snapshot.snapshot_id());
            }
        }

        self.tx.append_updates(vec![TableUpdate::RemoveSnapshots {
            snapshot_ids: snapshot_to_remove,
        }])?;

        if self.clear_expired_meta_data {
            let mut reachable_specs = HashSet::new();
            reachable_specs.insert(table_meta.current_schema_id());
            let mut reachable_schemas = HashSet::new();
            reachable_schemas.insert(table_meta.current_schema_id());

            //TODO: parallelize
            for snapshot in table_meta.snapshots() {
                if ids_to_retain.contains(&snapshot.snapshot_id()) {
                    let manifest_list = snapshot
                        .load_manifest_list(self.tx.table.file_io(), &table_meta)
                        .await?;

                    for manifest in manifest_list.entries() {
                        reachable_specs.insert(manifest.partition_spec_id);
                    }

                    if let Some(schema_id) = snapshot.schema_id() {
                        reachable_schemas.insert(schema_id);
                    }
                }
            }

            let spec_to_remove = self
                .tx
                .table
                .metadata()
                .partition_specs_iter()
                .filter_map(|spec| {
                    if !reachable_specs.contains(&spec.spec_id()) {
                        Some(spec.spec_id())
                    } else {
                        None
                    }
                })
                .unique()
                .collect();

            self.tx
                .append_updates(vec![TableUpdate::RemovePartitionSpecs {
                    spec_ids: spec_to_remove,
                }])?;

            let schema_to_remove = self
                .tx
                .table
                .metadata()
                .schemas_iter()
                .filter_map(|schema| {
                    if !reachable_schemas.contains(&schema.schema_id()) {
                        Some(schema.schema_id())
                    } else {
                        None
                    }
                })
                .unique()
                .collect();

            self.tx.append_updates(vec![TableUpdate::RemoveSchemas {
                schema_ids: schema_to_remove,
            }])?;
        }

        self.tx.append_requirements(vec![
            TableRequirement::UuidMatch {
                uuid: self.tx.table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: self.tx.table.metadata().current_snapshot_id(),
            },
        ])?;

        Ok(self.tx)
    }

    /// Finished building the action and apply it to the transaction.
    pub async fn apply(self) -> Result<Transaction<'a>> {
        self.apply_impl().await
    }

    fn compute_retained_refs(
        &self,
        snapshot_refs: &HashMap<String, SnapshotReference>,
    ) -> HashMap<String, SnapshotReference> {
        let table_meta = self.tx.table.metadata();
        let mut retained_refs = HashMap::new();

        for (ref_name, snapshot_ref) in snapshot_refs {
            if ref_name == MAIN_BRANCH {
                retained_refs.insert(ref_name.clone(), snapshot_ref.clone());
                continue;
            }

            let snapshot = table_meta.snapshot_by_id(snapshot_ref.snapshot_id);
            let max_ref_age_ms = match &snapshot_ref.retention {
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: _,
                    max_snapshot_age_ms: _,
                    max_ref_age_ms,
                } => max_ref_age_ms,
                SnapshotRetention::Tag { max_ref_age_ms } => max_ref_age_ms,
            }
            .unwrap_or(self.default_max_ref_age_ms);

            if let Some(snapshot) = snapshot {
                let ref_age_ms = self.now - snapshot.timestamp_ms();
                if ref_age_ms <= max_ref_age_ms {
                    retained_refs.insert(ref_name.clone(), snapshot_ref.clone());
                }
            } else {
                // warn
            }
        }

        retained_refs
    }

    fn compute_all_branch_snapshots_to_retain(
        &self,
        refs: impl Iterator<Item = &'_ SnapshotReference>,
    ) -> HashSet<i64> {
        let mut branch_snapshots_to_retain = HashSet::new();
        for snapshot_ref in refs {
            if snapshot_ref.is_branch() {
                let max_snapshot_age_ms = match snapshot_ref.retention {
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep: _,
                        max_snapshot_age_ms,
                        max_ref_age_ms: _,
                    } => max_snapshot_age_ms,
                    SnapshotRetention::Tag { max_ref_age_ms: _ } => None,
                }
                .unwrap_or(self.default_expired_older_than);

                let expire_snapshot_older_than = self.now - max_snapshot_age_ms;

                let min_snapshots_to_keep = match snapshot_ref.retention {
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep,
                        max_snapshot_age_ms: _,
                        max_ref_age_ms: _,
                    } => min_snapshots_to_keep,
                    SnapshotRetention::Tag { max_ref_age_ms: _ } => None,
                }
                .unwrap_or(self.default_min_num_snapshots);

                branch_snapshots_to_retain.extend(self.compute_branch_snapshots_to_retain(
                    snapshot_ref.snapshot_id,
                    expire_snapshot_older_than,
                    min_snapshots_to_keep as usize,
                ));
            }
        }

        branch_snapshots_to_retain
    }

    fn compute_branch_snapshots_to_retain(
        &self,
        snapshot_id: i64,
        expire_snapshots_older_than: i64,
        min_snapshots_to_keep: usize,
    ) -> HashSet<i64> {
        let mut ids_to_retain = HashSet::new();
        let table_meta = self.tx.table.metadata();
        if let Some(snapshot) = table_meta.snapshot_by_id(snapshot_id) {
            let mut snapshot = snapshot.clone();
            while let Some(ancestor) = snapshot.parent_snapshot(table_meta) {
                if ids_to_retain.len() < min_snapshots_to_keep
                    || snapshot.timestamp_ms() >= expire_snapshots_older_than
                {
                    ids_to_retain.insert(snapshot.snapshot_id());
                } else {
                    return ids_to_retain;
                }

                snapshot = ancestor;
            }
        }

        ids_to_retain
    }

    fn unreferenced_snapshots_to_retain(
        &self,
        refs: impl Iterator<Item = &SnapshotReference>,
    ) -> HashSet<i64> {
        let mut ids_to_retain = HashSet::new();

        for snapshot_ref in refs {
            if snapshot_ref.is_branch() {
                if let Some(snapshot) = self
                    .tx
                    .table
                    .metadata()
                    .snapshot_by_id(snapshot_ref.snapshot_id)
                {
                    let mut snapshot = snapshot.clone();
                    while let Some(ancestor) = snapshot.parent_snapshot(self.tx.table.metadata()) {
                        ids_to_retain.insert(snapshot.snapshot_id());

                        snapshot = ancestor;
                    }
                }
            } else {
                ids_to_retain.insert(snapshot_ref.snapshot_id);
            }
        }

        ids_to_retain
    }
}
