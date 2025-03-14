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

//! This module contains transaction api.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::mem::discriminant;
use std::ops::RangeFrom;
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::StringArray;
use futures::TryStreamExt;
use uuid::Uuid;

use crate::error::Result;
use crate::io::FileIO;
use crate::remove_snapshots::RemoveSnapshotAction;
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, ManifestContentType, ManifestEntry,
    ManifestFile, ManifestListWriter, ManifestStatus, ManifestWriter, ManifestWriterBuilder,
    NullOrder, Operation, Snapshot, SnapshotReference, SnapshotRetention, SortDirection, SortField,
    SortOrder, Struct, StructType, Summary, Transform, MAIN_BRANCH,
};
use crate::table::Table;
use crate::utils::bin::ListPacker;
use crate::writer::file_writer::ParquetWriter;
use crate::TableUpdate::UpgradeFormatVersion;
use crate::{Catalog, Error, ErrorKind, TableCommit, TableRequirement, TableUpdate};

const META_ROOT_PATH: &str = "metadata";

/// Target size of manifest file when merging manifests.
pub const MANIFEST_TARGET_SIZE_BYTES: &str = "commit.manifest.target-size-bytes";
const MANIFEST_TARGET_SIZE_BYTES_DEFAULT: u32 = 8 * 1024 * 1024; // 8 MB
/// Minimum number of manifests to merge.
pub const MANIFEST_MIN_MERGE_COUNT: &str = "commit.manifest.min-count-to-merge";
const MANIFEST_MIN_MERGE_COUNT_DEFAULT: u32 = 100;
/// Whether allow to merge manifests.
pub const MANIFEST_MERGE_ENABLED: &str = "commit.manifest-merge.enabled";
const MANIFEST_MERGE_ENABLED_DEFAULT: bool = false;

/// Table transaction.
pub struct Transaction<'a> {
    base_table: &'a Table,
    pub(crate) current_table: Table,
    updates: Vec<TableUpdate>,
    requirements: Vec<TableRequirement>,
}

impl<'a> Transaction<'a> {
    /// Creates a new transaction.
    pub fn new(table: &'a Table) -> Self {
        Self {
            base_table: table,
            current_table: table.clone(),
            updates: vec![],
            requirements: vec![],
        }
    }

    fn update_table_metadata(&mut self, updates: &[TableUpdate]) -> Result<()> {
        let mut metadata_builder = self.current_table.metadata().clone().into_builder(None);
        for update in updates {
            metadata_builder = update.clone().apply(metadata_builder)?;
        }

        self.current_table
            .with_metadata(Arc::new(metadata_builder.build()?.metadata));

        Ok(())
    }

    pub(crate) fn apply(
        &mut self,
        updates: Vec<TableUpdate>,
        requirements: Vec<TableRequirement>,
    ) -> Result<()> {
        for requirement in &requirements {
            requirement.check(Some(self.current_table.metadata()))?;
        }

        self.update_table_metadata(&updates)?;

        self.updates.extend(updates);

        // For the requirements, it does not make sense to add a requirement more than once
        // For example, you cannot assert that the current schema has two different IDs
        for new_requirement in requirements {
            if self
                .requirements
                .iter()
                .map(discriminant)
                .all(|d| d != discriminant(&new_requirement))
            {
                self.requirements.push(new_requirement);
            }
        }

        // # TODO
        // Support auto commit later.

        Ok(())
    }

    /// Sets table to a new version.
    pub fn upgrade_table_version(mut self, format_version: FormatVersion) -> Result<Self> {
        let current_version = self.current_table.metadata().format_version();
        match current_version.cmp(&format_version) {
            Ordering::Greater => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot downgrade table version from {} to {}",
                        current_version, format_version
                    ),
                ));
            }
            Ordering::Less => {
                self.apply(vec![UpgradeFormatVersion { format_version }], vec![])?;
            }
            Ordering::Equal => {
                // Do nothing.
            }
        }
        Ok(self)
    }

    /// Update table's property.
    pub fn set_properties(mut self, props: HashMap<String, String>) -> Result<Self> {
        self.apply(vec![TableUpdate::SetProperties { updates: props }], vec![])?;
        Ok(self)
    }

    /// Generate a unique snapshot id.
    pub fn generate_unique_snapshot_id(&self) -> i64 {
        let generate_random_id = || -> i64 {
            let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
            let snapshot_id = (lhs ^ rhs) as i64;
            if snapshot_id < 0 {
                -snapshot_id
            } else {
                snapshot_id
            }
        };
        let mut snapshot_id = generate_random_id();
        while self
            .current_table
            .metadata()
            .snapshots()
            .any(|s| s.snapshot_id() == snapshot_id)
        {
            snapshot_id = generate_random_id();
        }
        snapshot_id
    }

    /// Creates a fast append action.
    pub fn fast_append(
        self,
        snapshot_id: Option<i64>,
        commit_uuid: Option<Uuid>,
        key_metadata: Vec<u8>,
    ) -> Result<FastAppendAction<'a>> {
        let snapshot_id = if let Some(snapshot_id) = snapshot_id {
            if self
                .current_table
                .metadata()
                .snapshots()
                .any(|s| s.snapshot_id() == snapshot_id)
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Snapshot id {} already exists", snapshot_id),
                ));
            }
            snapshot_id
        } else {
            self.generate_unique_snapshot_id()
        };
        FastAppendAction::new(
            self,
            snapshot_id,
            commit_uuid.unwrap_or_else(Uuid::now_v7),
            key_metadata,
            HashMap::new(),
        )
    }

    /// Creates a merge append action.
    pub fn merge_append(
        self,
        commit_uuid: Option<Uuid>,
        key_metadata: Vec<u8>,
    ) -> Result<MergeAppendAction<'a>> {
        let snapshot_id = self.generate_unique_snapshot_id();
        MergeAppendAction::new(
            self,
            snapshot_id,
            commit_uuid.unwrap_or_else(Uuid::now_v7),
            key_metadata,
            HashMap::new(),
        )
    }

    /// Creates replace sort order action.
    pub fn replace_sort_order(self) -> ReplaceSortOrderAction<'a> {
        ReplaceSortOrderAction {
            tx: self,
            sort_fields: vec![],
        }
    }

    /// Creates remove snapshot action.
    pub fn expire_snapshot(self) -> RemoveSnapshotAction<'a> {
        RemoveSnapshotAction::new(self)
    }

    /// Remove properties in table.
    pub fn remove_properties(mut self, keys: Vec<String>) -> Result<Self> {
        self.apply(
            vec![TableUpdate::RemoveProperties { removals: keys }],
            vec![],
        )?;
        Ok(self)
    }

    /// Commit transaction.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        let table_commit = TableCommit::builder()
            .ident(self.base_table.identifier().clone())
            .updates(self.updates)
            .requirements(self.requirements)
            .build();

        catalog.update_table(table_commit).await
    }
}

/// FastAppendAction is a transaction action for fast append data files to the table.
pub struct FastAppendAction<'a> {
    snapshot_produce_action: SnapshotProduceAction<'a>,
}

impl<'a> FastAppendAction<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        tx: Transaction<'a>,
        snapshot_id: i64,
        commit_uuid: Uuid,
        key_metadata: Vec<u8>,
        snapshot_properties: HashMap<String, String>,
    ) -> Result<Self> {
        Ok(Self {
            snapshot_produce_action: SnapshotProduceAction::new(
                tx,
                snapshot_id,
                key_metadata,
                commit_uuid,
                snapshot_properties,
            )?,
        })
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(
        &mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<&mut Self> {
        self.snapshot_produce_action.add_data_files(data_files)?;
        Ok(self)
    }

    /// Adds existing parquet files
    #[allow(dead_code)]
    async fn add_parquet_files(mut self, file_path: Vec<String>) -> Result<Transaction<'a>> {
        if !self
            .snapshot_produce_action
            .tx
            .current_table
            .metadata()
            .default_spec
            .is_unpartitioned()
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Appending to partitioned tables is not supported",
            ));
        }

        let data_files = ParquetWriter::parquet_files_to_data_files(
            self.snapshot_produce_action.tx.current_table.file_io(),
            file_path,
            self.snapshot_produce_action.tx.current_table.metadata(),
        )
        .await?;

        self.add_data_files(data_files)?;

        self.apply().await
    }

    /// Finished building the action and apply it to the transaction.
    pub async fn apply(self) -> Result<Transaction<'a>> {
        // Checks duplicate files
        let new_files: HashSet<&str> = self
            .snapshot_produce_action
            .added_data_files
            .iter()
            .map(|df| df.file_path.as_str())
            .collect();

        let mut manifest_stream = self
            .snapshot_produce_action
            .tx
            .current_table
            .inspect()
            .manifests()
            .scan()
            .await?;
        let mut referenced_files = Vec::new();

        while let Some(batch) = manifest_stream.try_next().await? {
            let file_path_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Failed to downcast file_path column to StringArray",
                    )
                })?;

            for i in 0..batch.num_rows() {
                let file_path = file_path_array.value(i);
                if new_files.contains(file_path) {
                    referenced_files.push(file_path.to_string());
                }
            }
        }

        if !referenced_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add files that are already referenced by table, files: {}",
                    referenced_files.join(", ")
                ),
            ));
        }

        self.snapshot_produce_action
            .apply(FastAppendOperation, DefaultManifestProcess)
            .await
    }
}

/// MergeAppendAction is a transaction action similar to fast append except that it will merge manifests
/// based on the target size.
pub struct MergeAppendAction<'a> {
    snapshot_produce_action: SnapshotProduceAction<'a>,
    target_size_bytes: u32,
    min_count_to_merge: u32,
    merge_enabled: bool,
}

impl<'a> MergeAppendAction<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        tx: Transaction<'a>,
        snapshot_id: i64,
        commit_uuid: Uuid,
        key_metadata: Vec<u8>,
        snapshot_properties: HashMap<String, String>,
    ) -> Result<Self> {
        let target_size_bytes: u32 = tx
            .current_table
            .metadata()
            .properties()
            .get(MANIFEST_TARGET_SIZE_BYTES)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
        let min_count_to_merge: u32 = tx
            .current_table
            .metadata()
            .properties()
            .get(MANIFEST_MIN_MERGE_COUNT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MIN_MERGE_COUNT_DEFAULT);
        let merge_enabled = tx
            .current_table
            .metadata()
            .properties()
            .get(MANIFEST_MERGE_ENABLED)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MERGE_ENABLED_DEFAULT);
        Ok(Self {
            snapshot_produce_action: SnapshotProduceAction::new(
                tx,
                snapshot_id,
                key_metadata,
                commit_uuid,
                snapshot_properties,
            )?,
            target_size_bytes,
            min_count_to_merge,
            merge_enabled,
        })
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(
        &mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<&mut Self> {
        self.snapshot_produce_action.add_data_files(data_files)?;
        Ok(self)
    }

    /// Finished building the action and apply it to the transaction.
    pub async fn apply(self) -> Result<Transaction<'a>> {
        if self.merge_enabled {
            let process = MergeManifestProcess {
                target_size_bytes: self.target_size_bytes,
                min_count_to_merge: self.min_count_to_merge,
            };
            self.snapshot_produce_action
                .apply(FastAppendOperation, process)
                .await
        } else {
            self.snapshot_produce_action
                .apply(FastAppendOperation, DefaultManifestProcess)
                .await
        }
    }
}

struct FastAppendOperation;

impl SnapshotProduceOperation for FastAppendOperation {
    fn operation(&self) -> Operation {
        Operation::Append
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProduceAction<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProduceAction<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce
            .tx
            .current_table
            .metadata()
            .current_snapshot()
        else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.tx.current_table.file_io(),
                snapshot_produce.tx.current_table.metadata(),
            )
            .await?;

        Ok(manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.has_added_files() || entry.has_existing_files())
            .cloned()
            .collect())
    }
}

trait SnapshotProduceOperation: Send + Sync {
    fn operation(&self) -> Operation;
    #[allow(unused)]
    fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProduceAction,
    ) -> impl Future<Output = Result<Vec<ManifestEntry>>> + Send;
    fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProduceAction,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;
}

struct DefaultManifestProcess;

impl ManifestProcess for DefaultManifestProcess {
    async fn process_manifest<'a>(
        &self,
        _snapshot_producer: &mut SnapshotProduceAction<'a>,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        Ok(manifests)
    }
}

struct MergeManifestProcess {
    target_size_bytes: u32,
    min_count_to_merge: u32,
}

impl MergeManifestProcess {
    pub fn new(target_size_bytes: u32, min_count_to_merge: u32) -> Self {
        Self {
            target_size_bytes,
            min_count_to_merge,
        }
    }
}

impl ManifestProcess for MergeManifestProcess {
    async fn process_manifest<'a>(
        &self,
        snapshot_produce: &mut SnapshotProduceAction<'a>,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        let (unmerge_data_manifest, unmerge_delete_manifest): (Vec<_>, Vec<_>) = manifests
            .into_iter()
            .partition(|manifest| matches!(manifest.content, ManifestContentType::Data));
        let mut data_manifest = {
            let manifest_merge_manager = MergeManifestManager::new(
                self.target_size_bytes,
                self.min_count_to_merge,
                ManifestContentType::Data,
            );
            manifest_merge_manager
                .merge_manifest(snapshot_produce, unmerge_data_manifest)
                .await?
        };
        data_manifest.extend(unmerge_delete_manifest);
        Ok(data_manifest)
    }
}

struct MergeManifestManager {
    target_size_bytes: u32,
    min_count_to_merge: u32,
    content: ManifestContentType,
}

impl MergeManifestManager {
    pub fn new(
        target_size_bytes: u32,
        min_count_to_merge: u32,
        content: ManifestContentType,
    ) -> Self {
        Self {
            target_size_bytes,
            min_count_to_merge,
            content,
        }
    }

    fn group_by_spec(&self, manifests: Vec<ManifestFile>) -> BTreeMap<i32, Vec<ManifestFile>> {
        let mut grouped_manifests = BTreeMap::new();
        for manifest in manifests {
            grouped_manifests
                .entry(manifest.partition_spec_id)
                .or_insert_with(Vec::new)
                .push(manifest);
        }
        grouped_manifests
    }

    async fn merge_bin(
        &self,
        snapshot_id: i64,
        file_io: FileIO,
        manifest_bin: Vec<ManifestFile>,
        mut writer: ManifestWriter,
    ) -> Result<ManifestFile> {
        for manifest_file in manifest_bin {
            let manifest_file = manifest_file.load_manifest(&file_io).await?;
            for manifest_entry in manifest_file.entries() {
                if manifest_entry.status() == ManifestStatus::Deleted
                    && manifest_entry
                        .snapshot_id()
                        .is_some_and(|id| id == snapshot_id)
                {
                    //only files deleted by this snapshot should be added to the new manifest
                    writer.add_delete_entry(manifest_entry.as_ref().clone())?;
                } else if manifest_entry.status() == ManifestStatus::Added
                    && manifest_entry
                        .snapshot_id()
                        .is_some_and(|id| id == snapshot_id)
                {
                    //added entries from this snapshot are still added, otherwise they should be existing
                    writer.add_entry(manifest_entry.as_ref().clone())?;
                } else if manifest_entry.status() != ManifestStatus::Deleted {
                    // add all non-deleted files from the old manifest as existing files
                    writer.add_existing_entry(manifest_entry.as_ref().clone())?;
                }
            }
        }

        writer.write_manifest_file().await
    }

    async fn merge_group<'a>(
        &self,
        snapshot_produce: &mut SnapshotProduceAction<'a>,
        first_manifest: &ManifestFile,
        group_manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        let packer: ListPacker<ManifestFile> = ListPacker::new(self.target_size_bytes);
        let manifest_bins =
            packer.pack(group_manifests, |manifest| manifest.manifest_length as u32);

        let manifest_merge_futures = manifest_bins
            .into_iter()
            .map(|manifest_bin| {
                if manifest_bin.len() == 1 {
                    Ok(Box::pin(async { Ok(manifest_bin) })
                        as Pin<
                            Box<dyn Future<Output = Result<Vec<ManifestFile>>> + Send>,
                        >)
                }
                //  if the bin has the first manifest (the new data files or an appended manifest file) then only
                //  merge it if the number of manifests is above the minimum count. this is applied only to bins
                //  with an in-memory manifest so that large manifests don't prevent merging older groups.
                else if manifest_bin
                    .iter()
                    .any(|manifest| manifest == first_manifest)
                    && manifest_bin.len() < self.min_count_to_merge as usize
                {
                    Ok(Box::pin(async { Ok(manifest_bin) })
                        as Pin<
                            Box<dyn Future<Output = Result<Vec<ManifestFile>>> + Send>,
                        >)
                } else {
                    let writer = snapshot_produce.new_manifest_writer(&self.content,snapshot_produce.tx.current_table.metadata().default_partition_spec_id())?;
                    let snapshot_id = snapshot_produce.snapshot_id;
                    let file_io = snapshot_produce.tx.current_table.file_io().clone();
                    Ok((Box::pin(async move {
                        Ok(vec![
                            self.merge_bin(
                                snapshot_id,
                                file_io,
                                manifest_bin,
                                writer,
                            )
                            .await?,
                        ])
                    }))
                        as Pin<Box<dyn Future<Output = Result<Vec<ManifestFile>>> + Send>>)
                }
            })
            .collect::<Result<Vec<Pin<Box<dyn Future<Output = Result<Vec<ManifestFile>>> + Send>>>>>()?;

        let merged_bins: Vec<Vec<ManifestFile>> =
            futures::future::join_all(manifest_merge_futures.into_iter())
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;

        Ok(merged_bins.into_iter().flatten().collect())
    }

    pub(crate) async fn merge_manifest<'a>(
        &self,
        snapshot_produce: &mut SnapshotProduceAction<'a>,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        if manifests.is_empty() {
            return Ok(manifests);
        }

        let first_manifest = manifests[0].clone();

        let group_manifests = self.group_by_spec(manifests);

        let mut merge_manifests = vec![];
        for (_spec_id, manifests) in group_manifests.into_iter().rev() {
            merge_manifests.extend(
                self.merge_group(snapshot_produce, &first_manifest, manifests)
                    .await?,
            );
        }

        Ok(merge_manifests)
    }
}

trait ManifestProcess: Send + Sync {
    fn process_manifest<'a>(
        &self,
        snapshot_produce: &mut SnapshotProduceAction<'a>,
        manifests: Vec<ManifestFile>,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;
}

struct SnapshotProduceAction<'a> {
    tx: Transaction<'a>,
    snapshot_id: i64,
    key_metadata: Vec<u8>,
    commit_uuid: Uuid,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,

    // A counter used to generate unique manifest file names.
    // It starts from 0 and increments for each new manifest file.
    // Note: This counter is limited to the range of (0..u64::MAX).
    manifest_counter: RangeFrom<u64>,
}

impl<'a> SnapshotProduceAction<'a> {
    pub(crate) fn new(
        tx: Transaction<'a>,
        snapshot_id: i64,
        key_metadata: Vec<u8>,
        commit_uuid: Uuid,
        snapshot_properties: HashMap<String, String>,
    ) -> Result<Self> {
        Ok(Self {
            tx,
            snapshot_id,
            commit_uuid,
            snapshot_properties,
            added_data_files: vec![],
            added_delete_files: vec![],
            removed_data_files: vec![],
            removed_delete_files: vec![],
            manifest_counter: (0..),
            key_metadata,
        })
    }

    // Check if the partition value is compatible with the partition type.
    fn validate_partition_value(
        partition_value: &Struct,
        partition_type: &StructType,
    ) -> Result<()> {
        if partition_value.fields().len() != partition_type.fields().len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Partition value is not compatible with partition type",
            ));
        }

        for (value, field) in partition_value.fields().iter().zip(partition_type.fields()) {
            if !field
                .field_type
                .as_primitive_type()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Partition field should only be primitive type.",
                    )
                })?
                .compatible(&value.as_primitive_literal().unwrap())
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Partition value is not compatible partition type",
                ));
            }
        }
        Ok(())
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(
        &mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<&mut Self> {
        let data_files: Vec<DataFile> = data_files.into_iter().collect();
        for data_file in data_files {
            Self::validate_partition_value(
                data_file.partition(),
                self.tx.current_table.metadata().default_partition_type(),
            )?;
            if data_file.content_type() == DataContentType::Data {
                self.added_data_files.push(data_file);
            } else {
                self.added_delete_files.push(data_file);
            }
        }
        Ok(self)
    }

    /// Add remove files to the snapshot.
    #[allow(dead_code)]
    pub fn delete_files(
        &mut self,
        remove_data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<&mut Self> {
        // let data_files: Vec<DataFile> = remove_data_files.into_iter().collect();
        for data_file in remove_data_files.into_iter() {
            Self::validate_partition_value(
                data_file.partition(),
                self.tx.current_table.metadata().default_partition_type(),
            )?;
            if data_file.content_type() == DataContentType::Data {
                self.removed_data_files.push(data_file);
            } else {
                self.removed_delete_files.push(data_file);
            }
        }
        Ok(self)
    }

    fn new_manifest_writer(
        &mut self,
        content_type: &ManifestContentType,
        partition_spec_id: i32,
    ) -> Result<ManifestWriter> {
        let new_manifest_path = format!(
            "{}/{}/{}-m{}.{}",
            self.tx.current_table.metadata().location(),
            META_ROOT_PATH,
            self.commit_uuid,
            self.manifest_counter.next().unwrap(),
            DataFileFormat::Avro
        );
        let output = self
            .tx
            .current_table
            .file_io()
            .new_output(new_manifest_path)?;
        let partition_spec = self
            .tx
            .current_table
            .metadata()
            .partition_spec_by_id(partition_spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Invalid partition spec id for new manifest writer",
                )
                .with_context("partition spec id", partition_spec_id.to_string())
            })?
            .as_ref()
            .clone();
        let builder = ManifestWriterBuilder::new(
            output,
            Some(self.snapshot_id),
            self.key_metadata.clone(),
            self.tx.current_table.metadata().current_schema().clone(),
            partition_spec,
        );
        if self.tx.current_table.metadata().format_version() == FormatVersion::V1 {
            Ok(builder.build_v1())
        } else {
            match content_type {
                ManifestContentType::Data => Ok(builder.build_v2_data()),
                ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
            }
        }
    }

    // Write manifest file for added data files and return the ManifestFile for ManifestList.
    async fn write_added_manifest(
        &mut self,
        added_data_files: Vec<DataFile>,
    ) -> Result<ManifestFile> {
        let snapshot_id = self.snapshot_id;
        let format_version = self.tx.current_table.metadata().format_version();
        let content_type = {
            let mut data_num = 0;
            let mut delete_num = 0;
            for f in &added_data_files {
                match f.content_type() {
                    DataContentType::Data => data_num += 1,
                    DataContentType::PositionDeletes => delete_num += 1,
                    DataContentType::EqualityDeletes => delete_num += 1,
                }
            }
            if data_num == added_data_files.len() {
                ManifestContentType::Data
            } else if delete_num == added_data_files.len() {
                ManifestContentType::Deletes
            } else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "added DataFile for a ManifestFile should be same type (Data or Delete)",
                ));
            }
        };
        let manifest_entries = added_data_files.into_iter().map(|data_file| {
            let builder = ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .data_file(data_file);
            if format_version == FormatVersion::V1 {
                builder.snapshot_id(snapshot_id).build()
            } else {
                // For format version > 1, we set the snapshot id at the inherited time to avoid rewrite the manifest file when
                // commit failed.
                builder.build()
            }
        });
        let mut writer = self.new_manifest_writer(
            &content_type,
            self.tx.current_table.metadata().default_partition_spec_id(),
        )?;
        for entry in manifest_entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    async fn write_delete_manifest(
        &mut self,
        deleted_entries: Vec<ManifestEntry>,
    ) -> Result<Vec<ManifestFile>> {
        if deleted_entries.is_empty() {
            return Ok(vec![]);
        }

        // Group deleted entries by spec_id
        let mut partition_groups = HashMap::new();
        for entry in deleted_entries {
            partition_groups
                .entry(entry.data_file().partition_spec_id)
                .or_insert_with(Vec::new)
                .push(entry);
        }

        // Write a delete manifest per spec_id group
        let mut deleted_manifests = Vec::new();
        for (spec_id, entries) in partition_groups {
            let mut data_file_writer: Option<ManifestWriter> = None;
            let mut delete_file_writer: Option<ManifestWriter> = None;
            for entry in entries {
                match entry.content_type() {
                    DataContentType::Data => {
                        if data_file_writer.is_none() {
                            data_file_writer = Some(
                                self.new_manifest_writer(&ManifestContentType::Data, spec_id)?,
                            );
                        }
                        data_file_writer.as_mut().unwrap().add_entry(entry)?;
                    }
                    DataContentType::EqualityDeletes | DataContentType::PositionDeletes => {
                        if delete_file_writer.is_none() {
                            delete_file_writer = Some(
                                self.new_manifest_writer(&ManifestContentType::Deletes, spec_id)?,
                            );
                        }
                        delete_file_writer.as_mut().unwrap().add_entry(entry)?;
                    }
                }
            }
            if let Some(writer) = data_file_writer {
                deleted_manifests.push(writer.write_manifest_file().await?);
            }
            if let Some(writer) = delete_file_writer {
                deleted_manifests.push(writer.write_manifest_file().await?);
            }
        }

        Ok(deleted_manifests)
    }

    async fn manifest_file<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        &mut self,
        snapshot_produce_operation: &OP,
        manifest_process: &MP,
    ) -> Result<Vec<ManifestFile>> {
        let mut manifest_files = vec![];
        let data_files = std::mem::take(&mut self.added_data_files);
        let added_delete_files = std::mem::take(&mut self.added_delete_files);
        if !data_files.is_empty() {
            let added_manifest = self.write_added_manifest(data_files).await?;
            manifest_files.push(added_manifest);
        }
        if !added_delete_files.is_empty() {
            let added_delete_manifest = self.write_added_manifest(added_delete_files).await?;
            manifest_files.push(added_delete_manifest);
        }

        let delete_manifests = self
            .write_delete_manifest(snapshot_produce_operation.delete_entries(self).await?)
            .await?;
        manifest_files.extend(delete_manifests);

        let existing_manifests = snapshot_produce_operation.existing_manifest(self).await?;
        manifest_files.extend(existing_manifests);

        manifest_process
            .process_manifest(self, manifest_files)
            .await
    }

    // # TODO
    // Fulfill this function
    fn summary<OP: SnapshotProduceOperation>(&self, snapshot_produce_operation: &OP) -> Summary {
        Summary {
            operation: snapshot_produce_operation.operation(),
            additional_properties: self.snapshot_properties.clone(),
        }
    }

    fn generate_manifest_list_file_path(&self, attempt: i64) -> String {
        format!(
            "{}/{}/snap-{}-{}-{}.{}",
            self.tx.current_table.metadata().location(),
            META_ROOT_PATH,
            self.snapshot_id,
            attempt,
            self.commit_uuid,
            DataFileFormat::Avro
        )
    }

    /// Finished building the action and apply it to the transaction.
    pub async fn apply<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        mut self,
        snapshot_produce_operation: OP,
        process: MP,
    ) -> Result<Transaction<'a>> {
        let new_manifests = self
            .manifest_file(&snapshot_produce_operation, &process)
            .await?;
        let next_seq_num = self.tx.current_table.metadata().next_sequence_number();

        let summary = self.summary(&snapshot_produce_operation);

        let manifest_list_path = self.generate_manifest_list_file_path(0);

        let mut manifest_list_writer = match self.tx.current_table.metadata().format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(
                self.tx
                    .current_table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                self.tx.current_table.metadata().current_snapshot_id(),
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                self.tx
                    .current_table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                self.tx.current_table.metadata().current_snapshot_id(),
                next_seq_num,
            ),
        };
        manifest_list_writer.add_manifests(new_manifests.into_iter())?;
        manifest_list_writer.close().await?;

        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(self.tx.current_table.metadata().current_snapshot_id())
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(self.tx.current_table.metadata().current_schema_id())
            .with_timestamp_ms(commit_ts)
            .build();

        self.tx.apply(
            vec![
                TableUpdate::AddSnapshot {
                    snapshot: new_snapshot.clone(),
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: SnapshotReference::new(
                        self.snapshot_id,
                        SnapshotRetention::branch(None, None, None),
                    ),
                },
            ],
            vec![
                TableRequirement::UuidMatch {
                    uuid: self.tx.current_table.metadata().uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: self.tx.current_table.metadata().current_snapshot_id(),
                },
            ],
        )?;
        Ok(self.tx)
    }
}

/// Transaction action for replacing sort order.
pub struct ReplaceSortOrderAction<'a> {
    tx: Transaction<'a>,
    sort_fields: Vec<SortField>,
}

impl<'a> ReplaceSortOrderAction<'a> {
    /// Adds a field for sorting in ascending order.
    pub fn asc(self, name: &str, null_order: NullOrder) -> Result<Self> {
        self.add_sort_field(name, SortDirection::Ascending, null_order)
    }

    /// Adds a field for sorting in descending order.
    pub fn desc(self, name: &str, null_order: NullOrder) -> Result<Self> {
        self.add_sort_field(name, SortDirection::Descending, null_order)
    }

    /// Finished building the action and apply it to the transaction.
    pub fn apply(mut self) -> Result<Transaction<'a>> {
        let unbound_sort_order = SortOrder::builder()
            .with_fields(self.sort_fields)
            .build_unbound()?;

        let updates = vec![
            TableUpdate::AddSortOrder {
                sort_order: unbound_sort_order,
            },
            TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
        ];

        let requirements = vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: self
                    .tx
                    .current_table
                    .metadata()
                    .current_schema()
                    .schema_id(),
            },
            TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id: self
                    .tx
                    .current_table
                    .metadata()
                    .default_sort_order()
                    .order_id,
            },
        ];

        self.tx.apply(updates, requirements)?;
        Ok(self.tx)
    }

    fn add_sort_field(
        mut self,
        name: &str,
        sort_direction: SortDirection,
        null_order: NullOrder,
    ) -> Result<Self> {
        let field_id = self
            .tx
            .current_table
            .metadata()
            .current_schema()
            .field_id_by_name(name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot find field {} in table schema", name),
                )
            })?;

        let sort_field = SortField::builder()
            .source_id(field_id)
            .transform(Transform::Identity)
            .direction(sort_direction)
            .null_order(null_order)
            .build();

        self.sort_fields.push(sort_field);
        Ok(self)
    }
}

/// Transaction action for rewriting files.
#[allow(dead_code)]
pub struct RewriteFilesAction<'a> {
    snapshot_produce_action: SnapshotProduceAction<'a>,
    target_size_bytes: u32,
    min_count_to_merge: u32,
    merge_enabled: bool,
}

#[allow(dead_code)]
struct RewriteFilesOperation;

impl<'a> RewriteFilesAction<'a> {
    #[allow(dead_code)]
    fn new(
        tx: Transaction<'a>,
        snapshot_id: i64,
        commit_uuid: Uuid,
        key_metadata: Vec<u8>,
        snapshot_properties: HashMap<String, String>,
    ) -> Result<Self> {
        let target_size_bytes: u32 = tx
            .current_table
            .metadata()
            .properties()
            .get(MANIFEST_TARGET_SIZE_BYTES)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
        let min_count_to_merge: u32 = tx
            .current_table
            .metadata()
            .properties()
            .get(MANIFEST_MIN_MERGE_COUNT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MIN_MERGE_COUNT_DEFAULT);
        let merge_enabled = tx
            .current_table
            .metadata()
            .properties()
            .get(MANIFEST_MERGE_ENABLED)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MERGE_ENABLED_DEFAULT);

        Ok(Self {
            snapshot_produce_action: SnapshotProduceAction::new(
                tx,
                snapshot_id,
                key_metadata,
                commit_uuid,
                snapshot_properties,
            )
            .unwrap(),
            target_size_bytes,
            min_count_to_merge,
            merge_enabled,
        })
    }

    /// Add data files to the snapshot.

    pub fn add_data_files(
        &mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<&mut Self> {
        self.snapshot_produce_action.add_data_files(data_files)?;
        Ok(self)
    }

    /// Add remove files to the snapshot.
    pub fn delete_files(
        &mut self,
        remove_data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<&mut Self> {
        self.snapshot_produce_action
            .delete_files(remove_data_files)?;
        Ok(self)
    }

    /// Finished building the action and apply it to the transaction.
    pub async fn apply(self) -> Result<Transaction<'a>> {
        if self.merge_enabled {
            let process = MergeManifestProcess {
                target_size_bytes: self.target_size_bytes,
                min_count_to_merge: self.min_count_to_merge,
            };
            self.snapshot_produce_action
                .apply(RewriteFilesOperation, process)
                .await
        } else {
            self.snapshot_produce_action
                .apply(RewriteFilesOperation, DefaultManifestProcess)
                .await
        }
    }
}

impl SnapshotProduceOperation for RewriteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProduceAction<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // generate delete manifest entries from removed files
        let format_version = snapshot_produce
            .tx
            .current_table
            .metadata()
            .format_version();

        let gen_manifest_entry = |data_file: &DataFile| {
            let builder = ManifestEntry::builder()
                .status(ManifestStatus::Deleted)
                .data_file(data_file.clone());
            if format_version == FormatVersion::V1 {
                builder.snapshot_id(snapshot_produce.snapshot_id).build()
            } else {
                // For format version > 1, we set the snapshot id at the inherited time to avoid rewrite the manifest file when
                // commit failed.
                builder.build()
            }
        };

        let data_manifest_entries = snapshot_produce
            .removed_data_files
            .iter()
            .map(|data_file| gen_manifest_entry(data_file));

        let delete_manifest_entries = snapshot_produce
            .removed_delete_files
            .iter()
            .map(|data_file| gen_manifest_entry(data_file));

        Ok(data_manifest_entries
            .chain(delete_manifest_entries)
            .collect())
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProduceAction<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce
            .tx
            .current_table
            .metadata()
            .current_snapshot()
        else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.tx.current_table.file_io(),
                snapshot_produce.tx.current_table.metadata(),
            )
            .await?;

        Ok(manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.has_added_files() || entry.has_existing_files())
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;

    use crate::io::FileIOBuilder;
    use crate::scan::tests::TableTestFixture;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, Literal, Struct,
        TableMetadata,
    };
    use crate::table::Table;
    use crate::transaction::{Transaction, MAIN_BRANCH};
    use crate::{TableIdent, TableRequirement, TableUpdate};

    fn make_v1_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV1Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    fn make_v2_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    fn make_v2_minimal_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    fn make_v2_table_with_mutli_snapshot() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMultiSnapshot.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    #[test]
    fn test_upgrade_table_version_v1_to_v2() {
        let table = make_v1_table();
        let tx = Transaction::new(&table);
        let tx = tx.upgrade_table_version(FormatVersion::V2).unwrap();

        assert_eq!(
            vec![TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V2
            }],
            tx.updates
        );
    }

    #[test]
    fn test_upgrade_table_version_v2_to_v2() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx.upgrade_table_version(FormatVersion::V2).unwrap();

        assert!(
            tx.updates.is_empty(),
            "Upgrade table to same version should not generate any updates"
        );
        assert!(
            tx.requirements.is_empty(),
            "Upgrade table to same version should not generate any requirements"
        );
    }

    #[test]
    fn test_downgrade_table_version() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx.upgrade_table_version(FormatVersion::V1);

        assert!(tx.is_err(), "Downgrade table version should fail!");
    }

    #[test]
    fn test_set_table_property() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .set_properties(HashMap::from([("a".to_string(), "b".to_string())]))
            .unwrap();

        assert_eq!(
            vec![TableUpdate::SetProperties {
                updates: HashMap::from([("a".to_string(), "b".to_string())])
            }],
            tx.updates
        );
    }

    #[test]
    fn test_remove_property() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .remove_properties(vec!["a".to_string(), "b".to_string()])
            .unwrap();

        assert_eq!(
            vec![TableUpdate::RemoveProperties {
                removals: vec!["a".to_string(), "b".to_string()]
            }],
            tx.updates
        );
    }

    #[test]
    fn test_replace_sort_order() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx.replace_sort_order().apply().unwrap();

        assert_eq!(
            vec![
                TableUpdate::AddSortOrder {
                    sort_order: Default::default()
                },
                TableUpdate::SetDefaultSortOrder { sort_order_id: -1 }
            ],
            tx.updates
        );

        assert_eq!(
            vec![
                TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: 1
                },
                TableRequirement::DefaultSortOrderIdMatch {
                    default_sort_order_id: 3
                }
            ],
            tx.requirements
        );
    }

    #[tokio::test]
    async fn test_fast_append_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let mut action = tx.fast_append(None, None, vec![]).unwrap();

        // check add data file with incompatible partition value
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::string("test"))]))
            .build()
            .unwrap();
        assert!(action.add_data_files(vec![data_file.clone()]).is_err());

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        action.add_data_files(vec![data_file.clone()]).unwrap();
        let tx = action.apply().await.unwrap();

        // check updates and requirements
        assert!(
            matches!((&tx.updates[0],&tx.updates[1]), (TableUpdate::AddSnapshot { snapshot },TableUpdate::SetSnapshotRef { reference,ref_name }) if snapshot.snapshot_id() == reference.snapshot_id && ref_name == MAIN_BRANCH)
        );
        // requriments is based on original table metadata
        assert_eq!(
            vec![
                TableRequirement::UuidMatch {
                    uuid: table.metadata().uuid()
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: table.metadata().current_snapshot_id()
                }
            ],
            tx.requirements
        );

        // check manifest list
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &tx.updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        let manifest_list = new_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(1, manifest_list.entries().len());
        assert_eq!(
            manifest_list.entries()[0].sequence_number,
            new_snapshot.sequence_number()
        );

        // check manifset
        let manifest = manifest_list.entries()[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(1, manifest.entries().len());
        assert_eq!(
            new_snapshot.sequence_number(),
            manifest.entries()[0]
                .sequence_number()
                .expect("Inherit sequence number by load manifest")
        );

        assert_eq!(
            new_snapshot.snapshot_id(),
            manifest.entries()[0].snapshot_id().unwrap()
        );
        assert_eq!(data_file, *manifest.entries()[0].data_file());
    }

    #[tokio::test]
    async fn test_add_existing_parquet_files_to_unpartitioned_table() {
        let mut fixture = TableTestFixture::new_unpartitioned();
        fixture.setup_unpartitioned_manifest_files().await;
        let tx = crate::transaction::Transaction::new(&fixture.table);

        let file_paths = vec![
            format!("{}/1.parquet", &fixture.table_location),
            format!("{}/2.parquet", &fixture.table_location),
            format!("{}/3.parquet", &fixture.table_location),
        ];

        let fast_append_action = tx.fast_append(None, None, vec![]).unwrap();

        // Attempt to add the existing Parquet files with fast append.
        let new_tx = fast_append_action
            .add_parquet_files(file_paths.clone())
            .await
            .expect("Adding existing Parquet files should succeed");

        let mut found_add_snapshot = false;
        let mut found_set_snapshot_ref = false;
        for update in new_tx.updates.iter() {
            match update {
                TableUpdate::AddSnapshot { .. } => {
                    found_add_snapshot = true;
                }
                TableUpdate::SetSnapshotRef {
                    ref_name,
                    reference,
                } => {
                    found_set_snapshot_ref = true;
                    assert_eq!(ref_name, crate::transaction::MAIN_BRANCH);
                    assert!(reference.snapshot_id > 0);
                }
                _ => {}
            }
        }
        assert!(found_add_snapshot);
        assert!(found_set_snapshot_ref);

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &new_tx.updates[0] {
            snapshot
        } else {
            panic!("Expected the first update to be an AddSnapshot update");
        };

        let manifest_list = new_snapshot
            .load_manifest_list(fixture.table.file_io(), fixture.table.metadata())
            .await
            .expect("Failed to load manifest list");

        assert_eq!(
            manifest_list.entries().len(),
            2,
            "Expected 2 manifest list entries, got {}",
            manifest_list.entries().len()
        );

        // Load the manifest from the manifest list
        let manifest = manifest_list.entries()[0]
            .load_manifest(fixture.table.file_io())
            .await
            .expect("Failed to load manifest");

        // Check that the manifest contains three entries.
        assert_eq!(manifest.entries().len(), 3);

        // Verify each file path appears in manifest.
        let manifest_paths: Vec<String> = manifest
            .entries()
            .iter()
            .map(|entry| entry.data_file().file_path.clone())
            .collect();
        for path in file_paths {
            assert!(manifest_paths.contains(&path));
        }
    }

    #[tokio::test]
    async fn test_transaction_apply_upgrade() {
        let table = make_v1_table();
        let tx = Transaction::new(&table);
        // Upgrade v1 to v1, do nothing.
        let tx = tx.upgrade_table_version(FormatVersion::V1).unwrap();
        // Upgrade v1 to v2, success.
        let tx = tx.upgrade_table_version(FormatVersion::V2).unwrap();
        assert_eq!(
            vec![TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V2
            }],
            tx.updates
        );
        // Upgrade v2 to v1, return error.
        assert!(tx.upgrade_table_version(FormatVersion::V1).is_err());
    }

    #[tokio::test]
    async fn test_remove_snapshot_action() {
        let table = make_v2_table_with_mutli_snapshot();
        let table_meta = table.metadata().clone();
        assert_eq!(5, table_meta.snapshots().count());
        {
            let tx = Transaction::new(&table);
            let tx = tx.expire_snapshot().apply().await.unwrap();
            assert_eq!(4, tx.updates.len());

            assert_eq!(
                vec![
                    TableRequirement::UuidMatch {
                        uuid: tx.current_table.metadata().uuid()
                    },
                    TableRequirement::RefSnapshotIdMatch {
                        r#ref: MAIN_BRANCH.to_string(),
                        snapshot_id: tx.current_table.metadata().current_snapshot_id
                    }
                ],
                tx.requirements
            );
        }

        {
            let tx = Transaction::new(&table);
            let tx = tx.expire_snapshot().retain_last(2).apply().await.unwrap();
            assert_eq!(3, tx.updates.len());

            assert_eq!(
                vec![
                    TableRequirement::UuidMatch {
                        uuid: tx.current_table.metadata().uuid()
                    },
                    TableRequirement::RefSnapshotIdMatch {
                        r#ref: MAIN_BRANCH.to_string(),
                        snapshot_id: tx.current_table.metadata().current_snapshot_id
                    }
                ],
                tx.requirements
            );
        }

        {
            let tx = Transaction::new(&table);
            let tx = tx
                .expire_snapshot()
                .retain_last(100)
                .expire_older_than(100)
                .apply()
                .await
                .unwrap();
            assert_eq!(0, tx.updates.len());
            assert_eq!(
                vec![
                    TableRequirement::UuidMatch {
                        uuid: tx.current_table.metadata().uuid()
                    },
                    TableRequirement::RefSnapshotIdMatch {
                        r#ref: MAIN_BRANCH.to_string(),
                        snapshot_id: tx.current_table.metadata().current_snapshot_id
                    }
                ],
                tx.requirements
            );
        }

        {
            // test remove main current snapshot
            let tx = Transaction::new(&table);
            let err = tx
                .expire_snapshot()
                .expire_snapshot_id(table.metadata().current_snapshot_id().unwrap())
                .apply()
                .await
                .err()
                .unwrap();
            assert_eq!(
                "DataInvalid => Cannot remove snapshot 3067729675574597004 with retained references: [\"main\"]",
                err.to_string()
            )
        }
    }
}
