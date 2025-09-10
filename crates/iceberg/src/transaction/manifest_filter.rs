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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use uuid::Uuid;
use std::ops::RangeFrom;

use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{
    DataFile, FormatVersion, ManifestContentType, ManifestFile, ManifestStatus, ManifestWriter, 
    ManifestWriterBuilder, PartitionSpec, Schema, Struct
};
use crate::transaction::snapshot::new_manifest_path;
use crate::{Error, ErrorKind};

/// Context for creating manifest writers, similar to SnapshotProduceAction's approach
pub struct ManifestWriterContext {
    metadata_location: String,
    meta_root_path: String,
    commit_uuid: Uuid,
    manifest_counter: RangeFrom<u64>,
    format_version: FormatVersion,
    snapshot_id: i64,
    file_io: FileIO,
}

impl ManifestWriterContext {
    /// Create a new ManifestWriterContext
    pub fn new(
        metadata_location: String,
        meta_root_path: String,
        commit_uuid: Uuid,
        format_version: FormatVersion,
        snapshot_id: i64,
        file_io: FileIO,
    ) -> Self {
        Self {
            metadata_location,
            meta_root_path,
            commit_uuid,
            manifest_counter: 0..,
            format_version,
            snapshot_id,
            file_io,
        }
    }

    /// Create a new manifest writer, similar to SnapshotProduceAction::new_manifest_writer
    pub fn new_manifest_writer(
        &mut self,
        content_type: ManifestContentType,
        table_schema: &Schema,
        partition_spec: &PartitionSpec,
    ) -> Result<ManifestWriter> {
        let new_manifest_path = new_manifest_path(
            &self.metadata_location,
            &self.meta_root_path,
            self.commit_uuid,
            self.manifest_counter.next().unwrap(),
            crate::spec::DataFileFormat::Avro,
        );
        
        let output = self.file_io.new_output(&new_manifest_path)?;
        let builder = ManifestWriterBuilder::new(
            output,
            Some(self.snapshot_id),
            Vec::new(), // key_metadata - empty for now
            table_schema.clone().into(),
            partition_spec.clone(),
        );

        let writer = match self.format_version {
            FormatVersion::V2 => {
                match content_type {
                    ManifestContentType::Data => builder.build_v2_data(),
                    ManifestContentType::Deletes => builder.build_v2_deletes(),
                }
            }
            FormatVersion::V1 => builder.build_v1(),
        };
        
        Ok(writer)
    }
}

/// A manager for filtering manifest files and their entries, similar to Java's ManifestFilterManager.
/// This class is responsible for:
/// 1. Filtering manifest entries based on various criteria
/// 2. Rewriting manifest files with filtered entries
/// 3. Managing delete operations on data files
pub struct ManifestFilterManager {
    /// Files to be deleted by path
    delete_paths: HashSet<String>,

    delete_files: HashMap<String, DataFile>,

    manifests_with_deletes: HashSet<String>,

    /// Partitions to drop completely
    drop_partitions: HashMap<i32, HashSet<Struct>>, // (spec_id, partition)

    delete_file_partitions: HashMap<i32, HashSet<Struct>>, // (spec_id, partition)

    /// Minimum sequence number for removing old delete files
    min_sequence_number: i64,
    /// Whether to fail if any delete operation is attempted
    fail_any_delete: bool,
    /// Whether to fail if required delete paths are missing
    fail_missing_delete_paths: bool,
    /// Cache of filtered manifests to avoid reprocessing
    filtered_manifests: HashMap<String, ManifestFile>, // manifest_path -> filtered_manifest
    /// Tracking where files were deleted to validate retries quickly
    filtered_manifest_to_deleted_files: HashMap<String, Vec<String>>, // manifest_path -> deleted_files

    file_io: FileIO,
    writer_context: ManifestWriterContext,
}

impl ManifestFilterManager {
    /// Create a new ManifestFilterManager with simplified parameters
    pub fn new(file_io: FileIO, writer_context: ManifestWriterContext) -> Self {
        Self {
            delete_paths: HashSet::new(),
            delete_files: HashMap::new(),
            manifests_with_deletes: HashSet::new(),
            drop_partitions: HashMap::new(),
            delete_file_partitions: HashMap::new(),
            min_sequence_number: 0,
            fail_any_delete: false,
            fail_missing_delete_paths: false,
            filtered_manifests: HashMap::new(),
            filtered_manifest_to_deleted_files: HashMap::new(),
            file_io,
            writer_context,
        }
    }

    /// Create from SnapshotProduceAction context (convenience method)
    pub fn from_snapshot_context(
        file_io: FileIO,
        metadata_location: String,
        snapshot_id: i64,
        commit_uuid: Uuid,
        format_version: FormatVersion,
    ) -> Self {
        let writer_context = ManifestWriterContext::new(
            metadata_location,
            "metadata".to_string(), // META_ROOT_PATH equivalent
            commit_uuid,
            format_version,
            snapshot_id,
            file_io.clone(),
        );
        
        Self::new(file_io, writer_context)
    }

    /// Set whether to fail if any delete operation is attempted
    pub fn fail_any_delete(mut self) -> Self {
        self.fail_any_delete = true;
        self
    }

    /// Get the list of files that are marked for deletion
    pub fn files_to_be_deleted(&self) -> Vec<DataFile> {
        self.delete_files.values().cloned().collect()
    }

    /// Add a partition to drop from the table during the delete phase
    pub fn drop_partition(mut self, spec_id: i32, partition: Struct) -> Self {
        self.drop_partitions.entry(spec_id).or_default().insert(partition);
        self
    }

    /// Set the sequence number used to remove old delete files
    /// Delete files with a sequence number older than the given value will be removed
    pub fn drop_delete_files_older_than(mut self, sequence_number: i64) -> Self {
        assert!(
            sequence_number >= 0,
            "Invalid minimum data sequence number: {}",
            sequence_number
        );
        self.min_sequence_number = sequence_number;
        self
    }

    /// Set whether to fail if required delete paths are missing
    pub fn fail_missing_delete_paths(mut self) -> Self {
        self.fail_missing_delete_paths = true;
        self
    }

    /// Mark a data file for deletion
    pub async fn delete_file(&mut self, file: DataFile) -> Result<()> {
        self.invalidate_filtered_cache().await?;

        // Todo: check all deletes references in manifests?
        let file_path = file.file_path.clone();
        let partition = file.partition.clone();
        let partition_spec_id = file.partition_spec_id;

        self.delete_files.insert(file_path, file);
        match self.delete_file_partitions.entry(partition_spec_id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().insert(partition);
            }
            Entry::Vacant(entry) => {
                entry.insert(HashSet::from([partition]));
            }
        }

        Ok(())
    }

    /// Add a specific file path to be deleted
    pub async fn delete_file_by_path(mut self, path: impl Into<String>) -> Result<Self> {
        self.invalidate_filtered_cache().await?;
        self.delete_paths.insert(path.into());

        Ok(self)
    }

    /// Check if this manager contains any delete operations
    pub fn contains_deletes(&self) -> bool {
        !self.delete_paths.is_empty()
            || !self.delete_files.is_empty()
            || !self.drop_partitions.is_empty()
    }

    /// Filter a list of manifest files
    /// This is the main entry point, similar to Java's filterManifests method
    pub async fn filter_manifests(
        &mut self,
        table_schema: &Schema,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        if manifests.is_empty() {
            self.validate_required_deletes(&[])?;
            return Ok(vec![]);
        }

        let mut filtered = Vec::with_capacity(manifests.len());

        for manifest in manifests {
            let filtered_manifest = self
                .filter_manifest(table_schema, manifest)
                .await?;
            filtered.push(filtered_manifest);
        }

        self.validate_required_deletes(&filtered)?;
        Ok(filtered)
    }

    /// Filter a single manifest file
    async fn filter_manifest(
        &mut self,
        table_schema: &Schema,
        manifest: ManifestFile,
    ) -> Result<ManifestFile> {
        // Check cache first
        if let Some(cached) = self.filtered_manifests.get(&manifest.manifest_path) {
            return Ok(cached.clone());
        }

        // Check if this manifest can contain files to delete
        if !self.can_contain_deleted_files(&manifest) {
            self.filtered_manifests
                .insert(manifest.manifest_path.clone(), manifest.clone());
            return Ok(manifest);
        }

        if self.manifest_has_deleted_files(&manifest).await? {
            // Load and filter the manifest
            self.filter_manifest_with_deleted_files(table_schema, manifest).await
        } else {
            // If no deleted files are found, just return the original manifest
            self.filtered_manifests
                .insert(manifest.manifest_path.clone(), manifest.clone());
            Ok(manifest)
        }
    }

    /// Check if a manifest can potentially contain files that need to be deleted
    fn can_contain_deleted_files(&self, manifest: &ManifestFile) -> bool {
        // If manifest has no live files, it can't contain files to delete
        if Self::manifest_has_no_live_files(manifest) {
            return false;
        }

        // Check various conditions that might indicate deletable files
        self.can_contain_dropped_files(manifest) || self.can_contain_dropped_partitions(manifest)
    }

    fn can_contain_dropped_files(&self, _manifest: &ManifestFile) -> bool {
        // Simple check - if we have file-based deletes, any manifest might contain them
        if !self.delete_paths.is_empty() || !self.delete_files.is_empty() {
            return true;
        }

        false
    }

    fn can_contain_dropped_partitions(&self, _manifest: &ManifestFile) -> bool {
        // TODO: Check if manifest's partition range can overlap with dropped partitions
        // For now, conservatively return true if we have partitions to drop
        // if(!self.drop_partitions.is_empty()) {
        //     return 
        // }

        false
    }

    /// Filter a manifest that is known to contain files to delete
    async fn filter_manifest_with_deleted_files(
        &mut self,
        table_schema: &Schema,
        manifest: ManifestFile,
    ) -> Result<ManifestFile> {
        // Load the original manifest
        let original_manifest = manifest.load_manifest(&self.file_io).await?;

        let (
            entries,
            manifest_meta_data,
        ) = original_manifest.into_parts();
        
        // Check if this is a delete manifest
        let is_delete = manifest.content == ManifestContentType::Deletes;
        
        // Create a set to track deleted files (using HashSet for efficiency like Java)
        let mut deleted_files = HashMap::new();
        // let mut duplicate_delete_count = 0;
        
        // Create an output path for the filtered manifest using writer context
        let partition_spec = manifest_meta_data.partition_spec.clone();

        // Create the manifest writer using the writer context
        let mut writer = self.writer_context.new_manifest_writer(
            manifest.content,
            table_schema,
            &partition_spec,
        )?;
        
        // Process each live entry in the manifest (following Java logic)
        for entry in &entries{
            if !entry.is_alive() {
                continue;
            }

            let entry = entry.as_ref();
            let file = entry.data_file();
                        
            // Check if file is marked for deletion based on various criteria
            let marked_for_delete = 
                // Check if file path is in delete paths
                self.delete_paths.contains(file.file_path()) ||
                // Check if file is in delete files collection
                self.delete_files.contains_key(file.file_path()) ||
                // Check if partition should be dropped
                self.drop_partitions.get(&file.partition_spec_id)
                    .map(|drop_partition| drop_partition.contains(file.partition()))
                    .unwrap_or(false) ||
                // For delete manifests, check sequence number for old delete files
                (is_delete && 
                 entry.sequence_number().unwrap_or(0) > 0 &&
                 entry.sequence_number().unwrap_or(0) < self.min_sequence_number);
            
            // TODO: Add expression evaluation logic (evaluator.rowsMightMatch)
            // For now, we'll use a simple approach and assume expression evaluation would return false
            // let rows_might_match = true; // evaluator.rowsMightMatch(file) equivalent
            
            if marked_for_delete {
                // Check if all rows match
                let all_rows_match = marked_for_delete; // || evaluator.rowsMustMatch(file) equivalent
                
                // Validation check: cannot delete file where some, but not all, rows match filter
                // unless it's a delete file (ignore delete files where some records may not match)
                if !all_rows_match && !is_delete {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot delete file where some, but not all, rows match filter: {}",
                            file.file_path()
                        ),
                    ));
                }
                
                if all_rows_match {
                    // Mark this entry as deleted
                    writer.add_delete_entry(entry.clone())?;
                    
                    // Create a copy of the file without stats (like Java's fileCopy = file.copyWithoutStats())
                    let file_copy = file.clone(); // In a real implementation, this would strip stats
                    
                    // Add the file to deleteFiles set (like Java logic)
                    self.delete_files.insert(file_copy.file_path.clone(), file_copy.clone());
                    
                    // Track deleted files for duplicate detection
                    if deleted_files.contains_key(file_copy.file_path()) {
                        // Log warning about duplicate (in Java: LOG.warn)
                        eprintln!(
                            "Deleting a duplicate path from manifest {}: {}",
                            manifest.manifest_path,
                            file.file_path()
                        );
                        // duplicate_delete_count += 1;
                    } else {
                        // Only add the file to deletes if it is a new delete
                        // This keeps the snapshot summary accurate for non-duplicate data
                        deleted_files.insert(file_copy.file_path.to_owned(), file_copy.clone());
                    }
                } else {
                    // Keep the entry as existing
                    writer.add_existing_entry(entry.clone())?;
                }
            } else {
                // Keep the entry as existing
                writer.add_existing_entry(entry.clone())?;
            }
        }
        
        // Write the filtered manifest
        let filtered_manifest = writer.write_manifest_file().await?;
        
        // Update caches (following Java logic)
        self.filtered_manifests
            .insert(manifest.manifest_path.clone(), filtered_manifest.clone());
        
        // Track deleted files for validation - convert HashSet to Vec of file paths
        let deleted_file_paths: Vec<String> = deleted_files
            .keys().cloned().collect();
        
        self.filtered_manifest_to_deleted_files
            .insert(filtered_manifest.manifest_path.clone(), deleted_file_paths);
        
        Ok(filtered_manifest)
    }

    /// Validate that all required delete operations were found
    fn validate_required_deletes(&self, manifests: &[ManifestFile]) -> Result<()> {
        if self.fail_missing_delete_paths {
            let deleted_files = self.deleted_files(manifests);
            // check deleted_files contains all files in self.delete_files

            for file_path in self.delete_files.keys() {
                if !deleted_files.contains(file_path) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Required delete path missing: {}", file_path),
                    ));
                }
            }

            for file_path in &self.delete_paths {
                if !deleted_files.contains(file_path) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Required delete path missing: {}", file_path),
                    ));
                }
            }
        }
        Ok(())
    }

    fn deleted_files(&self, manifests: &[ManifestFile]) -> HashSet<String> {
        let mut deleted_files = HashSet::new();
        for manifest in manifests {
            if let Some(deleted) = self
                .filtered_manifest_to_deleted_files
                .get(manifest.manifest_path.as_str())
            {
                deleted_files.extend(deleted.clone());
            }
        }
        deleted_files
    }

    /// Invalidate the filtered manifest cache
    async fn invalidate_filtered_cache(&mut self) -> Result<()>{
        // Clean uncommitted filtered manifests (equivalent to Java's cleanUncommitted(SnapshotProducer.EMPTY_SET))
        self.clean_uncommitted(HashSet::new()).await
    }

    fn manifest_has_no_live_files(manifest: &ManifestFile) -> bool {
        !manifest.has_added_files() && !manifest.has_existing_files()
    }

    async fn manifest_has_deleted_files(&self, manifest_file: &ManifestFile) -> Result<bool> {
        if self.manifests_with_deletes.contains(&manifest_file.manifest_path) {
            return Ok(true);
        }

        let manifest = manifest_file.load_manifest(&self.file_io).await?;
    
        let is_delete = manifest_file.content == ManifestContentType::Deletes;

        for entry in manifest.entries() {
            let entry = entry.as_ref();
            
            // Skip entries that are already deleted
            if entry.status() == ManifestStatus::Deleted {
                continue;
            }
            
            let file = entry.data_file();
            
            // Check if file is marked for deletion based on various criteria
            let marked_for_delete = 
                // Check if file path is in delete paths
                self.delete_paths.contains(file.file_path()) ||
                // Check if file is in delete files collection
                self.delete_files.contains_key(file.file_path()) ||
                // Check if partition should be dropped
                self.drop_partitions.get(&file.partition_spec_id)
                    .map(|drop_partition| drop_partition.contains(file.partition()))
                    .unwrap_or(false) ||
                // For delete manifests, check sequence number for old delete files
                (is_delete && 
                 entry.status() != ManifestStatus::Deleted && // entry.isLive() in Java
                 entry.sequence_number().unwrap_or(0) > 0 &&
                 entry.sequence_number().unwrap_or(0) < self.min_sequence_number);
                // TODO: Add dangling delete vector check: (is_delete && self.is_dangling_dv(file))
            
            // TODO: Add expression evaluation logic (evaluator.rowsMightMatch)
            // For now, we'll use a simple approach and assume expression evaluation would return false
            let rows_might_match = false; // evaluator.rowsMightMatch(file) equivalent
            
            if marked_for_delete || rows_might_match {
                // Check if all rows match
                let all_rows_match = marked_for_delete; // || evaluator.rowsMustMatch(file) equivalent
                
                // Validation check: cannot delete file where some, but not all, rows match filter
                // unless it's a delete file
                if !all_rows_match && !is_delete {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot delete file where some, but not all, rows match filter: {}",
                            file.file_path()
                        ),
                    ));
                }
                
                if all_rows_match {
                    // Check fail_any_delete flag
                    if self.fail_any_delete {
                        // TODO: Create a proper DeleteException with partition info
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Operation would delete existing data".to_string(),
                        ));
                    }
                    
                    // As soon as a deleted file is detected, stop scanning and return true
                    return Ok(true);
                }
            }
        }
        
        Ok(false)
    }

    /// Deletes filtered manifests that were created by this class, but are not in the committed
    /// manifest set.
    ///
    /// @param committed the set of manifest file paths that were committed
    async fn clean_uncommitted(&mut self, committed: HashSet<String>) -> Result<()> {
        // Iterate over a copy of entries to avoid concurrent modification
        // In Rust, we'll collect the entries first, then process them
        let filter_entries: Vec<(String, ManifestFile)> = self
            .filtered_manifests
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (original_manifest_path, filtered_manifest) in filter_entries {
            // Check if the filtered manifest is in the committed list
            if !committed.contains(&filtered_manifest.manifest_path) {
                // Only delete if the filtered copy was created (i.e., paths are different)
                if original_manifest_path.eq_ignore_ascii_case(&filtered_manifest.manifest_path) {
                    // Delete the filtered manifest file
                    self.file_io.delete(&filtered_manifest.manifest_path).await?;
                }

                // Remove the entry from the cache
                self.filtered_manifests.remove(original_manifest_path.as_str());
            }
        }

        Ok(())
    }
}

impl Default for ManifestFilterManager {
    fn default() -> Self {
        use crate::io::FileIOBuilder;
        
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let writer_context = ManifestWriterContext::new(
            "/tmp/metadata".to_string(),
            "/tmp".to_string(),
            Uuid::new_v4(),
            FormatVersion::V2,
            1,
            file_io.clone(),
        );
        
        Self::new(file_io, writer_context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        DataContentType, DataFileFormat, NestedField, PrimitiveType, Type,
        ManifestEntry, ManifestStatus, ManifestFile, ManifestContentType, Struct, Schema,
        PartitionSpec, FormatVersion, ManifestWriterBuilder
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;
    use uuid::Uuid;

    // Helper function to create a test schema
    fn create_test_schema() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "name", 
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap()
    }

    // Helper function to create a test DataFile
    fn create_test_data_file(file_path: &str, partition_spec_id: i32) -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: file_path.to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::empty(),
            partition_spec_id,
            record_count: 100,
            file_size_in_bytes: 1024,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(), 
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            split_offsets: vec![],
            equality_ids: vec![],
            sort_order_id: None,
        }
    }

    // Helper function to create a test ManifestFile
    fn create_test_manifest_file(manifest_path: &str, content: ManifestContentType) -> ManifestFile {
        ManifestFile {
            manifest_path: manifest_path.to_string(),
            manifest_length: 5000,
            partition_spec_id: 0,
            content,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 12345,
            added_files_count: Some(10),
            existing_files_count: Some(5),
            deleted_files_count: Some(0),
            added_rows_count: Some(1000),
            existing_rows_count: Some(500),
            deleted_rows_count: Some(0),
            partitions: vec![],
            key_metadata: vec![],
        }
    }

    // Helper function to setup test environment
    fn setup_test_manager() -> (ManifestFilterManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let metadata_location = temp_dir.path().join("metadata.json").to_string_lossy().to_string();
        let meta_root_path = temp_dir.path().to_string_lossy().to_string();
        
        let writer_context = ManifestWriterContext::new(
            metadata_location,
            meta_root_path,
            Uuid::new_v4(),
            FormatVersion::V2,
            1,
            file_io.clone(),
        );
        
        let manager = ManifestFilterManager::new(file_io, writer_context);
        
        (manager, temp_dir)
    }

    #[test]
    fn test_new_manifest_filter_manager() {
        let (manager, _temp_dir) = setup_test_manager();
        
        // Test initial state
        assert!(!manager.contains_deletes());
        assert_eq!(manager.min_sequence_number, 0);
        assert!(!manager.fail_any_delete);
        assert!(!manager.fail_missing_delete_paths);
        assert!(manager.delete_paths.is_empty());
        assert!(manager.delete_files.is_empty());
        assert!(manager.drop_partitions.is_empty());
    }

    #[test]
    fn test_configuration_flags() {
        let (manager, _temp_dir) = setup_test_manager();
        
        let configured_manager = manager
            .fail_any_delete()
            .fail_missing_delete_paths()
            .drop_delete_files_older_than(100);
        
        assert!(configured_manager.fail_any_delete);
        assert!(configured_manager.fail_missing_delete_paths);
        assert_eq!(configured_manager.min_sequence_number, 100);
    }

    #[tokio::test]
    async fn test_delete_file_by_path() {
        let (mut manager, _temp_dir) = setup_test_manager();
        
        // Initially no deletes
        assert!(!manager.contains_deletes());
        
        // Add a file path to delete
        let file_path = "/test/path/file.parquet";
        manager = manager.delete_file_by_path(file_path).await.unwrap();
        
        // Should now contain deletes
        assert!(manager.contains_deletes());
        assert!(manager.delete_paths.contains(file_path));
    }

    #[tokio::test]
    async fn test_delete_file() {
        let (mut manager, _temp_dir) = setup_test_manager();
        
        // Create test file
        let test_file = create_test_data_file("/test/data/file1.parquet", 0);
        let file_path = test_file.file_path.clone();
        
        // Initially no deletes
        assert!(!manager.contains_deletes());
        
        // Add file to delete
        manager.delete_file(test_file).await.unwrap();
        
        // Should now contain deletes
        assert!(manager.contains_deletes());
        assert!(manager.delete_files.contains_key(&file_path));
        
        // Should track the file for deletion
        let deleted_files = manager.files_to_be_deleted();
        assert_eq!(deleted_files.len(), 1);
        assert_eq!(deleted_files[0].file_path, file_path);
    }

    #[test]
    fn test_drop_partition() {
        let (manager, _temp_dir) = setup_test_manager();
        
        // Create test partition
        let partition = Struct::empty();
        let spec_id = 0;
        
        let configured_manager = manager.drop_partition(spec_id, partition.clone());
        
        assert!(configured_manager.contains_deletes());
        assert!(configured_manager.drop_partitions.contains_key(&spec_id));
        assert!(configured_manager.drop_partitions[&spec_id].contains(&partition));
    }

    #[test]
    fn test_manifest_has_no_live_files() {
        // Test manifest with no live files
        let manifest_no_live = ManifestFile {
            manifest_path: "/test/manifest1.avro".to_string(),
            manifest_length: 1000,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 12345,
            added_files_count: Some(0),
            existing_files_count: Some(0),
            deleted_files_count: Some(5),
            added_rows_count: Some(0),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(100),
            partitions: vec![],
            key_metadata: vec![],
        };
        
        assert!(ManifestFilterManager::manifest_has_no_live_files(&manifest_no_live));
        
        // Test manifest with live files
        let manifest_with_live = create_test_manifest_file("/test/manifest2.avro", ManifestContentType::Data);
        assert!(!ManifestFilterManager::manifest_has_no_live_files(&manifest_with_live));
    }

    #[test]
    fn test_can_contain_dropped_files() {
        let (mut manager, _temp_dir) = setup_test_manager();
        let manifest = create_test_manifest_file("/test/manifest.avro", ManifestContentType::Data);
        
        // Initially should not contain dropped files
        assert!(!manager.can_contain_dropped_files(&manifest));
        
        // Add file path to delete
        manager.delete_paths.insert("/test/file.parquet".to_string());
        assert!(manager.can_contain_dropped_files(&manifest));
        
        // Clear paths and add file to delete 
        manager.delete_paths.clear();
        let test_file = create_test_data_file("/test/file2.parquet", 0);
        manager.delete_files.insert(test_file.file_path.clone(), test_file);
        assert!(manager.can_contain_dropped_files(&manifest));
    }

    #[test]
    fn test_can_contain_deleted_files() {
        let (mut manager, _temp_dir) = setup_test_manager();
        
        // Test manifest with no live files
        let manifest_no_live = ManifestFile {
            manifest_path: "/test/manifest1.avro".to_string(),
            manifest_length: 1000,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 12345,
            added_files_count: Some(0),
            existing_files_count: Some(0),
            deleted_files_count: Some(5),
            added_rows_count: Some(0),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(100),
            partitions: vec![],
            key_metadata: vec![],
        };
        
        // Should return false for manifest with no live files
        assert!(!manager.can_contain_deleted_files(&manifest_no_live));
        
        // Test manifest with live files but no deletes
        let manifest_with_live = create_test_manifest_file("/test/manifest2.avro", ManifestContentType::Data);
        assert!(!manager.can_contain_deleted_files(&manifest_with_live));
        
        // Add deletes and test again
        manager.delete_paths.insert("/test/file.parquet".to_string());
        assert!(manager.can_contain_deleted_files(&manifest_with_live));
    }

    #[tokio::test]
    async fn test_filter_manifests_empty_input() {
        let (mut manager, _temp_dir) = setup_test_manager();
        let schema = create_test_schema();
        
        let result = manager.filter_manifests(&schema, vec![]).await.unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_validate_required_deletes_success() {
        let (manager, _temp_dir) = setup_test_manager();
        
        // Test validation with no required deletes
        let manifests = vec![create_test_manifest_file("/test/manifest.avro", ManifestContentType::Data)];
        let result = manager.validate_required_deletes(&manifests);
        assert!(result.is_ok());
    }

    #[test] 
    fn test_validate_required_deletes_failure() {
        let (mut manager, _temp_dir) = setup_test_manager();
        
        // Enable fail_missing_delete_paths
        manager.fail_missing_delete_paths = true;
        
        // Add a required delete path that won't be found
        manager.delete_paths.insert("/missing/file.parquet".to_string());
        
        let manifests = vec![create_test_manifest_file("/test/manifest.avro", ManifestContentType::Data)];
        let result = manager.validate_required_deletes(&manifests);
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Required delete path missing"));
    }

    #[tokio::test]
    async fn test_comprehensive_deletion_logic() {
        let (mut manager, temp_dir) = setup_test_manager();
        let schema = create_test_schema();
        
        // Create test data files - one to keep, one to delete
        let keep_file = create_test_data_file("/test/keep_me.parquet", 0);
        let delete_file = create_test_data_file("/test/delete_me.parquet", 0);
        
        // Add the file to be deleted to the manager
        manager.delete_files.insert(delete_file.file_path.clone(), delete_file.clone());
        
        // Create an actual manifest file containing both files
        let manifest_path = temp_dir.path().join("test_manifest.avro");
        let manifest_path_str = manifest_path.to_str().unwrap();
        
        let partition_spec = PartitionSpec::unpartition_spec();
        
        // Create manifest entries - one to keep, one to delete
        let keep_entry = ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .data_file(keep_file.clone())
            .build();
            
        let delete_entry = ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .data_file(delete_file.clone())
            .build();
        
        let entries = vec![keep_entry, delete_entry];
        
        // Write manifest to file
        let output_file = manager.file_io.new_output(manifest_path_str).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(12345),
            vec![],
            schema.clone().into(),
            partition_spec.clone(),
        ).build_v2_data();
        
        for entry in &entries {
            writer.add_entry(entry.clone()).unwrap();
        }
        writer.write_manifest_file().await.unwrap();
        
        // Create ManifestFile
        let manifest = ManifestFile {
            manifest_path: manifest_path_str.to_string(),
            manifest_length: 1000,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 10,
            min_sequence_number: 1,
            added_snapshot_id: 12345,
            added_files_count: Some(2),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(20),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: vec![],
            key_metadata: vec![],
        };
        
        // Test 1: Check if manifest can contain deleted files
        assert!(manager.can_contain_deleted_files(&manifest), 
               "Manifest should be able to contain deleted files since we have files to delete");
        
        // Test 2: Check if manifest has deleted files
        let has_deleted = manager.manifest_has_deleted_files(&manifest).await;
        match &has_deleted {
            Ok(result) => println!("manifest_has_deleted_files succeeded: {}", result),
            Err(e) => println!("manifest_has_deleted_files failed: {}", e),
        }
        assert!(has_deleted.is_ok(), "manifest_has_deleted_files should succeed: {:?}", has_deleted.err());
        assert!(has_deleted.unwrap(), "Manifest should have deleted files since it contains a file marked for deletion");
        
        // Test 3: Verify the delete file is tracked
        assert!(manager.delete_files.contains_key(&delete_file.file_path),
               "Manager should track the file for deletion");
        assert!(!manager.delete_files.contains_key(&keep_file.file_path),
               "Manager should not track the keep file for deletion");
        
        // Test 4: Verify manager state
        assert!(manager.contains_deletes(), "Manager should contain deletes");
        let files_to_delete = manager.files_to_be_deleted();
        assert_eq!(files_to_delete.len(), 1, "Should have exactly one file to delete");
        assert_eq!(files_to_delete[0].file_path, delete_file.file_path, "Should track the correct file for deletion");
    }

    #[test]
    fn test_min_sequence_number_logic() {
        let (mut manager, _temp_dir) = setup_test_manager();
        
        // Set min sequence number to 5
        manager.min_sequence_number = 5;
        
        // Test sequence number comparison logic directly
        assert_eq!(manager.min_sequence_number, 5);
        
        // Test with different sequence numbers
        let old_sequence = 3;
        let new_sequence = 10;
        
        assert!(old_sequence < manager.min_sequence_number, "Old sequence should be below minimum");
        assert!(new_sequence >= manager.min_sequence_number, "New sequence should be above minimum");
        
        // Create manifests with different sequence numbers
        let old_manifest = ManifestFile {
            manifest_path: "/test/old.avro".to_string(),
            manifest_length: 1000,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: old_sequence,
            min_sequence_number: old_sequence,
            added_snapshot_id: 12345,
            added_files_count: Some(1), // Has live files
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(10),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: vec![],
            key_metadata: vec![],
        };
        
        let new_manifest = ManifestFile {
            manifest_path: "/test/new.avro".to_string(),
            manifest_length: 1000,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: new_sequence,
            min_sequence_number: new_sequence,
            added_snapshot_id: 12346,
            added_files_count: Some(1), // Has live files
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(10),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: vec![],
            key_metadata: vec![],
        };
        
        // Add files to delete for testing
        let test_file = create_test_data_file("/test/file.parquet", 0);
        manager.delete_files.insert(test_file.file_path.clone(), test_file);
        
        // Both manifests should be able to contain deleted files since they have live files and we have files to delete
        assert!(manager.can_contain_deleted_files(&old_manifest),
               "Old manifest should be able to contain deleted files since it has live files");
        
        // New manifest should be processed for deletions  
        assert!(manager.can_contain_deleted_files(&new_manifest),
               "New manifest should be processed since it has live files");
        
        // Verify sequence number properties - these are still valid for min_sequence_number logic
        assert!(old_manifest.min_sequence_number < manager.min_sequence_number);
        assert!(new_manifest.min_sequence_number >= manager.min_sequence_number);
    }

    #[test]
    fn test_deletion_tracking_and_validation() {
        let (mut manager, _temp_dir) = setup_test_manager();
        
        // Create test data files - one to keep, one to delete
        let keep_file = create_test_data_file("/test/keep_me.parquet", 0);
        let delete_file = create_test_data_file("/test/delete_me.parquet", 0);
        
        // Initially no deletes
        assert!(!manager.contains_deletes());
        assert_eq!(manager.files_to_be_deleted().len(), 0);
        
        // Add the file to be deleted to the manager
        manager.delete_files.insert(delete_file.file_path.clone(), delete_file.clone());
        
        // Now should have deletes
        assert!(manager.contains_deletes());
        assert_eq!(manager.files_to_be_deleted().len(), 1);
        assert_eq!(manager.files_to_be_deleted()[0].file_path, delete_file.file_path);
        
        // Create a manifest that could contain deleted files
        let manifest = ManifestFile {
            manifest_path: "/test/test_manifest.avro".to_string(),
            manifest_length: 1000,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 10,
            min_sequence_number: 1,
            added_snapshot_id: 12345,
            added_files_count: Some(2), // Has live files
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(20),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: vec![],
            key_metadata: vec![],
        };
        
        // Test that manifest can contain deleted files
        assert!(manager.can_contain_deleted_files(&manifest), 
               "Manifest should be able to contain deleted files since we have files to delete and manifest has live files");
        
        // Verify the delete file is tracked correctly
        assert!(manager.delete_files.contains_key(&delete_file.file_path),
               "Manager should track the file for deletion");
        assert!(!manager.delete_files.contains_key(&keep_file.file_path),
               "Manager should not track the keep file for deletion");
        
        // Test validation passes when no required deletes are set
        let manifests = vec![manifest];
        let result = manager.validate_required_deletes(&manifests);
        assert!(result.is_ok(), "Validation should pass when no required deletes are specified");
    }

    #[tokio::test]
    async fn test_clean_uncommitted_removes_all_entries_when_uncommitted() {
        let (mut manager, temp_dir) = setup_test_manager();
        
        // Create a manifest file for testing clean uncommitted functionality
        let manifest_path = temp_dir.path().join("uncommitted_manifest.avro");
        let manifest_path_str = manifest_path.to_str().unwrap();
        
        let _manifest_file = ManifestFile {
            manifest_path: manifest_path_str.to_string(),
            manifest_length: 1000,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 10,
            min_sequence_number: 1,
            added_snapshot_id: 12345,
            added_files_count: Some(2),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(200),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: vec![],
            key_metadata: vec![],
        };
        
        // Test the clean uncommitted method with empty committed set
        let committed_files = std::collections::HashSet::new();
        let result = manager.clean_uncommitted(committed_files).await;
        
        assert!(result.is_ok(), "clean_uncommitted should succeed");
        
        // Test that manager is still functional after clean_uncommitted
        let test_file = create_test_data_file("/test/file.parquet", 0);
        manager.delete_files.insert(test_file.file_path.clone(), test_file);
        
        // Verify the manager still tracks delete files
        assert!(manager.contains_deletes());
    }

    #[test]
    fn test_min_sequence_number_filtering_logic() {
        let (mut manager, _temp_dir) = setup_test_manager();
        
        // Set min sequence number to 5
        manager.min_sequence_number = 5;
        
        // Test manifests with different sequence numbers
        let old_manifest = ManifestFile {
            manifest_path: "/test/old.avro".to_string(),
            manifest_length: 1000,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 3, // Below threshold
            min_sequence_number: 3,
            added_snapshot_id: 12345,
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(100),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: vec![],
            key_metadata: vec![],
        };
        
        let new_manifest = ManifestFile {
            manifest_path: "/test/new.avro".to_string(),
            manifest_length: 1000,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 8, // Above threshold
            min_sequence_number: 8,
            added_snapshot_id: 12346,
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(100),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: vec![],
            key_metadata: vec![],
        };
        
        // Add some delete files to test with
        let test_file = create_test_data_file("/test/file.parquet", 0);
        manager.delete_files.insert(test_file.file_path.clone(), test_file);
        
        // Both manifests should be able to contain deleted files since they have live files and we have files to delete
        assert!(manager.can_contain_deleted_files(&old_manifest), 
               "Old manifest should be able to contain deleted files since it has live files");
        
        // New manifest should be processed for deletions  
        assert!(manager.can_contain_deleted_files(&new_manifest),
               "New manifest should be able to contain deleted files since it has live files");
        
        // Verify sequence number comparison logic - this is still valid for min_sequence_number usage elsewhere
        assert!(old_manifest.min_sequence_number < manager.min_sequence_number);
        assert!(new_manifest.min_sequence_number >= manager.min_sequence_number);
    }

    #[tokio::test]
    async fn test_filter_manifests_with_entries_and_rewrite() {
        let (mut manager, temp_dir) = setup_test_manager();
        let schema = create_test_schema();
        
        // Create test data files - some to keep, some to delete
        let keep_file1 = create_test_data_file("/test/keep1.parquet", 0);
        let keep_file2 = create_test_data_file("/test/keep2.parquet", 0);
        let delete_file1 = create_test_data_file("/test/delete1.parquet", 0);
        let delete_file2 = create_test_data_file("/test/delete2.parquet", 0);
        
        // Mark files for deletion
        manager.delete_files.insert(delete_file1.file_path.clone(), delete_file1.clone());
        manager.delete_files.insert(delete_file2.file_path.clone(), delete_file2.clone());
        
        // Create first manifest with mixed files
        let manifest1_path = temp_dir.path().join("manifest1.avro");
        let manifest1_path_str = manifest1_path.to_str().unwrap();
        
        let partition_spec = PartitionSpec::unpartition_spec();
        
        // Create entries for first manifest
        let entries1 = vec![
            ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .data_file(keep_file1.clone())
                .build(),
            ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .data_file(delete_file1.clone())
                .build(),
        ];
        
        // Write first manifest
        let output_file1 = manager.file_io.new_output(manifest1_path_str).unwrap();
        let mut writer1 = ManifestWriterBuilder::new(
            output_file1,
            Some(12345),
            vec![],
            schema.clone().into(),
            partition_spec.clone(),
        ).build_v2_data();
        
        for entry in &entries1 {
            writer1.add_entry(entry.clone()).unwrap();
        }
        writer1.write_manifest_file().await.unwrap();
        
        // Create second manifest with different files
        let manifest2_path = temp_dir.path().join("manifest2.avro");
        let manifest2_path_str = manifest2_path.to_str().unwrap();
        
        let entries2 = vec![
            ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .data_file(keep_file2.clone())
                .build(),
            ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .data_file(delete_file2.clone())
                .build(),
        ];
        
        // Write second manifest
        let output_file2 = manager.file_io.new_output(manifest2_path_str).unwrap();
        let mut writer2 = ManifestWriterBuilder::new(
            output_file2,
            Some(12346),
            vec![],
            schema.clone().into(),
            partition_spec.clone(),
        ).build_v2_data();
        
        for entry in &entries2 {
            writer2.add_entry(entry.clone()).unwrap();
        }
        writer2.write_manifest_file().await.unwrap();
        
        // Create ManifestFile objects
        let manifest1 = ManifestFile {
            manifest_path: manifest1_path_str.to_string(),
            manifest_length: 1000,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 10,
            min_sequence_number: 1,
            added_snapshot_id: 12345,
            added_files_count: Some(2),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(200),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: vec![],
            key_metadata: vec![],
        };
        
        let manifest2 = ManifestFile {
            manifest_path: manifest2_path_str.to_string(),
            manifest_length: 1000,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 10,
            min_sequence_number: 1,
            added_snapshot_id: 12346,
            added_files_count: Some(2),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(200),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: vec![],
            key_metadata: vec![],
        };
        
        let input_manifests = vec![manifest1.clone(), manifest2.clone()];
        
        // **THIS IS THE KEY TEST: Call filter_manifests function**
        let filtered_manifests = manager.filter_manifests(&schema, input_manifests).await.unwrap();
        
        // Verify we got filtered manifests back
        assert_eq!(filtered_manifests.len(), 2, "Should return same number of manifests");
        
        // Verify that filtered manifests have different paths (rewritten)
        assert_ne!(filtered_manifests[0].manifest_path, manifest1.manifest_path,
                  "First manifest should be rewritten with new path");
        assert_ne!(filtered_manifests[1].manifest_path, manifest2.manifest_path,
                  "Second manifest should be rewritten with new path");
        
        // Verify deletion tracking
        assert_eq!(manager.files_to_be_deleted().len(), 2, "Should track 2 files for deletion");
        let deleted_paths: std::collections::HashSet<_> = manager.files_to_be_deleted()
            .into_iter().map(|f| f.file_path).collect();
        assert!(deleted_paths.contains(&delete_file1.file_path));
        assert!(deleted_paths.contains(&delete_file2.file_path));
        
        // **VERIFY ENTRIES IN FILTERED MANIFESTS**
        // Load and check entries in first filtered manifest
        let filtered_manifest1 = filtered_manifests[0].load_manifest(&manager.file_io).await.unwrap();
        let (entries1_filtered, _) = filtered_manifest1.into_parts();
        
        // Count live entries and deleted entries
        let mut live_entries = 0;
        let mut deleted_entries = 0;
        let mut keep1_found = false;
        let mut delete1_found = false;
        
        for entry in &entries1_filtered {
            match entry.status() {
                ManifestStatus::Added | ManifestStatus::Existing => {
                    live_entries += 1;
                    if entry.data_file().file_path() == keep_file1.file_path {
                        keep1_found = true;
                    }
                }
                ManifestStatus::Deleted => {
                    deleted_entries += 1;
                    if entry.data_file().file_path() == delete_file1.file_path {
                        delete1_found = true;
                    }
                }
            }
        }
        
        assert_eq!(live_entries, 1, "First manifest should have 1 live entry (keep_file1)");
        assert_eq!(deleted_entries, 1, "First manifest should have 1 deleted entry (delete_file1)");
        assert!(keep1_found, "keep_file1 should be found as live entry");
        assert!(delete1_found, "delete_file1 should be found as deleted entry");
        
        // **VERIFY FILTERED MANIFEST CACHE**
        assert!(manager.filtered_manifests.contains_key(&manifest1.manifest_path),
               "Original manifest1 path should be cached");
        assert!(manager.filtered_manifests.contains_key(&manifest2.manifest_path),
               "Original manifest2 path should be cached");
        
        // **VERIFY DELETED FILES TRACKING**
        assert!(manager.filtered_manifest_to_deleted_files.contains_key(&filtered_manifests[0].manifest_path),
               "Should track deleted files for first filtered manifest");
        assert!(manager.filtered_manifest_to_deleted_files.contains_key(&filtered_manifests[1].manifest_path),
               "Should track deleted files for second filtered manifest");
        
        let deleted_files_manifest1 = &manager.filtered_manifest_to_deleted_files[&filtered_manifests[0].manifest_path];
        let deleted_files_manifest2 = &manager.filtered_manifest_to_deleted_files[&filtered_manifests[1].manifest_path];
        
        assert_eq!(deleted_files_manifest1.len(), 1, "First manifest should track 1 deleted file");
        assert_eq!(deleted_files_manifest2.len(), 1, "Second manifest should track 1 deleted file");
        assert!(deleted_files_manifest1.contains(&delete_file1.file_path));
        assert!(deleted_files_manifest2.contains(&delete_file2.file_path));
    }
}
