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
    DataFile, FormatVersion, ManifestContentType, ManifestFile, ManifestStatus, ManifestWriterBuilder, Schema, Struct
};
use crate::transaction::snapshot::new_manifest_path;
use crate::{Error, ErrorKind};

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
    /// Whether the filtering is case sensitive
    case_sensitive: bool,
    /// Cache of filtered manifests to avoid reprocessing
    filtered_manifests: HashMap<String, ManifestFile>, // manifest_path -> filtered_manifest
    /// Tracking where files were deleted to validate retries quickly
    filtered_manifest_to_deleted_files: HashMap<String, Vec<String>>, // manifest_path -> deleted_files

    file_io: FileIO,

    // for manifest writer
    metadata_location: String,
    meta_root_path: String,
    commit_uuid: Uuid,
    manifest_counter: RangeFrom<u64>,
    format_version: FormatVersion,
}

impl ManifestFilterManager {
    /// Create a new ManifestFilterManager
    pub fn new() -> Self {
        todo!()
        // Self {
        //     delete_paths: HashSet::new(),
        //     delete_files: HashMap::new(),
        //     drop_partitions: HashMap::new(),
        //     delete_file_partitions: HashMap::new(),
        //     min_sequence_number: 0,
        //     fail_any_delete: false,
        //     fail_missing_delete_paths: false,
        //     case_sensitive: true,
        //     filtered_manifests: HashMap::new(),
        //     filtered_manifest_to_deleted_files: HashMap::new(),
        // }
    }

    /// Set whether to fail if any delete operation is attempted
    pub fn fail_any_delete(mut self) -> Self {
        self.fail_any_delete = true;
        self
    }

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

    /// Set whether the filtering is case sensitive
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Set whether to fail if required delete paths are missing
    pub fn fail_missing_delete_paths(mut self) -> Self {
        self.fail_missing_delete_paths = true;
        self
    }

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
    pub async fn delete_file_by_path(mut self, path: impl Into<String>) -> Result<()> {
        self.invalidate_filtered_cache().await?;
        self.delete_paths.insert(path.into());

        Ok(())
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

        // // Quick check: if we know this manifest has deletes, return true immediately
        // if self.manifests_with_deletes.contains(&manifest.manifest_path) {
        //     return true;
        // }

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

        return false;
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
        
        // Create a new output path for the filtered manifest
        let filtered_manifest_path = new_manifest_path(
            &self.metadata_location,
            &self.meta_root_path,
            self.commit_uuid,
            self.manifest_counter.next().unwrap(),
            crate::spec::DataFileFormat::Avro,
        );
        let output = self.file_io.new_output(&filtered_manifest_path)?;
        
        // Create a minimal partition spec for now. In a real implementation, 
        // this would come from the table metadata using the partition_spec_id
        let partition_spec = manifest_meta_data.partition_spec.clone();

        // Create the manifest writer
        let writer_builder = ManifestWriterBuilder::new(
            output,
            Some(manifest.added_snapshot_id),  // Use the original snapshot ID
            Vec::new(), // key_metadata - empty for now
            table_schema.clone().into(), // Convert to Arc<Schema>
            partition_spec,
        );
        
        // let mut writer = if manifest.content == ManifestContentType::Data {
        //     writer_builder.build_v2_data()
        // } else {
        //     writer_builder.build_v2_deletes()
        // };

        let mut writer = match self.format_version {
            FormatVersion::V2 => {
                match manifest.content {
                    ManifestContentType::Data => writer_builder.build_v2_data(),
                    ManifestContentType::Deletes => writer_builder.build_v2_deletes(),
                }
            }
            FormatVersion::V1 => writer_builder.build_v1(),
        };
        
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
                    .map(|drop_partition| drop_partition.contains(&file.partition()))
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
                    .map(|drop_partition| drop_partition.contains(&file.partition()))
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
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_filter_manager_creation() {
        let manager = ManifestFilterManager::new();
        assert!(!manager.contains_deletes());
        assert!(manager.case_sensitive);
    }

    #[test]
    fn test_delete_operations() {
        let manager = ManifestFilterManager::new()
            .delete_file_by_path("test/path/file.parquet")
            .drop_delete_files_older_than(100);

        assert!(manager.contains_deletes());
        assert_eq!(manager.min_sequence_number, 100);
        assert!(manager.delete_paths.contains("test/path/file.parquet"));
    }

    #[test]
    fn test_configuration() {
        let manager = ManifestFilterManager::new()
            .case_sensitive(false)
            .fail_any_delete()
            .fail_missing_delete_paths();

        assert!(!manager.case_sensitive);
        assert!(manager.fail_any_delete);
        assert!(manager.fail_missing_delete_paths);
    }
}
