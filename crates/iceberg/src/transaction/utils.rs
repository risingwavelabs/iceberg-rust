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

//! Utilities for transaction operations, including file cleanup strategies.

use std::collections::HashSet;

use futures::stream::{self, StreamExt, TryStreamExt};

use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{ManifestFile, Snapshot, TableMetadataRef};

/// Default concurrency limit for deleting files.
const DEFAULT_DELETE_CONCURRENCY_LIMIT: usize = 50;

/// Strategy for cleaning up unreachable files after snapshot expiration.
///
/// This strategy compares metadata before and after snapshot expiration
/// to identify and delete files that are no longer reachable.
pub struct ReachableFileCleanupStrategy {
    file_io: FileIO,
}

impl ReachableFileCleanupStrategy {
    /// Creates a new cleanup strategy with the given FileIO.
    pub fn new(file_io: FileIO) -> Self {
        Self { file_io }
    }

    /// Cleans up files that became unreachable after snapshot expiration.
    ///
    /// This method:
    /// 1. Identifies expired snapshots by comparing before and after metadata
    /// 2. Finds manifest lists that are no longer referenced
    /// 3. Finds manifests that are no longer referenced
    /// 4. Finds data files within expired manifests that are not referenced elsewhere
    /// 5. Deletes all unreachable files
    ///
    /// # Arguments
    ///
    /// * `before_expiration` - Table metadata before snapshot expiration
    /// * `after_expiration` - Table metadata after snapshot expiration
    ///
    /// # Returns
    ///
    /// `Ok(())` if cleanup succeeds, `Err` if any deletion fails
    pub async fn clean_files(
        &self,
        before_expiration: &TableMetadataRef,
        after_expiration: &TableMetadataRef,
    ) -> Result<()> {
        // Identify expired snapshots
        let before_snapshot_ids: HashSet<i64> = before_expiration
            .snapshots()
            .map(|s| s.snapshot_id())
            .collect();
        let after_snapshot_ids: HashSet<i64> =
            after_expiration.snapshots().map(|s| s.snapshot_id()).collect();

        let expired_snapshot_ids: HashSet<i64> = before_snapshot_ids
            .difference(&after_snapshot_ids)
            .copied()
            .collect();

        if expired_snapshot_ids.is_empty() {
            tracing::debug!("No snapshots expired, skipping file cleanup");
            return Ok(());
        }

        tracing::info!(
            "Cleaning up files from {} expired snapshots",
            expired_snapshot_ids.len()
        );

        // Find expired snapshots
        let expired_snapshots: Vec<std::sync::Arc<Snapshot>> = before_expiration
            .snapshots()
            .filter(|s| expired_snapshot_ids.contains(&s.snapshot_id()))
            .cloned()
            .collect();

        // Collect manifest list paths to delete
        let manifest_lists_to_delete: Vec<String> = expired_snapshots
            .iter()
            .map(|s| s.manifest_list().to_string())
            .collect();

        // Collect manifest files that might need deletion
        let deletion_candidates = {
            let mut deletion_candidates = HashSet::new();
            // Load manifest lists from expired snapshots
            // TODO: Consider parallelizing this if it becomes a bottleneck
            for snapshot in expired_snapshots {
                let manifest_list = snapshot
                    .load_manifest_list(&self.file_io, before_expiration)
                    .await?;
                for manifest_file in manifest_list.entries() {
                    deletion_candidates.insert(manifest_file.clone());
                }
            }
            deletion_candidates
        };

        if !deletion_candidates.is_empty() {
            let (manifests_to_delete, referenced_manifests) = self
                .prune_referenced_manifests(
                    after_expiration.snapshots(),
                    after_expiration,
                    deletion_candidates,
                )
                .await?;

            if !manifests_to_delete.is_empty() {
                let files_to_delete = self
                    .find_files_to_delete(&manifests_to_delete, &referenced_manifests)
                    .await?;

                // Delete data files
                if !files_to_delete.is_empty() {
                    tracing::info!("Deleting {} data files", files_to_delete.len());
                    stream::iter(files_to_delete)
                        .map(|file_path| async move {
                            tracing::debug!("Deleting data file: {}", file_path);
                            self.file_io.delete(&file_path).await.map_err(|e| {
                                tracing::error!("Failed to delete data file {}: {}", file_path, e);
                                e
                            })
                        })
                        .buffer_unordered(DEFAULT_DELETE_CONCURRENCY_LIMIT)
                        .try_collect::<Vec<_>>()
                        .await?;
                }

                // Delete manifest files
                tracing::info!("Deleting {} manifest files", manifests_to_delete.len());
                stream::iter(manifests_to_delete)
                    .map(|manifest_file| async move {
                        tracing::debug!("Deleting manifest: {}", manifest_file.manifest_path);
                        self.file_io
                            .delete(&manifest_file.manifest_path)
                            .await
                            .map_err(|e| {
                                tracing::error!(
                                    "Failed to delete manifest {}: {}",
                                    manifest_file.manifest_path,
                                    e
                                );
                                e
                            })
                    })
                    .buffer_unordered(DEFAULT_DELETE_CONCURRENCY_LIMIT)
                    .try_collect::<Vec<_>>()
                    .await?;
            }
        }

        // Delete manifest lists
        if !manifest_lists_to_delete.is_empty() {
            tracing::info!(
                "Deleting {} manifest lists",
                manifest_lists_to_delete.len()
            );
            stream::iter(manifest_lists_to_delete)
                .map(|path| async move {
                    tracing::debug!("Deleting manifest list: {}", path);
                    self.file_io.delete(&path).await.map_err(|e| {
                        tracing::error!("Failed to delete manifest list {}: {}", path, e);
                        e
                    })
                })
                .buffer_unordered(DEFAULT_DELETE_CONCURRENCY_LIMIT)
                .try_collect::<Vec<_>>()
                .await?;
        }

        tracing::info!("File cleanup completed successfully");
        Ok(())
    }

    /// Separates manifests into those that can be deleted and those still referenced.
    ///
    /// # Arguments
    ///
    /// * `snapshots` - Iterator over snapshots in the current table metadata
    /// * `table_metadata_ref` - Reference to the current table metadata
    /// * `deletion_candidates` - Set of manifest files that might be deleted
    ///
    /// # Returns
    ///
    /// A tuple of (manifests_to_delete, referenced_manifests)
    async fn prune_referenced_manifests(
        &self,
        snapshots: impl Iterator<Item = &std::sync::Arc<Snapshot>>,
        table_metadata_ref: &TableMetadataRef,
        mut deletion_candidates: HashSet<ManifestFile>,
    ) -> Result<(HashSet<ManifestFile>, HashSet<ManifestFile>)> {
        let mut referenced_manifests = HashSet::default();
        for snapshot in snapshots {
            let manifest_list = snapshot
                .load_manifest_list(&self.file_io, table_metadata_ref)
                .await?;
            for manifest_file in manifest_list.entries() {
                if deletion_candidates.remove(manifest_file) {
                    referenced_manifests.insert(manifest_file.clone());
                }
            }
        }
        Ok((deletion_candidates, referenced_manifests))
    }

    /// Finds data files that can be safely deleted.
    ///
    /// This method loads manifests from the deletion candidates and collects
    /// all data file paths. Then it removes any paths that are still referenced
    /// in the manifests that are being kept.
    ///
    /// # Arguments
    ///
    /// * `manifests_to_delete` - Manifests that will be deleted
    /// * `referenced_manifests` - Manifests that are still referenced
    ///
    /// # Returns
    ///
    /// Set of data file paths that can be safely deleted
    async fn find_files_to_delete(
        &self,
        manifests_to_delete: &HashSet<ManifestFile>,
        referenced_manifests: &HashSet<ManifestFile>,
    ) -> Result<HashSet<String>> {
        let mut files_to_delete = HashSet::default();

        // Collect all data files from manifests being deleted
        for manifest_file in manifests_to_delete {
            let manifest = manifest_file.load_manifest(&self.file_io).await?;
            for entry in manifest.entries() {
                files_to_delete.insert(entry.data_file().file_path().to_owned());
            }
        }

        if files_to_delete.is_empty() {
            return Ok(files_to_delete);
        }

        // Remove files that are still referenced
        for manifest_file in referenced_manifests {
            let manifest = manifest_file.load_manifest(&self.file_io).await?;
            for entry in manifest.entries() {
                files_to_delete.remove(entry.data_file().file_path());
            }
        }

        Ok(files_to_delete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cleanup_strategy_creation() {
        use crate::io::FileIOBuilder;

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let strategy = ReachableFileCleanupStrategy::new(file_io);
        assert!(std::mem::size_of_val(&strategy) > 0);
    }
}
