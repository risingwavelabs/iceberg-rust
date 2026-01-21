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

//! Delete orphan files action.
//!
//! Scans the table location and removes files not referenced by any snapshot.
//! A file is considered orphan if:
//!
//! 1. Not referenced by any snapshot (data files, delete files, manifests, manifest lists)
//! 2. Not part of table metadata (metadata files, statistics files, version-hint.text)
//! 3. Older than the specified threshold (default: 1 day before action creation time)
//!
//! Similar to Spark's `DeleteOrphanFilesSparkAction`.
//!
//! # Safety
//!
//! Files without a `last_modified` timestamp are skipped to avoid deleting
//! in-progress writes.
//!
//! # Path Matching
//!
//! Orphan detection relies on exact path string matching. Paths from
//! `FileIO::list_recursive` must match those stored in metadata.
//! Object stores (S3, GCS, Azure) use absolute URIs (e.g., `s3://bucket/path`).
//! Local file systems should use paths consistently (with or without `file://`).

use std::collections::HashSet;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use futures::TryStreamExt;

use crate::io::FileIO;
use crate::spec::TableMetadataRef;
use crate::table::Table;
use crate::Result;

/// Default older-than duration for orphan file deletion (1 day in milliseconds).
const DEFAULT_OLDER_THAN_MS: i64 = 24 * 60 * 60 * 1000;

/// Default concurrency limit for file deletion.
const DEFAULT_DELETE_CONCURRENCY: usize = 10;

/// Result of the delete orphan files action.
#[derive(Debug, Default)]
pub struct DeleteOrphanFilesResult {
    /// Orphan file paths that were deleted (empty in dry run mode).
    pub deleted_files: Vec<String>,
    /// Orphan file paths found but not deleted (only populated in dry run mode).
    pub orphan_files: Vec<String>,
}

/// Action to delete orphan files from a table's location.
///
/// Orphan files are unreferenced files left behind by failed writes,
/// expired snapshots, or other incomplete operations.
///
/// # Example
///
/// ```ignore
/// let result = DeleteOrphanFilesAction::new(table)
///     .older_than(Duration::from_secs(3600)) // 1 hour
///     .dry_run(true) // preview without deleting
///     .execute()
///     .await?;
///
/// println!("Found {} orphan files", result.orphan_files.len());
/// ```
pub struct DeleteOrphanFilesAction {
    table: Table,
    /// Files older than this timestamp (in milliseconds) can be deleted.
    older_than_ms: i64,
    /// Whether to actually delete files or just identify them.
    dry_run: bool,
    /// Maximum concurrent delete operations.
    delete_concurrency: usize,
    /// Optional custom location to scan (defaults to table location).
    location: Option<String>,
}

impl DeleteOrphanFilesAction {
    /// Creates a new delete orphan files action for the given table.
    pub fn new(table: Table) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as i64;

        Self {
            table,
            older_than_ms: now_ms - DEFAULT_OLDER_THAN_MS,
            dry_run: false,
            delete_concurrency: DEFAULT_DELETE_CONCURRENCY,
            location: None,
        }
    }

    /// Sets the timestamp threshold for orphan files.
    ///
    /// Only files with a last modified time older than this value (in milliseconds
    /// since Unix epoch) will be considered for deletion.
    pub fn older_than_ms(mut self, timestamp_ms: i64) -> Self {
        self.older_than_ms = timestamp_ms;
        self
    }

    /// Sets the older-than duration relative to now.
    ///
    /// Files older than `now - duration` will be considered for deletion.
    pub fn older_than(mut self, duration: Duration) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as i64;
        self.older_than_ms = now_ms - duration.as_millis() as i64;
        self
    }

    /// Enables dry run mode - identifies orphan files without deleting them.
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Sets the concurrency limit for delete operations.
    pub fn delete_concurrency(mut self, concurrency: usize) -> Self {
        self.delete_concurrency = concurrency.max(1);
        self
    }

    /// Sets a custom location to scan for orphan files.
    ///
    /// By default, the table's location is used.
    pub fn location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Executes the delete orphan files action.
    pub async fn execute(self) -> Result<DeleteOrphanFilesResult> {
        let file_io = self.table.file_io();
        let table_metadata = self.table.metadata_ref();
        let location = self
            .location
            .as_deref()
            .unwrap_or_else(|| table_metadata.location());

        // Build the set of reachable files
        let reachable_files = self.collect_reachable_files(file_io, &table_metadata).await?;

        // List all files under the table location
        let all_files = file_io.list_recursive(location).await?;

        // Find orphan files: not reachable, not a directory, and older than threshold
        let orphan_files: Vec<String> = all_files
            .into_iter()
            .filter(|entry| {
                // Must be a file (not directory)
                !entry.metadata.is_dir
                    // Must not be reachable
                    && !reachable_files.contains(&entry.path)
                    // Must have a timestamp and be older than threshold
                    // (files without timestamp are skipped to protect in-progress writes)
                    && entry.metadata.last_modified_ms.is_some_and(|ts| ts < self.older_than_ms)
            })
            .map(|entry| entry.path)
            .collect();

        if self.dry_run {
            return Ok(DeleteOrphanFilesResult {
                deleted_files: Vec::new(),
                orphan_files,
            });
        }

        // Delete orphan files concurrently
        stream::iter(&orphan_files)
            .map(|path| {
                let file_io = file_io.clone();
                async move { file_io.delete(path).await }
            })
            .buffer_unordered(self.delete_concurrency)
            .try_collect::<Vec<_>>()
            .await?;

        Ok(DeleteOrphanFilesResult {
            deleted_files: orphan_files,
            orphan_files: Vec::new(),
        })
    }

    /// Collects all files that are reachable from the table metadata.
    ///
    /// This includes:
    /// - Content files (data files, delete files) from all snapshots
    /// - Manifest files from all snapshots
    /// - Manifest list files from all snapshots
    /// - Metadata files (current and historical)
    /// - version-hint.text file
    async fn collect_reachable_files(
        &self,
        file_io: &FileIO,
        table_metadata: &TableMetadataRef,
    ) -> Result<HashSet<String>> {
        let mut reachable = HashSet::new();

        // 1. Collect metadata files
        self.collect_metadata_files(table_metadata, &mut reachable);

        // 2. Collect files from all snapshots
        self.collect_snapshot_files(file_io, table_metadata, &mut reachable)
            .await?;

        Ok(reachable)
    }

    /// Collects metadata-related files that should be preserved.
    fn collect_metadata_files(
        &self,
        table_metadata: &TableMetadataRef,
        reachable: &mut HashSet<String>,
    ) {
        let location = table_metadata.location();

        // version-hint.text
        reachable.insert(format!("{}/metadata/version-hint.text", location));

        // Current metadata file
        if let Some(metadata_location) = self.table.metadata_location() {
            reachable.insert(metadata_location.to_string());
        }

        // Historical metadata files
        reachable.extend(
            table_metadata
                .metadata_log()
                .iter()
                .map(|e| e.metadata_file.clone()),
        );

        // Statistics files
        reachable.extend(
            table_metadata
                .statistics_iter()
                .map(|s| s.statistics_path.clone()),
        );

        // Partition statistics files
        reachable.extend(
            table_metadata
                .partition_statistics_iter()
                .map(|s| s.statistics_path.clone()),
        );
    }

    /// Collects files referenced by snapshots.
    async fn collect_snapshot_files(
        &self,
        file_io: &FileIO,
        table_metadata: &TableMetadataRef,
        reachable: &mut HashSet<String>,
    ) -> Result<()> {
        // Collect all manifest list paths first
        let snapshots: Vec<_> = table_metadata.snapshots().collect();
        
        for snapshot in &snapshots {
            let manifest_list_path = snapshot.manifest_list();
            if !manifest_list_path.is_empty() {
                reachable.insert(manifest_list_path.to_string());
            }
        }

        // Load manifest lists concurrently
        const MANIFEST_LIST_LOAD_CONCURRENCY: usize = 8;
        let manifest_lists: Vec<_> = stream::iter(snapshots)
            .map(|snapshot| {
                let file_io = file_io.clone();
                let table_metadata = table_metadata.clone();
                async move {
                    snapshot.load_manifest_list(&file_io, &table_metadata).await
                }
            })
            .buffer_unordered(MANIFEST_LIST_LOAD_CONCURRENCY)
            .try_collect()
            .await?;

        // Collect all manifest files from manifest lists
        let mut all_manifest_files = Vec::new();
        for manifest_list in manifest_lists {
            for manifest_file in manifest_list.entries() {
                reachable.insert(manifest_file.manifest_path.clone());
                all_manifest_files.push(manifest_file.clone());
            }
        }

        // Deduplicate manifest files (same manifest can be referenced by multiple snapshots)
        all_manifest_files.sort_by(|a, b| a.manifest_path.cmp(&b.manifest_path));
        all_manifest_files.dedup_by(|a, b| a.manifest_path == b.manifest_path);

        // Load manifests concurrently and collect content files
        const MANIFEST_LOAD_CONCURRENCY: usize = 16;
        let content_files: Vec<Vec<String>> = stream::iter(all_manifest_files)
            .map(|manifest_file| {
                let file_io = file_io.clone();
                async move {
                    let manifest = manifest_file.load_manifest(&file_io).await?;
                    let paths: Vec<String> = manifest
                        .entries()
                        .iter()
                        .map(|entry| entry.data_file().file_path().to_string())
                        .collect();
                    Ok::<_, crate::Error>(paths)
                }
            })
            .buffer_unordered(MANIFEST_LOAD_CONCURRENCY)
            .try_collect()
            .await?;

        for paths in content_files {
            reachable.extend(paths);
        }

        Ok(())
    }
}