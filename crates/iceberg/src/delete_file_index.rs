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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, RwLock};

use futures::StreamExt;
use futures::channel::mpsc::{Sender, channel};
use tokio::sync::Notify;

use crate::runtime::Runtime;
use crate::scan::{DeleteFileContext, FileScanTaskDeleteFile};
use crate::spec::{DataContentType, DataFile, Struct};

// Iceberg field ID for the `file_path` column in position delete files.
//
// See Iceberg spec (position deletes) and our `POSITION_DELETE_SCHEMA` in
// `writer/base_writer/position_delete_file_writer.rs`.
pub(crate) const POSITION_DELETE_FILE_PATH_FIELD_ID: i32 = 2147483546;

/// Infers the single data file a position delete file targets, from its path bounds.
///
/// Returns `None` when the delete file cannot be attributed to exactly one data file.
pub(crate) fn try_infer_single_referenced_data_file_from_bounds(
    delete_file: &DataFile,
) -> Option<String> {
    // Match Iceberg Java's `DeleteFileUtil.referencedDataFile(DeleteFile)` heuristic:
    // if lower and upper bounds for PATH_ID are present and equal, the delete file
    // targets a single data file.
    let lower = delete_file
        .lower_bounds
        .get(&POSITION_DELETE_FILE_PATH_FIELD_ID)?;
    let upper = delete_file
        .upper_bounds
        .get(&POSITION_DELETE_FILE_PATH_FIELD_ID)?;

    if lower != upper {
        return None;
    }

    let bytes = lower.to_bytes().ok()?;
    let path = std::str::from_utf8(bytes.as_ref()).ok()?;
    Some(path.to_string())
}

/// Returns whether a position delete file may contain deletes for the data file.
///
/// An explicit referenced data file is exact and takes precedence over metrics. Otherwise, file
/// path bounds are used only to prove that the data file is outside the delete file's path range.
/// Missing or incompatible metrics fail open so that scan planning never drops an applicable
/// delete.
fn can_contain_pos_deletes_for_file(data_file: &DataFile, delete_file: &DataFile) -> bool {
    if let Some(referenced_data_file) = delete_file.referenced_data_file() {
        return referenced_data_file == data_file.file_path();
    }

    let (Some(lower), Some(upper)) = (
        delete_file
            .lower_bounds()
            .get(&POSITION_DELETE_FILE_PATH_FIELD_ID),
        delete_file
            .upper_bounds()
            .get(&POSITION_DELETE_FILE_PATH_FIELD_ID),
    ) else {
        return true;
    };
    let data_file_path = crate::spec::Datum::string(data_file.file_path());

    let (Some(lower_to_upper), Some(path_to_lower), Some(path_to_upper)) = (
        lower.partial_cmp(upper),
        data_file_path.partial_cmp(lower),
        data_file_path.partial_cmp(upper),
    ) else {
        return true;
    };
    if lower_to_upper == Ordering::Greater {
        return true;
    }

    path_to_lower != Ordering::Less && path_to_upper != Ordering::Greater
}

fn may_contain_null(file: &DataFile, field_id: i32) -> bool {
    file.null_value_counts()
        .get(&field_id)
        .is_none_or(|count| *count > 0)
}

/// Returns whether an equality delete file may contain a row that matches the data file.
///
/// File metrics are used only to prove that a match is impossible. Missing or incompatible
/// metrics fail open so that scan planning never drops a delete that may apply.
fn can_contain_eq_deletes_for_file(data_file: &DataFile, delete_file: &DataFile) -> bool {
    let Some(equality_ids) = delete_file.equality_ids.as_deref() else {
        return true;
    };

    for &field_id in equality_ids {
        if may_contain_null(data_file, field_id) && may_contain_null(delete_file, field_id) {
            // Null equals null for equality deletes, so this field may match.
            continue;
        }

        let (Some(data_lower), Some(data_upper), Some(delete_lower), Some(delete_upper)) = (
            data_file.lower_bounds().get(&field_id),
            data_file.upper_bounds().get(&field_id),
            delete_file.lower_bounds().get(&field_id),
            delete_file.upper_bounds().get(&field_id),
        ) else {
            continue;
        };

        if matches!(
            data_lower.partial_cmp(delete_upper),
            Some(Ordering::Greater)
        ) || matches!(
            delete_lower.partial_cmp(data_upper),
            Some(Ordering::Greater)
        ) {
            return false;
        }
    }

    true
}

/// Index of delete files
#[derive(Debug, Clone)]
pub(crate) struct DeleteFileIndex {
    state: Arc<RwLock<DeleteFileIndexState>>,
}

#[derive(Debug)]
enum DeleteFileIndexState {
    Populating(Arc<Notify>),
    Populated(PopulatedDeleteFileIndex),
}

#[derive(Debug)]
struct PopulatedDeleteFileIndex {
    global_equality_deletes: Vec<Arc<DeleteFileContext>>,
    eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    // TODO: do we need this?
    // pos_deletes_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>>,

    // TODO: Deletion Vector support
}

impl DeleteFileIndex {
    /// create a new `DeleteFileIndex` along with the sender that populates it with delete files
    pub(crate) fn new(runtime: Runtime) -> (DeleteFileIndex, Sender<DeleteFileContext>) {
        // TODO: what should the channel limit be?
        let (tx, rx) = channel(10);
        let notify = Arc::new(Notify::new());
        let state = Arc::new(RwLock::new(DeleteFileIndexState::Populating(
            notify.clone(),
        )));
        let delete_file_stream = rx.boxed();

        runtime.io().spawn({
            let state = state.clone();
            async move {
                let delete_files: Vec<DeleteFileContext> =
                    delete_file_stream.collect::<Vec<_>>().await;

                let populated_delete_file_index = PopulatedDeleteFileIndex::new(delete_files);

                {
                    let mut guard = state.write().unwrap();
                    *guard = DeleteFileIndexState::Populated(populated_delete_file_index);
                }
                notify.notify_waiters();
            }
        });

        (DeleteFileIndex { state }, tx)
    }

    /// Gets all the delete files that apply to the specified data file.
    pub(crate) async fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<FileScanTaskDeleteFile> {
        // Create the `Notified` while holding the read lock. The read lock ensures that
        // when we go inside it, either the state is already at Populated or it is still
        // at Populating AND `notify_waiters()` has not been called yet. Any `Notified`
        // created before the invocation of `notify_waiters()` will be notified by it
        // even if `await` has not been called on it yet.
        let notified = {
            let guard = self.state.read().unwrap();
            match &*guard {
                DeleteFileIndexState::Populating(notifier) => notifier.clone().notified_owned(),
                DeleteFileIndexState::Populated(index) => {
                    return index.get_deletes_for_data_file(data_file, seq_num);
                }
            }
        };

        notified.await;

        let guard = self.state.read().unwrap();
        match guard.deref() {
            DeleteFileIndexState::Populated(index) => {
                index.get_deletes_for_data_file(data_file, seq_num)
            }
            _ => unreachable!("Cannot be any other state than loaded"),
        }
    }
}

impl PopulatedDeleteFileIndex {
    /// Creates a new populated delete file index from a list of delete file contexts, which
    /// allows for fast lookup when determining which delete files apply to a given data file.
    ///
    /// 1. The partition information is extracted from each delete file's manifest entry.
    /// 2. If the partition is empty and the delete file is not a positional delete,
    ///    it is added to the `global_equality_deletes` vector
    /// 3. Otherwise, the delete file is added to one of two hash maps based on its content type.
    fn new(files: Vec<DeleteFileContext>) -> PopulatedDeleteFileIndex {
        let mut eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();
        let mut pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>> =
            HashMap::default();

        let mut global_equality_deletes: Vec<Arc<DeleteFileContext>> = vec![];

        files.into_iter().for_each(|ctx| {
            let arc_ctx = Arc::new(ctx);

            let partition = arc_ctx.manifest_entry.data_file().partition();

            // The spec states that "Equality delete files stored with an unpartitioned spec are applied as global deletes".
            if partition.fields().is_empty() {
                // TODO: confirm we're good to skip here if we encounter a pos del
                if arc_ctx.manifest_entry.content_type() != DataContentType::PositionDeletes {
                    global_equality_deletes.push(arc_ctx);
                    return;
                }
            }

            let destination_map = match arc_ctx.manifest_entry.content_type() {
                DataContentType::PositionDeletes => &mut pos_deletes_by_partition,
                DataContentType::EqualityDeletes => &mut eq_deletes_by_partition,
                _ => unreachable!(),
            };

            destination_map
                .entry(partition.clone())
                .and_modify(|entry| {
                    entry.push(arc_ctx.clone());
                })
                .or_insert(vec![arc_ctx.clone()]);
        });

        PopulatedDeleteFileIndex {
            global_equality_deletes,
            eq_deletes_by_partition,
            pos_deletes_by_partition,
        }
    }

    /// Determine all the delete files that apply to the provided `DataFile`.
    fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<FileScanTaskDeleteFile> {
        let mut results = vec![];

        self.global_equality_deletes
            .iter()
            // filter that returns true if the provided delete file's sequence number is **greater than** `seq_num`
            .filter(|&delete| {
                seq_num
                    .map(|seq_num| delete.manifest_entry.sequence_number() > Some(seq_num))
                    .unwrap_or_else(|| true)
                    && can_contain_eq_deletes_for_file(data_file, delete.manifest_entry.data_file())
            })
            .for_each(|delete| results.push(delete.as_ref().into()));

        if let Some(deletes) = self.eq_deletes_by_partition.get(data_file.partition()) {
            deletes
                .iter()
                // filter that returns true if the provided delete file's sequence number is **greater than** `seq_num`
                .filter(|&delete| {
                    seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() > Some(seq_num))
                        .unwrap_or_else(|| true)
                        && data_file.partition_spec_id == delete.partition_spec_id
                        && can_contain_eq_deletes_for_file(
                            data_file,
                            delete.manifest_entry.data_file(),
                        )
                })
                .for_each(|delete| results.push(delete.as_ref().into()));
        }

        if let Some(deletes) = self.pos_deletes_by_partition.get(data_file.partition()) {
            deletes
                .iter()
                // filter that returns true if the provided delete file's sequence number is **greater than or equal to** `seq_num`
                .filter(|&delete| {
                    let delete_file = delete.manifest_entry.data_file();

                    seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() >= Some(seq_num))
                        .unwrap_or_else(|| true)
                        && data_file.partition_spec_id == delete.partition_spec_id
                        && can_contain_pos_deletes_for_file(data_file, delete_file)
                })
                .for_each(|delete| results.push(delete.as_ref().into()));
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, Literal, ManifestEntry,
        ManifestStatus, Struct,
    };

    #[test]
    fn test_delete_file_index_unpartitioned() {
        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(4, &build_unpartitioned_eq_delete()),
            build_added_manifest_entry(6, &build_unpartitioned_eq_delete()),
            build_added_manifest_entry(5, &build_unpartitioned_pos_delete()),
            build_added_manifest_entry(6, &build_unpartitioned_pos_delete()),
        ];

        let delete_file_paths: Vec<String> = deletes
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: 0,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        let data_file = build_unpartitioned_data_file();

        // All deletes apply to sequence 0
        let delete_files_to_apply_for_seq_0 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(0));
        assert_eq!(delete_files_to_apply_for_seq_0.len(), 4);

        // All deletes apply to sequence 3
        let delete_files_to_apply_for_seq_3 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(3));
        assert_eq!(delete_files_to_apply_for_seq_3.len(), 4);

        // Last 3 deletes apply to sequence 4
        let delete_files_to_apply_for_seq_4 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(4));
        let actual_paths_to_apply_for_seq_4: Vec<String> = delete_files_to_apply_for_seq_4
            .into_iter()
            .map(|file| file.file_path)
            .collect();

        assert_eq!(
            actual_paths_to_apply_for_seq_4,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Last 3 deletes apply to sequence 5
        let delete_files_to_apply_for_seq_5 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(5));
        let actual_paths_to_apply_for_seq_5: Vec<String> = delete_files_to_apply_for_seq_5
            .into_iter()
            .map(|file| file.file_path)
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_5,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Only the last position delete applies to sequence 6
        let delete_files_to_apply_for_seq_6 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(6));
        let actual_paths_to_apply_for_seq_6: Vec<String> = delete_files_to_apply_for_seq_6
            .into_iter()
            .map(|file| file.file_path)
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_6,
            delete_file_paths[delete_file_paths.len() - 1..]
        );

        // The 2 global equality deletes should match against any partitioned file
        let partitioned_file =
            build_partitioned_data_file(&Struct::from_iter([Some(Literal::long(100))]), 1);

        let delete_files_to_apply_for_partitioned_file =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(0));
        let actual_paths_to_apply_for_partitioned_file: Vec<String> =
            delete_files_to_apply_for_partitioned_file
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert_eq!(
            actual_paths_to_apply_for_partitioned_file,
            delete_file_paths[..2]
        );
    }

    #[test]
    fn test_delete_file_index_partitioned() {
        let partition_one = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(4, &build_partitioned_eq_delete(&partition_one, spec_id)),
            build_added_manifest_entry(6, &build_partitioned_eq_delete(&partition_one, spec_id)),
            build_added_manifest_entry(5, &build_partitioned_pos_delete(&partition_one, spec_id)),
            build_added_manifest_entry(6, &build_partitioned_pos_delete(&partition_one, spec_id)),
        ];

        let delete_file_paths: Vec<String> = deletes
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        let partitioned_file =
            build_partitioned_data_file(&Struct::from_iter([Some(Literal::long(100))]), spec_id);

        // All deletes apply to sequence 0
        let delete_files_to_apply_for_seq_0 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(0));
        assert_eq!(delete_files_to_apply_for_seq_0.len(), 4);

        // All deletes apply to sequence 3
        let delete_files_to_apply_for_seq_3 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(3));
        assert_eq!(delete_files_to_apply_for_seq_3.len(), 4);

        // Last 3 deletes apply to sequence 4
        let delete_files_to_apply_for_seq_4 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(4));
        let actual_paths_to_apply_for_seq_4: Vec<String> = delete_files_to_apply_for_seq_4
            .into_iter()
            .map(|file| file.file_path)
            .collect();

        assert_eq!(
            actual_paths_to_apply_for_seq_4,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Last 3 deletes apply to sequence 5
        let delete_files_to_apply_for_seq_5 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(5));
        let actual_paths_to_apply_for_seq_5: Vec<String> = delete_files_to_apply_for_seq_5
            .into_iter()
            .map(|file| file.file_path)
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_5,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Only the last position delete applies to sequence 6
        let delete_files_to_apply_for_seq_6 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(6));
        let actual_paths_to_apply_for_seq_6: Vec<String> = delete_files_to_apply_for_seq_6
            .into_iter()
            .map(|file| file.file_path)
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_6,
            delete_file_paths[delete_file_paths.len() - 1..]
        );

        // Data file with different partition tuples does not match any delete files
        let partitioned_second_file =
            build_partitioned_data_file(&Struct::from_iter([Some(Literal::long(200))]), 1);
        let delete_files_to_apply_for_different_partition =
            delete_file_index.get_deletes_for_data_file(&partitioned_second_file, Some(0));
        let actual_paths_to_apply_for_different_partition: Vec<String> =
            delete_files_to_apply_for_different_partition
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert!(actual_paths_to_apply_for_different_partition.is_empty());

        // Data file with same tuple but different spec ID does not match any delete files
        let partitioned_different_spec = build_partitioned_data_file(&partition_one, 2);
        let delete_files_to_apply_for_different_spec =
            delete_file_index.get_deletes_for_data_file(&partitioned_different_spec, Some(0));
        let actual_paths_to_apply_for_different_spec: Vec<String> =
            delete_files_to_apply_for_different_spec
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert!(actual_paths_to_apply_for_different_spec.is_empty());
    }

    #[test]
    fn test_equality_delete_metrics_pruning() {
        let data_file =
            with_int_metrics(build_unpartitioned_data_file(), 1, Some(100), Some(200), 0);

        let disjoint_delete =
            with_int_metrics(build_unpartitioned_eq_delete(), 1, Some(1), Some(10), 0);
        assert!(!can_contain_eq_deletes_for_file(
            &data_file,
            &disjoint_delete
        ));

        for (lower, upper) in [(150, 250), (200, 250)] {
            let delete_file = with_int_metrics(
                build_unpartitioned_eq_delete(),
                1,
                Some(lower),
                Some(upper),
                0,
            );
            assert!(can_contain_eq_deletes_for_file(&data_file, &delete_file));
        }

        let missing_bound_delete =
            with_int_metrics(build_unpartitioned_eq_delete(), 1, Some(1), None, 0);
        assert!(can_contain_eq_deletes_for_file(
            &data_file,
            &missing_bound_delete
        ));

        let data_with_null =
            with_int_metrics(build_unpartitioned_data_file(), 1, Some(100), Some(200), 1);
        let delete_with_null =
            with_int_metrics(build_unpartitioned_eq_delete(), 1, Some(1), Some(10), 1);
        assert!(can_contain_eq_deletes_for_file(
            &data_with_null,
            &delete_with_null
        ));

        let data_file = with_int_metrics(data_file, 2, Some(10), Some(20), 0);
        let mut delete_file =
            with_int_metrics(build_unpartitioned_eq_delete(), 1, Some(150), Some(250), 0);
        delete_file.equality_ids = Some(vec![1, 2]);
        let delete_file = with_int_metrics(delete_file, 2, Some(30), Some(40), 0);
        assert!(!can_contain_eq_deletes_for_file(&data_file, &delete_file));
    }

    fn build_unpartitioned_eq_delete() -> DataFile {
        build_partitioned_eq_delete(&Struct::empty(), 0)
    }

    fn build_partitioned_eq_delete(partition: &Struct, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}_equality_delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::EqualityDeletes)
            .equality_ids(Some(vec![1]))
            .record_count(1)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_unpartitioned_pos_delete() -> DataFile {
        build_partitioned_pos_delete(&Struct::empty(), 0)
    }

    fn build_partitioned_pos_delete(partition: &Struct, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_unpartitioned_data_file() -> DataFile {
        build_unpartitioned_data_file_with_path(format!("{}-data.parquet", Uuid::new_v4()))
    }

    fn build_unpartitioned_data_file_with_path(file_path: impl Into<String>) -> DataFile {
        DataFileBuilder::default()
            .file_path(file_path.into())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_partitioned_data_file(partition_value: &Struct, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-data.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition_value.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn with_int_metrics(
        mut file: DataFile,
        field_id: i32,
        lower: Option<i32>,
        upper: Option<i32>,
        null_count: u64,
    ) -> DataFile {
        file.null_value_counts.insert(field_id, null_count);
        if let Some(lower) = lower {
            file.lower_bounds.insert(field_id, Datum::int(lower));
        }
        if let Some(upper) = upper {
            file.upper_bounds.insert(field_id, Datum::int(upper));
        }
        file
    }

    fn build_added_manifest_entry(data_seq_number: i64, file: &DataFile) -> ManifestEntry {
        ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .sequence_number(data_seq_number)
            .data_file(file.clone())
            .build()
    }

    #[test]
    fn test_position_delete_referenced_data_file_pruning() {
        let data_file = build_unpartitioned_data_file();
        let other_data_file = build_unpartitioned_data_file();
        let targeted_delete = DataFileBuilder::default()
            .file_path("targeted-delete.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .referenced_data_file(Some(data_file.file_path().to_string()))
            .content_offset(Some(17))
            .content_size_in_bytes(Some(23))
            .partition(Struct::empty())
            .partition_spec_id(0)
            .file_size_in_bytes(100)
            .build()
            .unwrap();
        let index = PopulatedDeleteFileIndex::new(vec![DeleteFileContext {
            manifest_entry: build_added_manifest_entry(1, &targeted_delete).into(),
            partition_spec_id: 0,
        }]);

        let deletes = index.get_deletes_for_data_file(&data_file, Some(0));
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].file_format, DataFileFormat::Puffin);
        assert_eq!(
            deletes[0].referenced_data_file.as_deref(),
            Some(data_file.file_path())
        );
        assert_eq!(deletes[0].content_offset, Some(17));
        assert_eq!(deletes[0].content_size_in_bytes, Some(23));
        assert!(
            index
                .get_deletes_for_data_file(&other_data_file, Some(0))
                .is_empty()
        );
    }

    #[test]
    fn test_position_delete_path_bounds_pruning() {
        let mut bounded_delete = build_unpartitioned_pos_delete();
        bounded_delete.lower_bounds.insert(
            POSITION_DELETE_FILE_PATH_FIELD_ID,
            Datum::string("s3://bucket/data/file-00002.parquet"),
        );
        bounded_delete.upper_bounds.insert(
            POSITION_DELETE_FILE_PATH_FIELD_ID,
            Datum::string("s3://bucket/data/file-00004.parquet"),
        );
        let index = PopulatedDeleteFileIndex::new(vec![DeleteFileContext {
            manifest_entry: build_added_manifest_entry(1, &bounded_delete).into(),
            partition_spec_id: 0,
        }]);

        let matching_file =
            build_unpartitioned_data_file_with_path("s3://bucket/data/file-00003.parquet");
        let before_range =
            build_unpartitioned_data_file_with_path("s3://bucket/data/file-00001.parquet");
        let after_range =
            build_unpartitioned_data_file_with_path("s3://bucket/data/file-00005.parquet");

        assert!(can_contain_pos_deletes_for_file(
            &before_range,
            &build_unpartitioned_pos_delete()
        ));

        assert_eq!(
            index
                .get_deletes_for_data_file(&matching_file, Some(0))
                .len(),
            1
        );
        assert!(
            index
                .get_deletes_for_data_file(&before_range, Some(0))
                .is_empty()
        );
        assert!(
            index
                .get_deletes_for_data_file(&after_range, Some(0))
                .is_empty()
        );
    }
}
