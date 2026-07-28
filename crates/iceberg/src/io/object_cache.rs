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

use std::collections::HashMap;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use crate::encryption::EncryptionManager;
use crate::io::FileIO;
use crate::spec::{
    DataFile, Datum, FieldSummary, FormatVersion, Literal, Manifest, ManifestEntry, ManifestFile,
    ManifestList, ManifestListReader, PrimitiveLiteral, SchemaId, SnapshotRef, TableMetadataRef,
};
use crate::{Error, ErrorKind, Result};

const DEFAULT_CACHE_SIZE_BYTES: u64 = 32 * 1024 * 1024; // 32MB
const ARC_ALLOCATION_OVERHEAD: usize = size_of::<usize>() * 2;

#[derive(Clone, Debug)]
pub(crate) enum CachedItem {
    ManifestList(Arc<ManifestList>),
    Manifest(Arc<Manifest>),
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum CachedObjectKey {
    ManifestList((String, FormatVersion, SchemaId)),
    Manifest(String),
}

fn estimated_hashmap_heap_size<K, V>(map: &HashMap<K, V>) -> usize {
    // Account for the bucket values and approximately one control byte per
    // bucket. The exact layout is intentionally left to the standard library.
    map.capacity()
        .saturating_mul(size_of::<(K, V)>().saturating_add(1))
}

fn estimated_primitive_literal_heap_size(literal: &PrimitiveLiteral) -> usize {
    match literal {
        PrimitiveLiteral::String(value) => value.capacity(),
        PrimitiveLiteral::Binary(value) => value.capacity(),
        _ => 0,
    }
}

fn estimated_datum_heap_size(datum: &Datum) -> usize {
    estimated_primitive_literal_heap_size(datum.literal())
}

fn estimated_data_file_heap_size(data_file: &DataFile) -> usize {
    let partition_heap_size = size_of_val(data_file.partition.fields())
        + data_file
            .partition
            .fields()
            .iter()
            .flatten()
            .map(|literal| match literal {
                Literal::Primitive(value) => estimated_primitive_literal_heap_size(value),
                _ => 0,
            })
            .sum::<usize>();

    data_file.file_path.capacity()
        + partition_heap_size
        + estimated_hashmap_heap_size(&data_file.column_sizes)
        + estimated_hashmap_heap_size(&data_file.value_counts)
        + estimated_hashmap_heap_size(&data_file.null_value_counts)
        + estimated_hashmap_heap_size(&data_file.nan_value_counts)
        + estimated_hashmap_heap_size(&data_file.lower_bounds)
        + data_file
            .lower_bounds
            .values()
            .map(estimated_datum_heap_size)
            .sum::<usize>()
        + estimated_hashmap_heap_size(&data_file.upper_bounds)
        + data_file
            .upper_bounds
            .values()
            .map(estimated_datum_heap_size)
            .sum::<usize>()
        + data_file
            .key_metadata
            .as_ref()
            .map_or(0, |value| value.capacity())
        + data_file
            .split_offsets
            .as_ref()
            .map_or(0, |value| value.capacity() * size_of::<i64>())
        + data_file
            .equality_ids
            .as_ref()
            .map_or(0, |value| value.capacity() * size_of::<i32>())
        + data_file
            .referenced_data_file
            .as_ref()
            .map_or(0, |value| value.capacity())
}

fn estimated_manifest_file_heap_size(manifest_file: &ManifestFile) -> usize {
    manifest_file.manifest_path.capacity()
        + manifest_file.partitions.as_ref().map_or(0, |partitions| {
            partitions.capacity() * size_of::<FieldSummary>()
                + partitions
                    .iter()
                    .map(|summary| {
                        summary.lower_bound.as_ref().map_or(0, |value| value.len())
                            + summary.upper_bound.as_ref().map_or(0, |value| value.len())
                    })
                    .sum::<usize>()
        })
        + manifest_file
            .key_metadata
            .as_ref()
            .map_or(0, |value| value.capacity())
}

fn estimated_manifest_list_heap_size(manifest_list: &ManifestList) -> usize {
    ARC_ALLOCATION_OVERHEAD
        + size_of::<ManifestList>()
        + size_of_val(manifest_list.entries())
        + manifest_list
            .entries()
            .iter()
            .map(estimated_manifest_file_heap_size)
            .sum::<usize>()
}

fn estimated_manifest_heap_size(manifest: &Manifest) -> usize {
    ARC_ALLOCATION_OVERHEAD
        + size_of::<Manifest>()
        + size_of_val(manifest.entries())
        + manifest
            .entries()
            .iter()
            .map(|entry| {
                ARC_ALLOCATION_OVERHEAD
                    + size_of::<ManifestEntry>()
                    + estimated_data_file_heap_size(entry.data_file())
            })
            .sum::<usize>()
}

fn estimated_cache_key_heap_size(key: &CachedObjectKey) -> usize {
    match key {
        CachedObjectKey::ManifestList((path, _, _)) | CachedObjectKey::Manifest(path) => {
            path.capacity()
        }
    }
}

fn cache_entry_weight(key: &CachedObjectKey, value: &CachedItem) -> u32 {
    let value_size = match value {
        CachedItem::ManifestList(value) => estimated_manifest_list_heap_size(value),
        CachedItem::Manifest(value) => estimated_manifest_heap_size(value),
    };
    (size_of::<CachedObjectKey>()
        + estimated_cache_key_heap_size(key)
        + size_of::<CachedItem>()
        + value_size)
        .min(u32::MAX as usize) as u32
}

/// Caches metadata objects deserialized from immutable files
#[derive(Clone, Debug)]
pub struct ObjectCache {
    cache: moka::future::Cache<CachedObjectKey, CachedItem>,
    file_io: FileIO,
    cache_disabled: bool,
    encryption_manager: Option<Arc<EncryptionManager>>,
}

impl ObjectCache {
    /// Creates a new [`ObjectCache`]
    /// with the default cache size
    pub(crate) fn new(file_io: FileIO, encryption_manager: Option<Arc<EncryptionManager>>) -> Self {
        Self::new_with_capacity(file_io, DEFAULT_CACHE_SIZE_BYTES, encryption_manager)
    }

    /// Creates a new [`ObjectCache`]
    /// with a specific cache size
    pub(crate) fn new_with_capacity(
        file_io: FileIO,
        cache_size_bytes: u64,
        encryption_manager: Option<Arc<EncryptionManager>>,
    ) -> Self {
        if cache_size_bytes == 0 {
            Self::with_disabled_cache(file_io, encryption_manager)
        } else {
            Self {
                cache: moka::future::Cache::builder()
                    .weigher(cache_entry_weight)
                    .max_capacity(cache_size_bytes)
                    .build(),
                file_io,
                cache_disabled: false,
                encryption_manager,
            }
        }
    }

    /// Creates a new [`ObjectCache`]
    /// with caching disabled
    pub(crate) fn with_disabled_cache(
        file_io: FileIO,
        encryption_manager: Option<Arc<EncryptionManager>>,
    ) -> Self {
        Self {
            cache: moka::future::Cache::new(0),
            file_io,
            cache_disabled: true,
            encryption_manager,
        }
    }

    pub(crate) fn share_entries_with(
        &self,
        file_io: FileIO,
        encryption_manager: Option<Arc<EncryptionManager>>,
    ) -> Self {
        Self {
            cache: self.cache.clone(),
            file_io,
            cache_disabled: self.cache_disabled,
            encryption_manager,
        }
    }

    /// Retrieves an Arc [`Manifest`] from the cache
    /// or retrieves one from FileIO and parses it if not present
    pub(crate) async fn get_manifest(&self, manifest_file: &ManifestFile) -> Result<Arc<Manifest>> {
        if self.cache_disabled {
            return manifest_file
                .load_manifest(&self.file_io)
                .await
                .map(Arc::new);
        }

        let key = CachedObjectKey::Manifest(manifest_file.manifest_path.clone());

        let cache_entry = self
            .cache
            .entry_by_ref(&key)
            .or_try_insert_with(self.fetch_and_parse_manifest(manifest_file))
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to load manifest {}", manifest_file.manifest_path),
                )
                .with_source(err)
            })?
            .into_value();

        match cache_entry {
            CachedItem::Manifest(arc_manifest) => Ok(arc_manifest),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                format!("cached object for key '{key:?}' is not a Manifest"),
            )),
        }
    }

    /// Retrieves an Arc [`ManifestList`] from the cache
    /// or retrieves one from FileIO and parses it if not present
    pub async fn get_manifest_list(
        &self,
        snapshot: &SnapshotRef,
        table_metadata: &TableMetadataRef,
    ) -> Result<Arc<ManifestList>> {
        if self.cache_disabled {
            return ManifestListReader::new(
                snapshot.clone(),
                self.file_io.clone(),
                table_metadata.clone(),
                self.encryption_manager.clone(),
            )
            .load()
            .await
            .map(Arc::new);
        }

        let key = CachedObjectKey::ManifestList((
            snapshot.manifest_list().to_string(),
            table_metadata.format_version,
            snapshot
                .schema_id()
                .unwrap_or_else(|| table_metadata.current_schema_id()),
        ));
        let cache_entry = self
            .cache
            .entry_by_ref(&key)
            .or_try_insert_with(self.fetch_and_parse_manifest_list(snapshot, table_metadata))
            .await
            .map_err(|err| {
                Arc::try_unwrap(err).unwrap_or_else(|err| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Failed to load manifest list in cache",
                    )
                    .with_source(err)
                })
            })?
            .into_value();

        match cache_entry {
            CachedItem::ManifestList(arc_manifest_list) => Ok(arc_manifest_list),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                format!("cached object for path '{key:?}' is not a manifest list"),
            )),
        }
    }

    async fn fetch_and_parse_manifest(&self, manifest_file: &ManifestFile) -> Result<CachedItem> {
        let manifest = manifest_file.load_manifest(&self.file_io).await?;

        Ok(CachedItem::Manifest(Arc::new(manifest)))
    }

    async fn fetch_and_parse_manifest_list(
        &self,
        snapshot: &SnapshotRef,
        table_metadata: &TableMetadataRef,
    ) -> Result<CachedItem> {
        let manifest_list = ManifestListReader::new(
            snapshot.clone(),
            self.file_io.clone(),
            table_metadata.clone(),
            self.encryption_manager.clone(),
        )
        .load()
        .await?;

        Ok(CachedItem::ManifestList(Arc::new(manifest_list)))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use minijinja::value::Value;
    use minijinja::{AutoEscape, Environment, context};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::TableIdent;
    use crate::io::{FileIO, OutputFile};
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestEntry,
        ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Struct, TableMetadata,
    };
    use crate::table::Table;
    use crate::test_utils::test_runtime;

    fn render_template(template: &str, ctx: Value) -> String {
        let mut env = Environment::new();
        env.set_auto_escape_callback(|_| AutoEscape::None);
        env.render_str(template, ctx).unwrap()
    }

    struct TableTestFixture {
        table_location: String,
        table: Table,
    }

    impl TableTestFixture {
        fn new() -> Self {
            let tmp_dir = TempDir::new().unwrap();
            let table_location = tmp_dir.path().join("table1");
            let manifest_list1_location = table_location.join("metadata/manifests_list_1.avro");
            let manifest_list2_location = table_location.join("metadata/manifests_list_2.avro");
            let table_metadata1_location = table_location.join("metadata/v1.json");

            let file_io = FileIO::new_with_fs();

            let table_metadata = {
                let template_json_str = fs::read_to_string(format!(
                    "{}/testdata/example_table_metadata_v2.json",
                    env!("CARGO_MANIFEST_DIR")
                ))
                .unwrap();
                let metadata_json = render_template(&template_json_str, context! {
                    table_location => &table_location,
                    manifest_list_1_location => &manifest_list1_location,
                    manifest_list_2_location => &manifest_list2_location,
                    table_metadata_1_location => &table_metadata1_location,
                });
                serde_json::from_str::<TableMetadata>(&metadata_json).unwrap()
            };

            let table = Table::builder()
                .metadata(table_metadata)
                .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
                .file_io(file_io.clone())
                .metadata_location(table_metadata1_location.as_os_str().to_str().unwrap())
                .runtime(test_runtime())
                .build()
                .unwrap();

            Self {
                table_location: table_location.to_str().unwrap().to_string(),
                table,
            }
        }

        fn next_manifest_file(&self) -> OutputFile {
            self.table
                .file_io()
                .new_output(format!(
                    "{}/metadata/manifest_{}.avro",
                    self.table_location,
                    Uuid::new_v4()
                ))
                .unwrap()
        }

        async fn setup_manifest_files(&mut self) {
            let current_snapshot = self.table.metadata().current_snapshot().unwrap();
            let current_schema = current_snapshot.schema(self.table.metadata()).unwrap();
            let current_partition_spec = self.table.metadata().default_partition_spec();

            // Write data files
            let mut writer = ManifestWriterBuilder::new(
                self.next_manifest_file(),
                Some(current_snapshot.snapshot_id()),
                current_schema.clone(),
                current_partition_spec.as_ref().clone(),
            )
            .build_v2_data();
            writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!("{}/1.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(100))]))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
            let data_file_manifest = writer.write_manifest_file().await.unwrap();

            // Write to manifest list
            let manifest_list_writer = self
                .table
                .file_io()
                .new_output(current_snapshot.manifest_list())
                .unwrap()
                .writer()
                .await
                .unwrap();
            let mut manifest_list_write = ManifestListWriter::v2(
                manifest_list_writer,
                current_snapshot.snapshot_id(),
                current_snapshot.parent_snapshot_id(),
                current_snapshot.sequence_number(),
            );
            manifest_list_write
                .add_manifests(vec![data_file_manifest].into_iter())
                .unwrap();
            manifest_list_write.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_get_manifest_list_and_manifest_from_disabled_cache() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let object_cache = ObjectCache::with_disabled_cache(fixture.table.file_io().clone(), None);

        let result_manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();

        assert_eq!(result_manifest_list.entries().len(), 1);

        let manifest_file = result_manifest_list.entries().first().unwrap();
        let result_manifest = object_cache.get_manifest(manifest_file).await.unwrap();

        assert_eq!(
            result_manifest
                .entries()
                .first()
                .unwrap()
                .file_path()
                .split("/")
                .last()
                .unwrap(),
            "1.parquet"
        );
    }

    #[tokio::test]
    async fn test_get_manifest_list_and_manifest_from_default_cache() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let object_cache = ObjectCache::new(fixture.table.file_io().clone(), None);

        // not in cache
        let result_manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();

        assert_eq!(result_manifest_list.entries().len(), 1);

        // retrieve cached version
        let result_manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();

        assert_eq!(result_manifest_list.entries().len(), 1);

        let shared_cache = object_cache.share_entries_with(fixture.table.file_io().clone(), None);
        let shared_manifest_list = shared_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&result_manifest_list, &shared_manifest_list));

        let manifest_file = result_manifest_list.entries().first().unwrap();

        // not in cache
        let result_manifest = object_cache.get_manifest(manifest_file).await.unwrap();

        assert_eq!(
            result_manifest
                .entries()
                .first()
                .unwrap()
                .file_path()
                .split("/")
                .last()
                .unwrap(),
            "1.parquet"
        );

        // retrieve cached version
        let result_manifest = object_cache.get_manifest(manifest_file).await.unwrap();

        assert_eq!(
            result_manifest
                .entries()
                .first()
                .unwrap()
                .file_path()
                .split("/")
                .last()
                .unwrap(),
            "1.parquet"
        );
    }

    #[tokio::test]
    async fn test_get_manifest_list_for_v1_snapshot_without_schema_id() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().join("table1");
        let manifest_list_location = table_location.join("metadata/manifest-list.avro");
        let table_metadata_location = table_location.join("metadata/v1.json");
        let file_io = FileIO::new_with_fs();

        let template_json = fs::read_to_string(format!(
            "{}/testdata/example_table_metadata_v1.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        let metadata_json = render_template(&template_json, context! {
            table_location => &table_location,
            manifest_list_location => &manifest_list_location,
            table_metadata_location => &table_metadata_location,
        });
        let table = Table::builder()
            .metadata(serde_json::from_str::<TableMetadata>(&metadata_json).unwrap())
            .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
            .file_io(file_io)
            .metadata_location(table_metadata_location.to_string_lossy())
            .runtime(test_runtime())
            .build()
            .unwrap();
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.schema_id(), None);

        let mut manifest_writer = ManifestWriterBuilder::new(
            table
                .file_io()
                .new_output(
                    table_location
                        .join("metadata/manifest.avro")
                        .to_string_lossy(),
                )
                .unwrap(),
            Some(snapshot.snapshot_id()),
            snapshot.schema(table.metadata()).unwrap(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v1();
        manifest_writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .content(DataContentType::Data)
                            .file_path(
                                table_location
                                    .join("1.parquet")
                                    .to_string_lossy()
                                    .into_owned(),
                            )
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(100)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let manifest_file = manifest_writer.write_manifest_file().await.unwrap();

        let writer = table
            .file_io()
            .new_output(snapshot.manifest_list())
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut manifest_list_writer = ManifestListWriter::v1(
            writer,
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
        );
        manifest_list_writer
            .add_manifests([manifest_file].into_iter())
            .unwrap();
        manifest_list_writer.close().await.unwrap();

        let manifest_list = table
            .object_cache()
            .get_manifest_list(snapshot, &table.metadata_ref())
            .await
            .unwrap();
        assert_eq!(manifest_list.entries().len(), 1);
    }

    #[test]
    fn test_data_file_weight_includes_column_statistics() {
        use std::collections::HashMap;

        let data_file = |columns| {
            let values = (0..columns).map(|id| (id, 1)).collect::<HashMap<_, _>>();
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path("s3://bucket/table/data.parquet".to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(1)
                .partition(Struct::empty())
                .column_sizes(values.clone())
                .value_counts(values.clone())
                .null_value_counts(values.clone())
                .nan_value_counts(values)
                .build()
                .unwrap()
        };

        let small = estimated_data_file_heap_size(&data_file(1));
        let large = estimated_data_file_heap_size(&data_file(100));
        assert!(
            large > small * 10,
            "{large} should be much larger than {small}"
        );
        assert!(large > size_of::<DataFile>());
    }
}
