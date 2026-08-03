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
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, StringArray, StructArray};
use futures::{StreamExt, TryStreamExt};
use tokio::sync::oneshot::{Receiver, channel};

use super::delete_filter::{DeleteFilter, EqDelLoadGuard, PosDelLoadAction};
use crate::arrow::delete_file_loader::BasicDeleteFileLoader;
use crate::arrow::scan_metrics::ScanMetrics;
use crate::arrow::{arrow_primitive_to_literal, arrow_schema_to_schema};
use crate::delete_vector::{DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE, DeleteVector};
use crate::expr::Predicate::AlwaysTrue;
use crate::expr::{Predicate, Reference};
use crate::io::{FileIO, InputFile};
use crate::puffin::{Blob, DELETION_VECTOR_V1, PuffinReader};
use crate::runtime::Runtime;
use crate::scan::{ArrowRecordBatchStream, FileScanTaskDeleteFile};
use crate::spec::{
    DataContentType, DataFileFormat, Datum, ListType, MapType, NestedField, NestedFieldRef,
    PartnerAccessor, PrimitiveType, Schema, SchemaRef, SchemaWithPartnerVisitor, StructType, Type,
    VariantType, visit_schema_with_partner,
};
use crate::{Error, ErrorKind, Result};

#[derive(Clone, Debug)]
pub(crate) struct CachingDeleteFileLoader {
    basic_delete_file_loader: BasicDeleteFileLoader,
    concurrency_limit_data_files: usize,
    /// Shared filter state to allow caching loaded deletes across multiple
    /// calls to `load_deletes` (e.g., across multiple file scan tasks).
    delete_filter: DeleteFilter,
    runtime: Runtime,
}

// Intermediate context during processing of a delete file task.
enum DeleteFileContext {
    ExistingEqDel,
    ExistingPosDel,
    PosDels {
        file_path: String,
        stream: ArrowRecordBatchStream,
    },
    DelVec {
        file_path: String,
        input_file: InputFile,
        referenced_data_file: Option<String>,
        blob_offset: u64,
        blob_length: u64,
    },
    FreshEqDel {
        batch_stream: ArrowRecordBatchStream,
        equality_ids: HashSet<i32>,
        load_guard: EqDelLoadGuard,
    },
}

// Final result of the processing of a delete file task before
// results are fully merged into the DeleteFileManager's state
enum ParsedDeleteFileContext {
    DelVecs {
        file_path: String,
        results: HashMap<String, DeleteVector>,
    },
    EqDel,
    ExistingPosDel,
}

#[allow(unused_variables)]
impl CachingDeleteFileLoader {
    pub(crate) fn new(
        file_io: FileIO,
        concurrency_limit_data_files: usize,
        runtime: Runtime,
    ) -> Self {
        let scan_metrics = ScanMetrics::new();
        CachingDeleteFileLoader {
            basic_delete_file_loader: BasicDeleteFileLoader::new(file_io, scan_metrics),
            concurrency_limit_data_files,
            delete_filter: DeleteFilter::new(runtime.clone()),
            runtime,
        }
    }

    pub(crate) fn with_scan_metrics(mut self, scan_metrics: ScanMetrics) -> Self {
        self.basic_delete_file_loader = BasicDeleteFileLoader::new(
            self.basic_delete_file_loader.file_io().clone(),
            scan_metrics,
        );
        self
    }

    /// Initiates loading of all deletes for all the specified tasks
    ///
    /// The returned future completes once all newly claimed delete files have loaded.
    /// Equality deletes already being loaded by another concurrent call are shared; the
    /// returned DeleteFilter awaits their published result when queried.
    ///
    ///  * Create a single stream of all delete file tasks irrespective of type,
    ///    so that we can respect the combined concurrency limit
    ///  * We then process each in two phases: load and parse.
    ///  * for positional deletes the load phase instantiates an ArrowRecordBatchStream to
    ///    stream the file contents out
    ///  * for eq deletes, we first check if the EQ delete is already loaded or being loaded by
    ///    another concurrently processing data file scan task. If it is, we skip it.
    ///    If not, the DeleteFilter records a shared result receiver and returns a load guard that
    ///    prevents other tasks from loading the same file. The task streams and parses the file,
    ///    then uses the guard to publish success or failure to every waiter. Failures stay cached
    ///    so that consumers arriving later still observe the original error: retryable failures
    ///    (including cancelled loads) can be reclaimed and retried by a later call, while
    ///    non-retryable failures keep serving the recorded error instead of re-reading a file
    ///    that cannot be read successfully.
    ///  * When this gets updated to add support for delete vectors, the load phase will return
    ///    a PuffinReader for them.
    ///  * The parse phase parses each record batch stream according to its associated data type.
    ///    The result of this is a map of data file paths to delete vectors for the positional
    ///    delete tasks (and in future for the delete vector tasks). For equality delete
    ///    file tasks, this results in an unbound Predicate.
    ///  * The unbound Predicates resulting from equality deletes are committed through their load
    ///    guards, which store successful predicates and publish the result to concurrent waiters.
    ///  * The results of all of these futures are awaited on in parallel with the specified
    ///    level of concurrency and collected into a vec. We then combine all the delete
    ///    vector maps that resulted from any positional delete or delete vector files into a
    ///    single map and persist it in the state.
    ///
    ///
    ///  Conceptually, the data flow is like this:
    /// ```none
    ///                                          FileScanTaskDeleteFile
    ///                                                     |
    ///                                             Skip Started EQ Deletes
    ///                                                     |
    ///                                                     |
    ///                                       [load recordbatch stream / puffin]
    ///                                             DeleteFileContext
    ///                                                     |
    ///                                                     |
    ///                       +-----------------------------+--------------------------+
    ///                     Pos Del           Del Vec (Not yet Implemented)         EQ Del
    ///                       |                             |                          |
    ///              [parse pos del stream]         [parse del vec puffin]       [parse eq del]
    ///          HashMap<String, RoaringTreeMap> HashMap<String, RoaringTreeMap> (Predicate, LoadGuard)
    ///                       |                             |                          |
    ///                       |                             |              [publish result/state]
    ///                       |                             |                          ()
    ///                       |                             |                          |
    ///                       +-----------------------------+--------------------------+
    ///                                                     |
    ///                                             [buffer unordered]
    ///                                                     |
    ///                                            [combine del vectors]
    ///                                        HashMap<String, RoaringTreeMap>
    ///                                                     |
    ///                                        [persist del vectors to state]
    ///                                                    ()
    ///                                                    |
    ///                                                    |
    ///                                                 [join!]
    /// ```
    pub(crate) fn load_deletes(
        &self,
        delete_file_entries: &[FileScanTaskDeleteFile],
        schema: SchemaRef,
    ) -> Receiver<Result<DeleteFilter>> {
        let (tx, rx) = channel();

        let stream_items = delete_file_entries
            .iter()
            .map(|t| {
                (
                    t.clone(),
                    self.basic_delete_file_loader.clone(),
                    self.delete_filter.clone(),
                    schema.clone(),
                )
            })
            .collect::<Vec<_>>();
        let task_stream = futures::stream::iter(stream_items);

        let del_filter = self.delete_filter.clone();
        let concurrency_limit_data_files = self.concurrency_limit_data_files;
        let basic_delete_file_loader = self.basic_delete_file_loader.clone();
        self.runtime.io().spawn(async move {
            let result = async move {
                let mut del_filter = del_filter;
                let basic_delete_file_loader = basic_delete_file_loader.clone();

                let mut results_stream = task_stream
                    .map(move |(task, file_io, del_filter, schema)| {
                        let basic_delete_file_loader = basic_delete_file_loader.clone();
                        async move {
                            Self::load_file_for_task(
                                &task,
                                basic_delete_file_loader.clone(),
                                del_filter,
                                schema,
                            )
                            .await
                        }
                    })
                    .map(move |ctx| {
                        Ok(async { Self::parse_file_content_for_task(ctx.await?).await })
                    })
                    .try_buffer_unordered(concurrency_limit_data_files);

                while let Some(item) = results_stream.next().await {
                    let item = item?;
                    if let ParsedDeleteFileContext::DelVecs { file_path, results } = item {
                        for (data_file_path, delete_vector) in results.into_iter() {
                            del_filter.upsert_delete_vector(data_file_path, delete_vector);
                        }
                        // Mark the positional delete file as fully loaded so waiters can proceed
                        del_filter.finish_pos_del_load(&file_path);
                    }
                }

                Ok(del_filter)
            }
            .await;

            let _ = tx.send(result);
        });

        rx
    }

    async fn load_file_for_task(
        task: &FileScanTaskDeleteFile,
        basic_delete_file_loader: BasicDeleteFileLoader,
        del_filter: DeleteFilter,
        schema: SchemaRef,
    ) -> Result<DeleteFileContext> {
        match task.file_type {
            DataContentType::PositionDeletes => {
                let load_key = Self::position_delete_load_key(task)?;
                match del_filter.try_start_pos_del_load(&load_key) {
                    PosDelLoadAction::AlreadyLoaded => Ok(DeleteFileContext::ExistingPosDel),
                    PosDelLoadAction::WaitFor(notified) => {
                        // Positional deletes are accessed synchronously by ArrowReader.
                        // We must wait here to ensure the data is ready before returning,
                        // otherwise ArrowReader might get an empty/partial result.
                        notified.await;
                        Ok(DeleteFileContext::ExistingPosDel)
                    }
                    PosDelLoadAction::Load => {
                        if task.file_format == DataFileFormat::Puffin {
                            let (blob_offset, blob_length) = Self::puffin_blob_range(task)?;
                            Ok(DeleteFileContext::DelVec {
                                file_path: load_key,
                                input_file: basic_delete_file_loader
                                    .file_io()
                                    .new_input(&task.file_path)?,
                                referenced_data_file: task.referenced_data_file.clone(),
                                blob_offset,
                                blob_length,
                            })
                        } else {
                            Ok(DeleteFileContext::PosDels {
                                file_path: load_key,
                                stream: basic_delete_file_loader
                                    .parquet_to_batch_stream(
                                        &task.file_path,
                                        task.file_size_in_bytes,
                                        task.key_metadata.as_deref(),
                                    )
                                    .await?,
                            })
                        }
                    }
                }
            }

            DataContentType::EqualityDeletes => {
                let Some(load_guard) = del_filter.try_start_eq_del_load(&task.file_path) else {
                    return Ok(DeleteFileContext::ExistingEqDel);
                };

                let load_result: Result<_> = async {
                    // Per the Iceberg spec, evolve schema for equality deletes but only for the
                    // equality_ids columns, not all table columns.
                    let equality_ids_vec = task.equality_ids.clone().ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "Equality delete file is missing equality IDs",
                        )
                        .with_context("file_path", &task.file_path)
                    })?;
                    let evolved_stream = BasicDeleteFileLoader::evolve_schema(
                        basic_delete_file_loader
                            .parquet_to_batch_stream(
                                &task.file_path,
                                task.file_size_in_bytes,
                                task.key_metadata.as_deref(),
                            )
                            .await?,
                        schema,
                        &equality_ids_vec,
                    )
                    .await?;

                    Ok((evolved_stream, HashSet::from_iter(equality_ids_vec)))
                }
                .await;

                match load_result {
                    Ok((batch_stream, equality_ids)) => Ok(DeleteFileContext::FreshEqDel {
                        batch_stream,
                        equality_ids,
                        load_guard,
                    }),
                    Err(error) => {
                        load_guard.fail(&error);
                        Err(error)
                    }
                }
            }

            DataContentType::Data => Err(Error::new(
                ErrorKind::Unexpected,
                "tasks with files of type Data not expected here",
            )),
        }
    }

    fn position_delete_load_key(task: &FileScanTaskDeleteFile) -> Result<String> {
        if task.file_format == DataFileFormat::Puffin {
            let (offset, length) = Self::puffin_blob_range(task)?;
            Ok(format!("{}#{offset}:{length}", task.file_path))
        } else {
            Ok(task.file_path.clone())
        }
    }

    fn puffin_blob_range(task: &FileScanTaskDeleteFile) -> Result<(u64, u64)> {
        let offset = task.content_offset.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector {} is missing content_offset",
                    task.file_path
                ),
            )
        })?;
        let length = task.content_size_in_bytes.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector {} is missing content_size_in_bytes",
                    task.file_path
                ),
            )
        })?;
        let offset = u64::try_from(offset).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("deletion vector {} has a negative offset", task.file_path),
            )
        })?;
        let length = u64::try_from(length).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("deletion vector {} has a negative length", task.file_path),
            )
        })?;
        if length == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("deletion vector {} has an empty blob", task.file_path),
            ));
        }
        let end = offset.checked_add(length).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("deletion vector {} blob range overflows", task.file_path),
            )
        })?;
        if end > task.file_size_in_bytes {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector {} blob range {offset}..{end} exceeds file size {}",
                    task.file_path, task.file_size_in_bytes
                ),
            ));
        }
        Ok((offset, length))
    }

    async fn parse_file_content_for_task(
        ctx: DeleteFileContext,
    ) -> Result<ParsedDeleteFileContext> {
        match ctx {
            DeleteFileContext::ExistingEqDel => Ok(ParsedDeleteFileContext::EqDel),
            DeleteFileContext::ExistingPosDel => Ok(ParsedDeleteFileContext::ExistingPosDel),
            DeleteFileContext::PosDels { file_path, stream } => {
                let del_vecs = Self::parse_positional_deletes_record_batch_stream(stream).await?;
                Ok(ParsedDeleteFileContext::DelVecs {
                    file_path,
                    results: del_vecs,
                })
            }
            DeleteFileContext::DelVec {
                file_path,
                input_file,
                referenced_data_file,
                blob_offset,
                blob_length,
            } => {
                let del_vecs = Self::parse_delete_vector_puffin(
                    input_file,
                    referenced_data_file,
                    blob_offset,
                    blob_length,
                )
                .await?;
                Ok(ParsedDeleteFileContext::DelVecs {
                    file_path,
                    results: del_vecs,
                })
            }
            DeleteFileContext::FreshEqDel {
                batch_stream,
                equality_ids,
                load_guard,
            } => {
                let result =
                    Self::parse_equality_deletes_record_batch_stream(batch_stream, equality_ids)
                        .await;

                match result {
                    Ok(predicate) => {
                        load_guard.finish(predicate);
                        Ok(ParsedDeleteFileContext::EqDel)
                    }
                    Err(error) => {
                        load_guard.fail(&error);
                        Err(error)
                    }
                }
            }
        }
    }

    /// Parses a record batch stream coming from positional delete files
    ///
    /// Returns a map of data file path to a delete vector
    async fn parse_positional_deletes_record_batch_stream(
        mut stream: ArrowRecordBatchStream,
    ) -> Result<HashMap<String, DeleteVector>> {
        let mut result: HashMap<String, DeleteVector> = HashMap::default();

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let schema = batch.schema();
            let columns = batch.columns();

            let Some(file_paths) = columns[0].as_any().downcast_ref::<StringArray>() else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Could not downcast file paths array to StringArray",
                ));
            };
            let Some(positions) = columns[1].as_any().downcast_ref::<Int64Array>() else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Could not downcast positions array to Int64Array",
                ));
            };

            for (file_path, pos) in file_paths.iter().zip(positions.iter()) {
                let (Some(file_path), Some(pos)) = (file_path, pos) else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "null values in delete file",
                    ));
                };
                if pos < 0 {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("negative position in delete file {file_path}: {pos}"),
                    ));
                }

                result
                    .entry(file_path.to_string())
                    .or_default()
                    .insert(pos as u64);
            }
        }

        Ok(result)
    }

    async fn parse_delete_vector_puffin(
        input_file: InputFile,
        referenced_data_file: Option<String>,
        blob_offset: u64,
        blob_length: u64,
    ) -> Result<HashMap<String, DeleteVector>> {
        if let Some(referenced_data_file) = referenced_data_file {
            let file_read = input_file.reader().await?;
            let blob_end = blob_offset.checked_add(blob_length).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "deletion vector blob range overflows",
                )
            })?;
            let bytes = file_read.read(blob_offset..blob_end).await?.to_vec();
            let blob = Blob::builder()
                .r#type(DELETION_VECTOR_V1.to_string())
                .fields(vec![])
                .snapshot_id(-1)
                .sequence_number(-1)
                .data(bytes)
                .properties(HashMap::from([(
                    DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE.to_string(),
                    referenced_data_file.clone(),
                )]))
                .build();
            let delete_vector = DeleteVector::from_puffin_blob(blob)?;

            return Ok(HashMap::from([(referenced_data_file, delete_vector)]));
        }

        let puffin_reader = PuffinReader::new(input_file);
        let file_metadata = puffin_reader.file_metadata().await?;
        let blob_metadata = file_metadata
            .blobs
            .iter()
            .find(|blob| blob.offset() == blob_offset && blob.length() == blob_length)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "deletion vector blob metadata not found",
                )
            })?;
        if blob_metadata.blob_type() != DELETION_VECTOR_V1 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("unexpected Puffin blob type: {}", blob_metadata.blob_type()),
            ));
        }

        let blob = puffin_reader.blob(blob_metadata).await?;
        let referenced_data_file = blob
            .properties()
            .get(DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "deletion vector referenced-data-file property missing",
                )
            })?
            .to_owned();
        let delete_vector = DeleteVector::from_puffin_blob(blob)?;

        Ok(HashMap::from([(referenced_data_file, delete_vector)]))
    }

    async fn parse_equality_deletes_record_batch_stream(
        mut stream: ArrowRecordBatchStream,
        equality_ids: HashSet<i32>,
    ) -> Result<Predicate> {
        let mut row_predicates = Vec::new();
        let mut batch_schema_iceberg: Option<Schema> = None;
        let accessor = EqDelRecordBatchPartnerAccessor;

        while let Some(record_batch) = stream.next().await {
            let record_batch = record_batch?;

            if record_batch.num_columns() == 0 {
                return Ok(AlwaysTrue);
            }

            let schema = match &batch_schema_iceberg {
                Some(schema) => schema,
                None => {
                    let schema = arrow_schema_to_schema(record_batch.schema().as_ref())?;
                    batch_schema_iceberg = Some(schema);
                    batch_schema_iceberg.as_ref().unwrap()
                }
            };

            let root_array: ArrayRef = Arc::new(StructArray::from(record_batch));

            let mut processor = EqDelColumnProcessor::new(&equality_ids);
            visit_schema_with_partner(schema, &root_array, &mut processor, &accessor)?;

            let mut datum_columns_with_names = processor.finish()?;
            if datum_columns_with_names.is_empty() {
                continue;
            }

            // Iceberg spec (Equality Delete Files): a null data value never equals a non-null
            // delete value, so a row with a null equality column must be kept. Build the keep
            // predicate as `col IS NULL OR col != v` (`col IS NOT NULL` for a null delete value);
            // a bare `col != v` drops nulls.
            #[allow(clippy::len_zero)]
            while datum_columns_with_names[0].0.len() > 0 {
                let mut row_keep_predicate = Predicate::AlwaysFalse;
                for &mut (ref mut column, ref field_name) in &mut datum_columns_with_names {
                    if let Some(item) = column.next() {
                        let reference = Reference::new(field_name.clone());
                        let cell_keep_predicate = if let Some(datum) = item? {
                            reference
                                .clone()
                                .is_null()
                                .or(reference.not_equal_to(datum.clone()))
                        } else {
                            reference.is_not_null()
                        };
                        row_keep_predicate = row_keep_predicate.or(cell_keep_predicate);
                    }
                }
                row_predicates.push(row_keep_predicate);
            }
        }

        // All row predicates are combined to a single predicate by creating a balanced binary tree.
        // Using a simple fold would result in a deeply nested predicate that can cause a stack overflow.
        while row_predicates.len() > 1 {
            let mut next_level = Vec::with_capacity(row_predicates.len().div_ceil(2));
            let mut iter = row_predicates.into_iter();
            while let Some(p1) = iter.next() {
                if let Some(p2) = iter.next() {
                    next_level.push(p1.and(p2));
                } else {
                    next_level.push(p1);
                }
            }
            row_predicates = next_level;
        }

        match row_predicates.pop() {
            Some(p) => Ok(p),
            None => Ok(AlwaysTrue),
        }
    }
}

struct EqDelColumnProcessor<'a> {
    equality_ids: &'a HashSet<i32>,
    collected_columns: Vec<(ArrayRef, String, Type)>,
}

impl<'a> EqDelColumnProcessor<'a> {
    fn new(equality_ids: &'a HashSet<i32>) -> Self {
        Self {
            equality_ids,
            collected_columns: Vec::with_capacity(equality_ids.len()),
        }
    }

    #[allow(clippy::type_complexity)]
    fn finish(
        self,
    ) -> Result<
        Vec<(
            Box<dyn ExactSizeIterator<Item = Result<Option<Datum>>>>,
            String,
        )>,
    > {
        self.collected_columns
            .into_iter()
            .map(|(array, field_name, field_type)| {
                let primitive_type = field_type
                    .as_primitive_type()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::Unexpected, "field is not a primitive type")
                    })?
                    .clone();

                let lit_vec = arrow_primitive_to_literal(&array, &field_type)?;
                let datum_iterator: Box<dyn ExactSizeIterator<Item = Result<Option<Datum>>>> =
                    Box::new(lit_vec.into_iter().map(move |c| {
                        c.map(|literal| {
                            literal
                                .as_primitive_literal()
                                .map(|primitive_literal| {
                                    Datum::new(primitive_type.clone(), primitive_literal)
                                })
                                .ok_or(Error::new(
                                    ErrorKind::Unexpected,
                                    "failed to convert to primitive literal",
                                ))
                        })
                        .transpose()
                    }));

                Ok((datum_iterator, field_name))
            })
            .collect::<Result<Vec<_>>>()
    }
}

impl SchemaWithPartnerVisitor<ArrayRef> for EqDelColumnProcessor<'_> {
    type T = ();

    fn schema(&mut self, _schema: &Schema, _partner: &ArrayRef, _value: ()) -> Result<()> {
        Ok(())
    }

    fn field(&mut self, field: &NestedFieldRef, partner: &ArrayRef, _value: ()) -> Result<()> {
        if self.equality_ids.contains(&field.id) && field.field_type.as_primitive_type().is_some() {
            self.collected_columns.push((
                partner.clone(),
                field.name.clone(),
                field.field_type.as_ref().clone(),
            ));
        }
        Ok(())
    }

    fn r#struct(
        &mut self,
        _struct: &StructType,
        _partner: &ArrayRef,
        _results: Vec<()>,
    ) -> Result<()> {
        Ok(())
    }

    fn list(&mut self, _list: &ListType, _partner: &ArrayRef, _value: ()) -> Result<()> {
        Ok(())
    }

    fn map(
        &mut self,
        _map: &MapType,
        _partner: &ArrayRef,
        _key_value: (),
        _value: (),
    ) -> Result<()> {
        Ok(())
    }

    fn primitive(&mut self, _primitive: &PrimitiveType, _partner: &ArrayRef) -> Result<()> {
        Ok(())
    }

    fn variant(&mut self, _v: &VariantType, _partner: &ArrayRef) -> Result<()> {
        Ok(())
    }
}

struct EqDelRecordBatchPartnerAccessor;

impl PartnerAccessor<ArrayRef> for EqDelRecordBatchPartnerAccessor {
    fn struct_partner<'a>(&self, schema_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Ok(schema_partner)
    }

    fn field_partner<'a>(
        &self,
        struct_partner: &'a ArrayRef,
        field: &NestedField,
    ) -> Result<&'a ArrayRef> {
        let Some(struct_array) = struct_partner.as_any().downcast_ref::<StructArray>() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Expected struct array for field extraction",
            ));
        };

        // Find the field by name within the struct
        for (i, field_def) in struct_array.fields().iter().enumerate() {
            if field_def.name() == &field.name {
                return Ok(struct_array.column(i));
            }
        }

        Err(Error::new(
            ErrorKind::Unexpected,
            format!("Field {} not found in parent struct", field.name),
        ))
    }

    fn list_element_partner<'a>(&self, _list_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "List columns are unsupported in equality deletes",
        ))
    }

    fn map_key_partner<'a>(&self, _map_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Map columns are unsupported in equality deletes",
        ))
    }

    fn map_value_partner<'a>(&self, _map_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Map columns are unsupported in equality deletes",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::cast::AsArray;
    use arrow_array::{
        ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
    };
    use arrow_schema::{DataType, Field, Fields};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::delete_filter::tests::setup;
    use crate::delete_vector::{
        DELETION_VECTOR_PROPERTY_CARDINALITY, DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE,
    };
    use crate::puffin::{CompressionCodec, PuffinWriter};
    use crate::scan::{FileScanTask, FileScanTaskDeleteFile};
    use crate::spec::{DataContentType, DataFileFormat, Schema};

    #[tokio::test]
    async fn test_delete_file_loader_parse_equality_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();
        let file_io = FileIO::new_with_fs();

        let eq_delete_file_path = setup_write_equality_delete_file_1(table_location);

        let basic_delete_file_loader =
            BasicDeleteFileLoader::new(file_io.clone(), ScanMetrics::new());
        let record_batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(
                &eq_delete_file_path,
                std::fs::metadata(&eq_delete_file_path).unwrap().len(),
                None,
            )
            .await
            .expect("could not get batch stream");

        let eq_ids = HashSet::from_iter(vec![2, 3, 4, 6, 8]);

        let parsed_eq_delete = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            record_batch_stream,
            eq_ids,
        )
        .await
        .expect("error parsing batch stream");

        let expected = "((((((y IS NULL) OR (y != 1)) OR ((z IS NULL) OR (z != 100))) OR ((a IS NULL) OR (a != \"HELP\"))) OR ((sa IS NULL) OR (sa != 4))) OR ((b IS NULL) OR (b != 62696E6172795F64617461))) AND ((((((y IS NULL) OR (y != 2)) OR (z IS NOT NULL)) OR (a IS NOT NULL)) OR ((sa IS NULL) OR (sa != 5))) OR (b IS NOT NULL))".to_string();

        assert_eq!(parsed_eq_delete.to_string(), expected);
    }

    // An equality delete keyed on a nullable column must not delete rows whose value in that
    // column is null: per the Iceberg spec (Equality Delete Files), a null matches only a null
    // delete value. Mirrors Iceberg-Java's
    // TestSparkReaderDeletes.testEqualityDeleteWithSchemaEvolution.
    #[tokio::test]
    async fn test_equality_delete_predicate_preserves_null_rows() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![simple_field(
            "status",
            DataType::Utf8,
            true,
            "3",
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![
                Arc::new(StringArray::from(vec![Some("INACTIVE")])) as ArrayRef,
            ])
            .unwrap();
        let stream: ArrowRecordBatchStream = futures::stream::iter(vec![Ok(batch)]).boxed();

        let predicate = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            stream,
            HashSet::from_iter(vec![3]),
        )
        .await
        .expect("error parsing equality delete stream");

        assert_eq!(
            predicate.to_string(),
            "(status IS NULL) OR (status != \"INACTIVE\")"
        );
    }

    // A delete row with a null value in the column matches only rows whose value is null (Iceberg
    // spec, Equality Delete Files), so the keep predicate is `col IS NOT NULL`.
    #[tokio::test]
    async fn test_equality_delete_predicate_matches_null_delete_value() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![simple_field(
            "status",
            DataType::Utf8,
            true,
            "3",
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![
            None as Option<&str>,
        ])) as ArrayRef])
        .unwrap();
        let stream: ArrowRecordBatchStream = futures::stream::iter(vec![Ok(batch)]).boxed();

        let predicate = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            stream,
            HashSet::from_iter(vec![3]),
        )
        .await
        .expect("error parsing equality delete stream");

        assert_eq!(predicate.to_string(), "status IS NOT NULL");
    }

    // A delete row with several equality columns keeps a data row that differs in any one of them,
    // so the per-column keep predicates are OR-ed.
    #[tokio::test]
    async fn test_equality_delete_predicate_multiple_columns() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            simple_field("id", DataType::Int64, true, "1"),
            simple_field("status", DataType::Utf8, true, "3"),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("X")])) as ArrayRef,
        ])
        .unwrap();
        let stream: ArrowRecordBatchStream = futures::stream::iter(vec![Ok(batch)]).boxed();

        let predicate = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            stream,
            HashSet::from_iter(vec![1, 3]),
        )
        .await
        .expect("error parsing equality delete stream");

        assert_eq!(
            predicate.to_string(),
            "((id IS NULL) OR (id != 1)) OR ((status IS NULL) OR (status != \"X\"))"
        );
    }

    // A data row is kept only if it matches none of the delete rows, so the per-row keep
    // predicates are AND-ed.
    #[tokio::test]
    async fn test_equality_delete_predicate_multiple_delete_rows() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![simple_field(
            "status",
            DataType::Utf8,
            true,
            "3",
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![
            Some("A"),
            Some("B"),
        ])) as ArrayRef])
        .unwrap();
        let stream: ArrowRecordBatchStream = futures::stream::iter(vec![Ok(batch)]).boxed();

        let predicate = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            stream,
            HashSet::from_iter(vec![3]),
        )
        .await
        .expect("error parsing equality delete stream");

        assert_eq!(
            predicate.to_string(),
            "((status IS NULL) OR (status != \"A\")) AND ((status IS NULL) OR (status != \"B\"))"
        );
    }

    #[tokio::test]
    async fn test_equality_delete_read_failure_is_cached_and_fresh_loader_retries() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();
        let file_io = FileIO::from_path(table_location).unwrap().build().unwrap();
        let delete_file_loader =
            CachingDeleteFileLoader::new(file_io.clone(), 10, Runtime::current());

        let table_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    crate::spec::NestedField::optional(
                        2,
                        "y",
                        crate::spec::Type::Primitive(crate::spec::PrimitiveType::Long),
                    )
                    .into(),
                    crate::spec::NestedField::optional(
                        3,
                        "z",
                        crate::spec::Type::Primitive(crate::spec::PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let equality_delete_path = format!("{table_location}/equality-deletes-1.parquet");
        let mut equality_delete_task = FileScanTaskDeleteFile::builder()
            .with_file_path(equality_delete_path.clone())
            .with_file_size_in_bytes(0)
            .with_file_type(DataContentType::EqualityDeletes)
            .with_partition_spec_id(0)
            .with_equality_ids(Some(vec![2, 3]))
            .build();

        let first_error = tokio::time::timeout(
            Duration::from_secs(5),
            delete_file_loader.load_deletes(&[equality_delete_task.clone()], table_schema.clone()),
        )
        .await
        .expect("missing equality delete read hung")
        .expect("delete loader task ended without a result")
        .expect_err("reading a missing equality delete file should fail");

        let make_data_file_task = |equality_delete_task: FileScanTaskDeleteFile| {
            FileScanTask::builder()
                .with_file_size_in_bytes(0)
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/data.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(table_schema.clone())
                .with_project_field_ids(vec![2, 3])
                .with_deletes(vec![equality_delete_task])
                .with_case_sensitive(false)
                .build()
        };

        // The read failure is not retryable, so the same loader keeps serving the
        // original error to consumers that arrive after the failure — even though
        // they never observed the `Loading` state — instead of a generic missing
        // predicate error or a doomed re-read.
        let late_filter = tokio::time::timeout(
            Duration::from_secs(5),
            delete_file_loader.load_deletes(&[equality_delete_task.clone()], table_schema.clone()),
        )
        .await
        .expect("second equality delete load hung")
        .expect("delete loader task ended without a result")
        .expect("skipping a cached failed load must not fail the load phase");
        let cached_error = tokio::time::timeout(
            Duration::from_secs(5),
            late_filter.build_equality_delete_predicate(&make_data_file_task(
                equality_delete_task.clone(),
            )),
        )
        .await
        .expect("cached equality delete failure lookup hung")
        .expect_err("cached equality delete failure should propagate to late consumers");
        assert_eq!(cached_error.kind(), first_error.kind());
        assert_eq!(cached_error.message(), first_error.message());
        assert_eq!(cached_error.retryable(), first_error.retryable());

        // A fresh loader (e.g. a new scan attempt) is unaffected by the cached
        // failure and can load the file once it becomes readable.
        assert_eq!(
            setup_write_equality_delete_file_1(table_location),
            equality_delete_path
        );
        equality_delete_task.file_size_in_bytes =
            std::fs::metadata(&equality_delete_path).unwrap().len();

        let fresh_loader = CachingDeleteFileLoader::new(file_io, 10, Runtime::current());
        let delete_filter = tokio::time::timeout(
            Duration::from_secs(5),
            fresh_loader.load_deletes(
                std::slice::from_ref(&equality_delete_task),
                table_schema.clone(),
            ),
        )
        .await
        .expect("equality delete retry hung")
        .expect("delete loader task ended without a result")
        .expect("equality delete retry failed");

        let predicate = tokio::time::timeout(
            Duration::from_secs(5),
            delete_filter
                .build_equality_delete_predicate(&make_data_file_task(equality_delete_task)),
        )
        .await
        .expect("equality delete predicate build hung")
        .expect("equality delete predicate build failed");
        assert!(predicate.is_some());
    }

    /// Create a simple field with metadata.
    fn simple_field(name: &str, ty: DataType, nullable: bool, value: &str) -> Field {
        Field::new(name, ty, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            value.to_string(),
        )]))
    }

    fn setup_write_equality_delete_file_1(table_location: &str) -> String {
        let col_y_vals = vec![1, 2];
        let col_y = Arc::new(Int64Array::from(col_y_vals)) as ArrayRef;

        let col_z_vals = vec![Some(100), None];
        let col_z = Arc::new(Int64Array::from(col_z_vals)) as ArrayRef;

        let col_a_vals = vec![Some("HELP"), None];
        let col_a = Arc::new(StringArray::from(col_a_vals)) as ArrayRef;

        let col_s = Arc::new(StructArray::from(vec![
            (
                Arc::new(simple_field("sa", DataType::Int32, false, "6")),
                Arc::new(Int32Array::from(vec![4, 5])) as ArrayRef,
            ),
            (
                Arc::new(simple_field("sb", DataType::Utf8, true, "7")),
                Arc::new(StringArray::from(vec![Some("x"), None])) as ArrayRef,
            ),
        ]));

        let col_b_vals = vec![Some(&b"binary_data"[..]), None];
        let col_b = Arc::new(BinaryArray::from(col_b_vals)) as ArrayRef;

        let equality_delete_schema = {
            let struct_field = DataType::Struct(Fields::from(vec![
                simple_field("sa", DataType::Int32, false, "6"),
                simple_field("sb", DataType::Utf8, true, "7"),
            ]));

            let fields = vec![
                Field::new("y", DataType::Int64, true).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "2".to_string(),
                )])),
                Field::new("z", DataType::Int64, true).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "3".to_string(),
                )])),
                Field::new("a", DataType::Utf8, true).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "4".to_string(),
                )])),
                simple_field("s", struct_field, false, "5"),
                simple_field("b", DataType::Binary, true, "8"),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let equality_deletes_to_write = RecordBatch::try_new(equality_delete_schema.clone(), vec![
            col_y, col_z, col_a, col_s, col_b,
        ])
        .unwrap();

        let path = format!("{}/equality-deletes-1.parquet", &table_location);

        let file = File::create(&path).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(
            file,
            equality_deletes_to_write.schema(),
            Some(props.clone()),
        )
        .unwrap();

        writer
            .write(&equality_deletes_to_write)
            .expect("Writing batch");

        // writer must be closed to write footer
        writer.close().unwrap();

        path
    }

    #[tokio::test]
    async fn test_caching_delete_file_loader_load_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::new_with_fs();

        let delete_file_loader =
            CachingDeleteFileLoader::new(file_io.clone(), 10, Runtime::current());

        let file_scan_tasks = setup(table_location);

        let delete_filter = delete_file_loader
            .load_deletes(&file_scan_tasks[0].deletes, file_scan_tasks[0].schema_ref())
            .await
            .unwrap()
            .unwrap();

        let result = delete_filter
            .get_delete_vector(&file_scan_tasks[0])
            .unwrap();

        // union of pos dels from pos del file 1 and 2, ie
        // [0, 1, 3, 5, 6, 8, 1022, 1023] | [0, 1, 3, 5, 20, 21, 22, 23]
        // = [0, 1, 3, 5, 6, 8, 20, 21, 22, 23, 1022, 1023]
        assert_eq!(result.lock().unwrap().len(), 12);

        let result = delete_filter.get_delete_vector(&file_scan_tasks[1]);
        assert!(result.is_none()); // no pos dels for file 3
    }

    #[tokio::test]
    async fn test_loads_distinct_deletion_vectors_from_shared_puffin() {
        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("shared.puffin");
        let file_path = file_path.to_string_lossy().into_owned();
        let file_io = FileIO::new_with_fs();
        let output_file = file_io.new_output(&file_path).unwrap();
        let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false)
            .await
            .unwrap();

        for (data_file, positions) in [("data-a.parquet", [1, 3]), ("data-b.parquet", [2, 8])] {
            let mut delete_vector = DeleteVector::default();
            for position in positions {
                delete_vector.insert(position);
            }
            let properties = HashMap::from([
                (
                    DELETION_VECTOR_PROPERTY_CARDINALITY.to_string(),
                    positions.len().to_string(),
                ),
                (
                    DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE.to_string(),
                    data_file.to_string(),
                ),
            ]);
            writer
                .add(
                    delete_vector.to_puffin_blob(properties).unwrap(),
                    CompressionCodec::None,
                )
                .await
                .unwrap();
        }

        let result = writer.close_with_metadata().await.unwrap();
        let tasks = result
            .blobs_metadata
            .iter()
            .zip(["data-a.parquet", "data-b.parquet"])
            .map(|(blob, data_file)| {
                FileScanTaskDeleteFile::builder()
                    .with_file_path(file_path.clone())
                    .with_file_size_in_bytes(result.file_size_in_bytes)
                    .with_file_type(DataContentType::PositionDeletes)
                    .with_partition_spec_id(0)
                    .with_file_format(DataFileFormat::Puffin)
                    .with_referenced_data_file(Some(data_file.to_string()))
                    .with_content_offset(Some(blob.offset() as i64))
                    .with_content_size_in_bytes(Some(blob.length() as i64))
                    .build()
            })
            .collect::<Vec<_>>();

        let loader = CachingDeleteFileLoader::new(file_io, 10, Runtime::current());
        let schema = Arc::new(Schema::builder().with_schema_id(1).build().unwrap());
        let filter = loader.load_deletes(&tasks, schema).await.unwrap().unwrap();

        let first = filter.get_delete_vector_for_path("data-a.parquet").unwrap();
        let second = filter.get_delete_vector_for_path("data-b.parquet").unwrap();
        assert_eq!(first.lock().unwrap().iter().collect::<Vec<_>>(), vec![1, 3]);
        assert_eq!(second.lock().unwrap().iter().collect::<Vec<_>>(), vec![
            2, 8
        ]);
    }

    #[tokio::test]
    async fn test_parse_positional_deletes_rejects_negative_positions() {
        let schema = crate::arrow::delete_filter::tests::create_pos_del_schema();
        let file_path_col = Arc::new(StringArray::from_iter_values(vec!["data.parquet"]));
        let pos_col = Arc::new(Int64Array::from_iter_values(vec![-1i64]));
        let batch = RecordBatch::try_new(schema, vec![file_path_col, pos_col]).unwrap();
        let stream = futures::stream::iter(vec![Ok(batch)]).boxed();

        let err = CachingDeleteFileLoader::parse_positional_deletes_record_batch_stream(stream)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("negative position"));
    }

    /// Verifies that evolve_schema on partial-schema equality deletes works correctly
    /// when only equality_ids columns are evolved, not all table columns.
    ///
    /// Per the [Iceberg spec](https://iceberg.apache.org/spec/#equality-delete-files),
    /// equality delete files can contain only a subset of columns.
    #[tokio::test]
    async fn test_partial_schema_equality_deletes_evolve_succeeds() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();

        // Create table schema with REQUIRED fields
        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "data", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Write equality delete file with PARTIAL schema (only 'data' column)
        let delete_file_path = {
            let data_vals = vec!["a", "d", "g"];
            let data_col = Arc::new(StringArray::from(data_vals)) as ArrayRef;

            let delete_schema = Arc::new(arrow_schema::Schema::new(vec![simple_field(
                "data",
                DataType::Utf8,
                false,
                "2", // field ID
            )]));

            let delete_batch = RecordBatch::try_new(delete_schema.clone(), vec![data_col]).unwrap();

            let path = format!("{}/partial-eq-deletes.parquet", &table_location);
            let file = File::create(&path).unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let mut writer =
                ArrowWriter::try_new(file, delete_batch.schema(), Some(props)).unwrap();
            writer.write(&delete_batch).expect("Writing batch");
            writer.close().unwrap();
            path
        };

        let file_io = FileIO::new_with_fs();
        let basic_delete_file_loader =
            BasicDeleteFileLoader::new(file_io.clone(), ScanMetrics::new());

        let batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(
                &delete_file_path,
                std::fs::metadata(&delete_file_path).unwrap().len(),
                None,
            )
            .await
            .unwrap();

        // Only evolve the equality_ids columns (field 2), not all table columns
        let equality_ids = vec![2];
        let evolved_stream =
            BasicDeleteFileLoader::evolve_schema(batch_stream, table_schema, &equality_ids)
                .await
                .unwrap();

        let result = evolved_stream.try_collect::<Vec<_>>().await;

        assert!(
            result.is_ok(),
            "Expected success when evolving only equality_ids columns, got error: {:?}",
            result.err()
        );

        let batches = result.unwrap();
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 1); // Only 'data' column

        // Verify the actual values are preserved after schema evolution
        let data_col = batch.column(0).as_string::<i32>();
        assert_eq!(data_col.value(0), "a");
        assert_eq!(data_col.value(1), "d");
        assert_eq!(data_col.value(2), "g");
    }

    /// Test loading a FileScanTask with BOTH positional and equality deletes.
    /// Verifies the fix for the inverted condition that caused "Missing predicate for equality delete file" errors.
    #[tokio::test]
    async fn test_load_deletes_with_mixed_types() {
        use crate::scan::FileScanTask;
        use crate::spec::{DataFileFormat, Schema};

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::new_with_fs();

        // Create the data file schema
        let data_file_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(2, "y", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(3, "z", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Write positional delete file
        let positional_delete_schema = crate::arrow::delete_filter::tests::create_pos_del_schema();
        let file_path_values =
            vec![format!("{}/data-1.parquet", table_location.to_str().unwrap()); 4];
        let file_path_col = Arc::new(StringArray::from_iter_values(&file_path_values));
        let pos_col = Arc::new(Int64Array::from_iter_values(vec![0i64, 1, 2, 3]));

        let positional_deletes_to_write =
            RecordBatch::try_new(positional_delete_schema.clone(), vec![
                file_path_col,
                pos_col,
            ])
            .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let pos_del_path = format!("{}/pos-del-mixed.parquet", table_location.to_str().unwrap());
        let file = File::create(&pos_del_path).unwrap();
        let mut writer = ArrowWriter::try_new(
            file,
            positional_deletes_to_write.schema(),
            Some(props.clone()),
        )
        .unwrap();
        writer.write(&positional_deletes_to_write).unwrap();
        writer.close().unwrap();

        // Write equality delete file
        let eq_delete_path = setup_write_equality_delete_file_1(table_location.to_str().unwrap());

        // Create FileScanTask with BOTH positional and equality deletes
        let pos_del = FileScanTaskDeleteFile::builder()
            .with_file_path(pos_del_path.clone())
            .with_file_size_in_bytes(std::fs::metadata(&pos_del_path).unwrap().len())
            .with_file_type(DataContentType::PositionDeletes)
            .with_partition_spec_id(0)
            .build();

        let eq_del = FileScanTaskDeleteFile::builder()
            .with_file_path(eq_delete_path.clone())
            .with_file_size_in_bytes(std::fs::metadata(&eq_delete_path).unwrap().len())
            .with_file_type(DataContentType::EqualityDeletes)
            .with_partition_spec_id(0)
            .with_equality_ids(Some(vec![2, 3])) // Only use field IDs that exist in both schemas
            .build();

        let file_scan_task = FileScanTask::builder()
            .with_file_size_in_bytes(0)
            .with_start(0)
            .with_length(0)
            .with_data_file_path(format!(
                "{}/data-1.parquet",
                table_location.to_str().unwrap()
            ))
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(data_file_schema.clone())
            .with_project_field_ids(vec![2, 3])
            .with_deletes(vec![pos_del, eq_del])
            .with_case_sensitive(false)
            .build();

        // Load the deletes - should handle both types without error
        let delete_file_loader =
            CachingDeleteFileLoader::new(file_io.clone(), 10, Runtime::current());
        let delete_filter = delete_file_loader
            .load_deletes(&file_scan_task.deletes, file_scan_task.schema_ref())
            .await
            .unwrap()
            .unwrap();

        // Verify both delete types can be processed together
        let result = delete_filter
            .build_equality_delete_predicate(&file_scan_task)
            .await;
        assert!(
            result.is_ok(),
            "Failed to build equality delete predicate: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_large_equality_delete_batch_stack_overflow() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();
        let file_io = FileIO::new_with_fs();

        // Create a large batch of equality deletes
        let num_rows = 20_000;
        let col_y_vals: Vec<i64> = (0..num_rows).collect();
        let col_y = Arc::new(Int64Array::from(col_y_vals)) as ArrayRef;

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("y", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        let record_batch = RecordBatch::try_new(schema.clone(), vec![col_y]).unwrap();

        // Write to file
        let path = format!("{}/large-eq-deletes.parquet", &table_location);
        let file = File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&record_batch).unwrap();
        writer.close().unwrap();

        let basic_delete_file_loader =
            BasicDeleteFileLoader::new(file_io.clone(), ScanMetrics::new());
        let record_batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(&path, std::fs::metadata(&path).unwrap().len(), None)
            .await
            .expect("could not get batch stream");

        let eq_ids = HashSet::from_iter(vec![2]);

        let result = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            record_batch_stream,
            eq_ids,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_caching_delete_file_loader_caches_results() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::new_with_fs();

        let delete_file_loader =
            CachingDeleteFileLoader::new(file_io.clone(), 10, Runtime::current());

        let file_scan_tasks = setup(table_location);

        // Load deletes for the first time
        let delete_filter_1 = delete_file_loader
            .load_deletes(&file_scan_tasks[0].deletes, file_scan_tasks[0].schema_ref())
            .await
            .unwrap()
            .unwrap();

        // Load deletes for the second time (same task/files)
        let delete_filter_2 = delete_file_loader
            .load_deletes(&file_scan_tasks[0].deletes, file_scan_tasks[0].schema_ref())
            .await
            .unwrap()
            .unwrap();

        let dv1 = delete_filter_1
            .get_delete_vector(&file_scan_tasks[0])
            .unwrap();
        let dv2 = delete_filter_2
            .get_delete_vector(&file_scan_tasks[0])
            .unwrap();

        // Verify that the delete vectors point to the same memory location,
        // confirming that the second load reused the result from the first.
        assert!(Arc::ptr_eq(&dv1, &dv2));
    }
}
