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

//! This module contains the equality delta writer.

use std::collections::HashMap;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{Int32Array, RecordBatch};
use arrow_ord::partition::partition;
use arrow_row::{OwnedRow, RowConverter, Rows, SortField};
use arrow_select::filter::filter_record_batch;
use itertools::Itertools;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::arrow::record_batch_projector::RecordBatchProjector;
use crate::arrow::schema_to_arrow_schema;
use crate::spec::DataFile;
use crate::writer::base_writer::sort_position_delete_writer::PositionDeleteInput;
use crate::writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Insert operation.
pub const INSERT_OP: i32 = 1;
/// Delete operation.
pub const DELETE_OP: i32 = 2;

/// Builder for `EqualityDeltaWriter`.
#[derive(Clone)]
pub struct EqualityDeltaWriterBuilder<DB, PDB, EDB> {
    data_writer_builder: DB,
    position_delete_writer_builder: PDB,
    equality_delete_writer_builder: EDB,
    unique_column_ids: Vec<i32>,
}

impl<DB, PDB, EDB> EqualityDeltaWriterBuilder<DB, PDB, EDB> {
    /// Create a new `EqualityDeltaWriterBuilder`.
    pub fn new(
        data_writer_builder: DB,
        position_delete_writer_builder: PDB,
        equality_delete_writer_builder: EDB,
        unique_column_ids: Vec<i32>,
    ) -> Self {
        Self {
            data_writer_builder,
            position_delete_writer_builder,
            equality_delete_writer_builder,
            unique_column_ids,
        }
    }
}

#[async_trait::async_trait]
impl<DB, PDB, EDB> IcebergWriterBuilder for EqualityDeltaWriterBuilder<DB, PDB, EDB>
where
    DB: IcebergWriterBuilder,
    PDB: IcebergWriterBuilder<PositionDeleteInput>,
    EDB: IcebergWriterBuilder,
    DB::R: CurrentFileStatus,
{
    type R = EqualityDeltaWriter<DB::R, PDB::R, EDB::R>;

    async fn build(self) -> Result<Self::R> {
        Self::R::try_new(
            self.data_writer_builder.build().await?,
            self.position_delete_writer_builder.build().await?,
            self.equality_delete_writer_builder.build().await?,
            self.unique_column_ids,
        )
    }
}

/// Equality delta writer.
pub struct EqualityDeltaWriter<D, PD, ED> {
    data_writer: D,
    position_delete_writer: PD,
    equality_delete_writer: ED,
    projector: RecordBatchProjector,
    inserted_row: HashMap<OwnedRow, PositionDeleteInput>,
    row_converter: RowConverter,
}

impl<D, PD, ED> EqualityDeltaWriter<D, PD, ED>
where
    D: IcebergWriter + CurrentFileStatus,
    PD: IcebergWriter<PositionDeleteInput>,
    ED: IcebergWriter,
{
    pub(crate) fn try_new(
        data_writer: D,
        position_delete_writer: PD,
        equality_delete_writer: ED,
        unique_column_ids: Vec<i32>,
    ) -> Result<Self> {
        let projector = RecordBatchProjector::new(
            &schema_to_arrow_schema(&data_writer.current_schema())?,
            &unique_column_ids,
            |field| {
                if !field.data_type().is_primitive() {
                    return Ok(None);
                }
                field
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .map(|s| {
                        s.parse::<i64>()
                            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))
                    })
                    .transpose()
            },
            |_| true,
        )?;
        let row_converter = RowConverter::new(
            projector
                .projected_schema_ref()
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )?;
        Ok(Self {
            data_writer,
            position_delete_writer,
            equality_delete_writer,
            projector,
            inserted_row: HashMap::new(),
            row_converter,
        })
    }
    /// Write the batch.
    /// 1. If a row with the same unique column is not written, then insert it.
    /// 2. If a row with the same unique column is written, then delete the previous row and insert the new row.
    async fn insert(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = self.extract_unique_column(&batch)?;
        let current_file_path = self.data_writer.current_file_path();
        let current_file_offset = self.data_writer.current_row_num();
        for (idx, row) in rows.iter().enumerate() {
            let previous_input = self.inserted_row.insert(row.owned(), PositionDeleteInput {
                path: current_file_path.clone(),
                offset: (current_file_offset + idx) as i64,
            });
            if let Some(previous_input) = previous_input {
                self.position_delete_writer.write(previous_input).await?;
            }
        }

        self.data_writer.write(batch).await?;

        Ok(())
    }

    async fn delete(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = self.extract_unique_column(&batch)?;
        let mut delete_row = BooleanBuilder::new();
        for row in rows.iter() {
            if let Some(previous_input) = self.inserted_row.remove(&row.owned()) {
                self.position_delete_writer.write(previous_input).await?;
                delete_row.append_value(false);
            } else {
                delete_row.append_value(true);
            }
        }
        let delete_batch = filter_record_batch(&batch, &delete_row.finish()).map_err(|err| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to filter record batch, error: {}", err),
            )
        })?;
        self.equality_delete_writer.write(delete_batch).await?;
        Ok(())
    }

    fn extract_unique_column(&mut self, batch: &RecordBatch) -> Result<Rows> {
        self.row_converter
            .convert_columns(&self.projector.project_column(batch.columns())?)
            .map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Failed to convert columns, error: {}", err),
                )
            })
    }
}

#[async_trait::async_trait]
impl<D, PD, ED> IcebergWriter for EqualityDeltaWriter<D, PD, ED>
where
    D: IcebergWriter + CurrentFileStatus,
    PD: IcebergWriter<PositionDeleteInput>,
    ED: IcebergWriter,
{
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // check the last column is int32 array.
        let ops = batch
            .column(batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or(Error::new(ErrorKind::DataInvalid, ""))?;

        // partition the ops.
        let partitions =
            partition(&[batch.column(batch.num_columns() - 1).clone()]).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Failed to partition ops, error: {}", err),
                )
            })?;
        for range in partitions.ranges() {
            let batch = batch
                .project(&(0..batch.num_columns() - 1).collect_vec())
                .unwrap()
                .slice(range.start, range.end - range.start);
            match ops.value(range.start) {
                // Insert
                INSERT_OP => self.insert(batch).await?,
                // Delete
                DELETE_OP => self.delete(batch).await?,
                op => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid ops: {op}"),
                    ))
                }
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let data_files = self.data_writer.close().await?;
        let position_delete_files = self.position_delete_writer.close().await?;
        let equality_delete_files = self.equality_delete_writer.close().await?;
        Ok(data_files
            .into_iter()
            .chain(position_delete_files)
            .chain(equality_delete_files)
            .collect())
    }
}
