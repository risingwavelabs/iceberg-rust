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

//! This module contains the precompute partition writer.

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use arrow_array::{RecordBatch, StructArray};
use arrow_row::{OwnedRow, RowConverter, SortField};
use arrow_schema::DataType;
use itertools::Itertools;

use crate::arrow::{convert_row_to_struct, split_with_partition, type_to_arrow_type};
use crate::spec::{BoundPartitionSpecRef, DataFile, Type};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// The builder for precompute partition writer.
#[derive(Clone)]
pub struct PrecomputePartitionWriterBuilder<B: IcebergWriterBuilder> {
    inner_writer_builder: B,
    partition_spec: BoundPartitionSpecRef,
}

impl<B: IcebergWriterBuilder> PrecomputePartitionWriterBuilder<B> {
    /// Create a new precompute partition writer builder.
    pub fn new(inner_writer_builder: B, partition_spec: BoundPartitionSpecRef) -> Self {
        Self {
            inner_writer_builder,
            partition_spec,
        }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder<(StructArray, RecordBatch)>
    for PrecomputePartitionWriterBuilder<B>
{
    type R = PrecomputePartitionWriter<B>;

    async fn build(self) -> Result<Self::R> {
        let arrow_type =
            type_to_arrow_type(&Type::Struct(self.partition_spec.partition_type().clone()))?;
        let DataType::Struct(fields) = &arrow_type else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The partition type is not a struct",
            ));
        };
        let partition_row_converter = RowConverter::new(
            fields
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;
        Ok(PrecomputePartitionWriter {
            inner_writer_builder: self.inner_writer_builder,
            partition_row_converter,
            partition_spec: self.partition_spec,
            partition_writers: HashMap::new(),
        })
    }
}

/// The precompute partition writer.
pub struct PrecomputePartitionWriter<B: IcebergWriterBuilder> {
    inner_writer_builder: B,
    partition_writers: HashMap<OwnedRow, B::R>,
    partition_row_converter: RowConverter,
    partition_spec: BoundPartitionSpecRef,
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriter<(StructArray, RecordBatch)>
    for PrecomputePartitionWriter<B>
{
    async fn write(&mut self, input: (StructArray, RecordBatch)) -> Result<()> {
        let splits =
            split_with_partition(&self.partition_row_converter, input.0.columns(), &input.1)?;

        for (partition, record_batch) in splits {
            match self.partition_writers.entry(partition) {
                Entry::Occupied(entry) => {
                    entry.into_mut().write(record_batch).await?;
                }
                Entry::Vacant(entry) => {
                    let writer = entry.insert(self.inner_writer_builder.clone().build().await?);
                    writer.write(record_batch).await?;
                }
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let (partition_rows, writers): (Vec<_>, Vec<_>) = self.partition_writers.drain().unzip();
        let partition_values = convert_row_to_struct(
            &self.partition_row_converter,
            self.partition_spec.partition_type(),
            partition_rows,
        )?;

        let mut result = Vec::new();
        for (partition_value, mut writer) in partition_values.into_iter().zip_eq(writers) {
            let mut data_files = writer.close().await?;
            for data_file in data_files.iter_mut() {
                data_file.rewrite_partition(partition_value.clone());
            }
            result.append(&mut data_files);
        }

        Ok(result)
    }
}
