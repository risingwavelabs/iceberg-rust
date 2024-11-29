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

//! This module contains the fanout partition writer.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_row::OwnedRow;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use itertools::Itertools;

use crate::arrow::{schema_to_arrow_schema, RecordBatchPartitionSpliter};
use crate::spec::{BoundPartitionSpecRef, DataFile};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::Result;

/// The builder for `FanoutPartitionWriter`.
#[derive(Clone)]
pub struct FanoutPartitionWriterBuilder<B> {
    inner_builder: B,
    partition_specs: BoundPartitionSpecRef,
    arrow_schema: ArrowSchemaRef,
}

impl<B> FanoutPartitionWriterBuilder<B> {
    /// Create a new `FanoutPartitionWriterBuilder` with the default arrow schema.
    pub fn new(inner_builder: B, partition_specs: BoundPartitionSpecRef) -> Result<Self> {
        let arrow_schema = Arc::new(schema_to_arrow_schema(partition_specs.schema())?);
        Ok(Self::new_with_custom_schema(
            inner_builder,
            partition_specs,
            arrow_schema,
        ))
    }

    /// Create a new `FanoutPartitionWriterBuilder` with a custom arrow schema.
    /// This function is useful for the user who has the input with extral columns.
    pub fn new_with_custom_schema(
        inner_builder: B,
        partition_specs: BoundPartitionSpecRef,
        arrow_schema: ArrowSchemaRef,
    ) -> Self {
        Self {
            inner_builder,
            partition_specs,
            arrow_schema,
        }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for FanoutPartitionWriterBuilder<B> {
    type R = FanoutPartitionWriter<B>;

    async fn build(self) -> Result<Self::R> {
        let partition_splitter =
            RecordBatchPartitionSpliter::new(&self.arrow_schema, self.partition_specs)?;
        Ok(FanoutPartitionWriter {
            inner_writer_builder: self.inner_builder,
            partition_splitter,
            partition_writers: HashMap::new(),
        })
    }
}

/// The fanout partition writer.
/// It will split the input record batch by the partition specs, and write the splitted record batches to the inner writers.
pub struct FanoutPartitionWriter<B: IcebergWriterBuilder> {
    inner_writer_builder: B,
    partition_splitter: RecordBatchPartitionSpliter,
    partition_writers: HashMap<OwnedRow, B::R>,
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriter for FanoutPartitionWriter<B> {
    async fn write(&mut self, input: RecordBatch) -> Result<()> {
        let splits = self.partition_splitter.split(&input)?;

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
        let partition_values = self.partition_splitter.convert_row(partition_rows)?;

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
