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

//! This module provide `EqualityDeleteWriter`.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field};
use itertools::Itertools;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::arrow::record_batch_projector::RecordBatchProjector;
use crate::arrow::{arrow_schema_to_schema, schema_to_arrow_schema};
use crate::spec::{DataFile, SchemaRef, Struct};
use crate::writer::file_writer::{FileWriter, FileWriterBuilder};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Builder for `EqualityDeleteWriter`.
#[derive(Clone)]
pub struct EqualityDeleteFileWriterBuilder<B: FileWriterBuilder> {
    inner: B,
    // Field ids used to determine row equality in equality delete files.
    equality_ids: Vec<i32>,
    // Projector used to project the data chunk into specific fields.
    projector: RecordBatchProjector,
    partition_value: Struct,
}

impl<B: FileWriterBuilder> EqualityDeleteFileWriterBuilder<B> {
    /// Create a new `EqualityDeleteFileWriterBuilder` using a `FileWriterBuilder`.
    pub fn new(
        inner: B,
        equality_ids: Vec<i32>,
        original_schema: SchemaRef,
        partition_value: Option<Struct>,
    ) -> Result<Self> {
        let original_arrow_schema = Arc::new(schema_to_arrow_schema(&original_schema)?);
        let projector = RecordBatchProjector::new(
            &original_arrow_schema,
            &equality_ids,
            // The following rule comes from https://iceberg.apache.org/spec/#identifier-field-ids
            // and https://iceberg.apache.org/spec/#equality-delete-files
            // - The identifier field ids must be used for primitive types.
            // - The identifier field ids must not be used for floating point types or nullable fields.
            |field| {
                // Only primitive type is allowed to be used for identifier field ids
                if field.data_type().is_nested()
                    || matches!(
                        field.data_type(),
                        DataType::Float16 | DataType::Float32 | DataType::Float64
                    )
                {
                    return Ok(None);
                }
                Ok(Some(
                    field
                        .metadata()
                        .get(PARQUET_FIELD_ID_META_KEY)
                        .ok_or_else(|| {
                            Error::new(ErrorKind::Unexpected, "Field metadata is missing.")
                        })?
                        .parse::<i64>()
                        .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?,
                ))
            },
            |_field: &Field| true,
        )?;
        Ok(Self {
            inner,
            equality_ids,
            projector,
            partition_value: partition_value.unwrap_or(Struct::empty()),
        })
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriterBuilder for EqualityDeleteFileWriterBuilder<B> {
    type R = EqualityDeleteFileWriter<B>;

    async fn build(self) -> Result<Self::R> {
        let schema = Arc::new(arrow_schema_to_schema(
            self.projector.projected_schema_ref(),
        )?);
        Ok(EqualityDeleteFileWriter {
            inner_writer: Some(self.inner.clone().build(schema).await?),
            projector: self.projector,
            equality_ids: self.equality_ids,
            partition_value: self.partition_value,
        })
    }
}

/// Writer used to write equality delete files.
pub struct EqualityDeleteFileWriter<B: FileWriterBuilder> {
    inner_writer: Option<B::R>,
    projector: RecordBatchProjector,
    equality_ids: Vec<i32>,
    partition_value: Struct,
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriter for EqualityDeleteFileWriter<B> {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let batch = self.projector.project_bacth(batch)?;
        if let Some(writer) = self.inner_writer.as_mut() {
            writer.write(&batch).await
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Equality delete inner writer has been closed.",
            ))
        }
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        if let Some(writer) = self.inner_writer.take() {
            Ok(writer
                .close()
                .await?
                .into_iter()
                .map(|mut res| {
                    res.content(crate::spec::DataContentType::EqualityDeletes);
                    res.equality_ids(self.equality_ids.iter().copied().collect_vec());
                    res.partition(self.partition_value.clone());
                    res.build().expect("msg")
                })
                .collect_vec())
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Equality delete inner writer has been closed.",
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::types::Int32Type;
    use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StructArray};
    use arrow_schema::DataType;
    use arrow_select::concat::concat_batches;
    use itertools::Itertools;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::arrow::schema_to_arrow_schema;
    use crate::io::{FileIO, FileIOBuilder};
    use crate::spec::{
        DataFile, DataFileFormat, ListType, MapType, NestedField, PrimitiveType, Schema,
        StructType, Type,
    };
    use crate::writer::base_writer::equality_delete_writer::EqualityDeleteFileWriterBuilder;
    use crate::writer::file_writer::location_generator::test::MockLocationGenerator;
    use crate::writer::file_writer::location_generator::DefaultFileNameGenerator;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};

    async fn check_parquet_data_file_with_equality_delete_write(
        file_io: &FileIO,
        data_file: &DataFile,
        batch: &RecordBatch,
    ) {
        assert_eq!(data_file.file_format, DataFileFormat::Parquet);

        // read the written file
        let input_file = file_io.new_input(data_file.file_path.clone()).unwrap();
        // read the written file
        let input_content = input_file.read().await.unwrap();
        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(input_content.clone()).unwrap();
        let metadata = reader_builder.metadata().clone();

        // check data
        let reader = reader_builder.build().unwrap();
        let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
        let res = concat_batches(&batch.schema(), &batches).unwrap();
        assert_eq!(*batch, res);

        // check metadata
        let expect_column_num = batch.num_columns();

        assert_eq!(
            data_file.record_count,
            metadata
                .row_groups()
                .iter()
                .map(|group| group.num_rows())
                .sum::<i64>() as u64
        );

        assert_eq!(data_file.file_size_in_bytes, input_content.len() as u64);

        assert_eq!(data_file.column_sizes.len(), expect_column_num);

        for (index, id) in data_file.column_sizes().keys().sorted().enumerate() {
            metadata
                .row_groups()
                .iter()
                .map(|group| group.columns())
                .for_each(|column| {
                    assert_eq!(
                        *data_file.column_sizes.get(id).unwrap() as i64,
                        column.get(index).unwrap().compressed_size()
                    );
                });
        }

        assert_eq!(data_file.value_counts.len(), expect_column_num);
        data_file.value_counts.iter().for_each(|(_, &v)| {
            let expect = metadata
                .row_groups()
                .iter()
                .map(|group| group.num_rows())
                .sum::<i64>() as u64;
            assert_eq!(v, expect);
        });

        for (index, id) in data_file.null_value_counts().keys().enumerate() {
            let expect = batch.column(index).null_count() as u64;
            assert_eq!(*data_file.null_value_counts.get(id).unwrap(), expect);
        }

        assert_eq!(data_file.split_offsets.len(), metadata.num_row_groups());
        data_file
            .split_offsets
            .iter()
            .enumerate()
            .for_each(|(i, &v)| {
                let expect = metadata.row_groups()[i].file_offset().unwrap();
                assert_eq!(v, expect);
            });
    }

    #[tokio::test]
    async fn test_equality_delete_writer() -> Result<(), anyhow::Error> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        // Int, Struct(Int), String, List(Int), Struct(Struct(Int))
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(0, "col0", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(
                    1,
                    "col1",
                    Type::Struct(StructType::new(vec![NestedField::required(
                        5,
                        "sub_col",
                        Type::Primitive(PrimitiveType::Int),
                    )
                    .into()])),
                )
                .into(),
                NestedField::required(2, "col2", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(
                    3,
                    "col3",
                    Type::List(ListType::new(
                        NestedField::required(6, "element", Type::Primitive(PrimitiveType::Int))
                            .into(),
                    )),
                )
                .into(),
                NestedField::required(
                    4,
                    "col4",
                    Type::Struct(StructType::new(vec![NestedField::required(
                        7,
                        "sub_col",
                        Type::Struct(StructType::new(vec![NestedField::required(
                            8,
                            "sub_sub_col",
                            Type::Primitive(PrimitiveType::Int),
                        )
                        .into()])),
                    )
                    .into()])),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let col0 = Arc::new(Int32Array::from_iter_values(vec![1; 1024])) as ArrayRef;
        let col1 = Arc::new(StructArray::new(
            if let DataType::Struct(fields) = arrow_schema.fields.get(1).unwrap().data_type() {
                fields.clone()
            } else {
                unreachable!()
            },
            vec![Arc::new(Int32Array::from_iter_values(vec![1; 1024]))],
            None,
        ));
        let col2 = Arc::new(arrow_array::StringArray::from_iter_values(vec![
            "test";
            1024
        ])) as ArrayRef;
        let col3 = Arc::new({
            let list_parts = arrow_array::ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
              Some(
                  vec![Some(1),]
              );
              1024
          ])
            .into_parts();
            arrow_array::ListArray::new(
                if let DataType::List(field) = arrow_schema.fields.get(3).unwrap().data_type() {
                    field.clone()
                } else {
                    unreachable!()
                },
                list_parts.1,
                list_parts.2,
                list_parts.3,
            )
        }) as ArrayRef;
        let col4 = Arc::new(StructArray::new(
            if let DataType::Struct(fields) = arrow_schema.fields.get(4).unwrap().data_type() {
                fields.clone()
            } else {
                unreachable!()
            },
            vec![Arc::new(StructArray::new(
                if let DataType::Struct(fields) = arrow_schema.fields.get(4).unwrap().data_type() {
                    if let DataType::Struct(fields) = fields.first().unwrap().data_type() {
                        fields.clone()
                    } else {
                        unreachable!()
                    }
                } else {
                    unreachable!()
                },
                vec![Arc::new(Int32Array::from_iter_values(vec![1; 1024]))],
                None,
            ))],
            None,
        ));
        let columns = vec![col0, col1, col2, col3, col4];
        let to_write = RecordBatch::try_new(arrow_schema.clone(), columns).unwrap();

        // prepare writer
        let pb = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut equality_delete_writer =
            EqualityDeleteFileWriterBuilder::new(pb, vec![0_i32, 8], Arc::new(schema), None)
                .unwrap()
                .build()
                .await?;
        let projector = equality_delete_writer.projector.clone();

        // write
        equality_delete_writer.write(to_write.clone()).await?;
        let res = equality_delete_writer.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res.into_iter().next().unwrap();

        // check
        let to_write_projected = projector.project_bacth(to_write)?;
        check_parquet_data_file_with_equality_delete_write(
            &file_io,
            &data_file,
            &to_write_projected,
        )
        .await;
        Ok(())
    }

    #[tokio::test]
    async fn test_equality_delete_unreachable_column() -> Result<(), anyhow::Error> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);
        let pb = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(0, "col0", Type::Primitive(PrimitiveType::Float)).into(),
                    NestedField::required(1, "col1", Type::Primitive(PrimitiveType::Double)).into(),
                    NestedField::optional(2, "col2", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(
                        3,
                        "col3",
                        Type::Struct(StructType::new(vec![NestedField::required(
                            4,
                            "sub_col",
                            Type::Primitive(PrimitiveType::Int),
                        )
                        .into()])),
                    )
                    .into(),
                    NestedField::optional(
                        5,
                        "col4",
                        Type::Struct(StructType::new(vec![NestedField::required(
                            6,
                            "sub_col2",
                            Type::Primitive(PrimitiveType::Int),
                        )
                        .into()])),
                    )
                    .into(),
                    NestedField::required(
                        7,
                        "col5",
                        Type::Map(MapType::new(
                            Arc::new(NestedField::required(
                                8,
                                "key",
                                Type::Primitive(PrimitiveType::String),
                            )),
                            Arc::new(NestedField::required(
                                9,
                                "value",
                                Type::Primitive(PrimitiveType::Int),
                            )),
                        )),
                    )
                    .into(),
                    NestedField::required(
                        10,
                        "col6",
                        Type::List(ListType::new(Arc::new(NestedField::required(
                            11,
                            "element",
                            Type::Primitive(PrimitiveType::Int),
                        )))),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        // Float and Double are not allowed to be used for equality delete
        assert!(
            EqualityDeleteFileWriterBuilder::new(pb.clone(), vec![0], schema.clone(), None)
                .is_err()
        );
        assert!(
            EqualityDeleteFileWriterBuilder::new(pb.clone(), vec![1], schema.clone(), None)
                .is_err()
        );
        // Struct is not allowed to be used for equality delete
        assert!(
            EqualityDeleteFileWriterBuilder::new(pb.clone(), vec![3], schema.clone(), None)
                .is_err()
        );
        // Nested field of struct is allowed to be used for equality delete
        assert!(
            EqualityDeleteFileWriterBuilder::new(pb.clone(), vec![4], schema.clone(), None).is_ok()
        );
        // Nested field of map is not allowed to be used for equality delete
        assert!(
            EqualityDeleteFileWriterBuilder::new(pb.clone(), vec![7], schema.clone(), None)
                .is_err()
        );
        assert!(
            EqualityDeleteFileWriterBuilder::new(pb.clone(), vec![8], schema.clone(), None)
                .is_err()
        );
        assert!(
            EqualityDeleteFileWriterBuilder::new(pb.clone(), vec![9], schema.clone(), None)
                .is_err()
        );
        // Nested field of list is not allowed to be used for equality delete
        assert!(
            EqualityDeleteFileWriterBuilder::new(pb.clone(), vec![10], schema.clone(), None)
                .is_err()
        );
        assert!(EqualityDeleteFileWriterBuilder::new(pb, vec![11], schema.clone(), None).is_err());

        Ok(())
    }
}
