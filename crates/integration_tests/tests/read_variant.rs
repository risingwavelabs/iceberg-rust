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

//! End-to-end coverage for a Spark-written Iceberg Variant column.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::DataType;
use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;
use futures::TryStreamExt;
use iceberg::spec::Type;
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_integration_tests::get_test_fixture;
use iceberg_storage_opendal::OpenDalStorageFactory;
use parquet::variant::variant_to_json;

async fn load_variant_table() -> Table {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::s3()))
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_variant_column"]).unwrap())
        .await
        .unwrap()
}

fn assert_variant_column(batch: &RecordBatch) {
    assert_eq!(
        batch
            .schema()
            .field_with_name("v")
            .unwrap()
            .metadata()
            .get(EXTENSION_TYPE_NAME_KEY)
            .map(String::as_str),
        Some("arrow.parquet.variant")
    );

    let column = batch.column_by_name("v").expect("variant column");
    let DataType::Struct(fields) = column.data_type() else {
        panic!(
            "expected Variant Struct storage, got {}",
            column.data_type()
        );
    };
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].name(), "metadata");
    assert_eq!(fields[0].data_type(), &DataType::Binary);
    assert_eq!(fields[1].name(), "value");
    assert_eq!(fields[1].data_type(), &DataType::Binary);
}

#[tokio::test]
async fn test_variant_schema_is_parsed() {
    let table = load_variant_table().await;
    let variant_field = table
        .metadata()
        .current_schema()
        .field_by_name("v")
        .expect("variant field");

    assert!(matches!(
        variant_field.field_type.as_ref(),
        Type::Variant(_)
    ));
}

#[tokio::test]
async fn test_variant_arrow_schema() {
    // Full scan without projection: exercises the non-projected read path.
    let table = load_variant_table().await;
    let batches: Vec<_> = table
        .scan()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert!(!batches.is_empty());
    assert_variant_column(&batches[0]);
}

#[tokio::test]
async fn test_spark_variant_scan_and_projection() {
    let table = load_variant_table().await;
    let batches: Vec<_> = table
        .scan()
        .select(["id", "v"])
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert!(!batches.is_empty());
    assert_variant_column(&batches[0]);
    assert!(batches[0].column_by_name("id").is_some());
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);

    let json_values = batches
        .iter()
        .flat_map(|batch| {
            let json = variant_to_json(batch.column_by_name("v").unwrap()).unwrap();
            json.iter()
                .map(|value| value.unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(json_values.len(), 3);
    assert!(json_values.iter().any(|value| matches!(
        value.as_str(),
        r#"{"a":1,"b":"hello"}"# | r#"{"b":"hello","a":1}"#
    )));
    assert!(json_values.iter().any(|value| value == "[1,2,3]"));
    assert!(json_values.iter().any(|value| value == "42"));
}
