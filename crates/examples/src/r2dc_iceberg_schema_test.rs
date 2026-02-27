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

//! Standalone script to register an Iceberg table with R2 Data Catalog (R2DC)
//! and write sample data through the full Iceberg writer pipeline.
//!
//! This is used to test that `iceberg.schema` metadata in Parquet footers
//! is correctly written and readable by Snowflake.
//!
//! Usage:
//!   # On the `add-iceberg.schema` branch (with iceberg.schema in footer):
//!   TABLE_NAME=with_iceberg_schema cargo run -p iceberg-examples --example r2dc-iceberg-schema-test
//!
//!   # On main / pre-feature commit (without iceberg.schema):
//!   TABLE_NAME=without_iceberg_schema cargo run -p iceberg-examples --example r2dc-iceberg-schema-test

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use parquet::file::properties::WriterProperties;

// R2 Data Catalog connection details (bobsled-testing staging account)
const R2DC_URI: &str =
    "https://catalog.cloudflarestorage.com/cc72dab93be2f1e3147d13b2a8aa1f26/bobsled-testing";
const R2DC_WAREHOUSE: &str = "cc72dab93be2f1e3147d13b2a8aa1f26_bobsled-testing";
const R2DC_TOKEN: &str = "cSLXSRdAQiq9HR3mTX4OwIXersjSSvGCJGrYsY7J";

const NAMESPACE: &str = "iceberg_schema_test";
const DEFAULT_TABLE_NAME: &str = "with_iceberg_schema";

#[tokio::main]
async fn main() {
    let table_name = std::env::var("TABLE_NAME").unwrap_or_else(|_| DEFAULT_TABLE_NAME.to_string());

    println!("=== R2DC Iceberg Schema Test ===");
    println!("Table name: {table_name}");
    println!("Namespace:  {NAMESPACE}");
    println!("Catalog:    {R2DC_URI}");
    println!();

    // Step 1: Connect to R2DC REST catalog
    println!("[1/7] Connecting to R2DC REST catalog...");
    let catalog = RestCatalogBuilder::default()
        .load(
            "r2dc",
            HashMap::from([
                (REST_CATALOG_PROP_URI.to_string(), R2DC_URI.to_string()),
                (
                    REST_CATALOG_PROP_WAREHOUSE.to_string(),
                    R2DC_WAREHOUSE.to_string(),
                ),
                ("token".to_string(), R2DC_TOKEN.to_string()),
            ]),
        )
        .await
        .expect("Failed to connect to R2DC catalog");
    println!("  Connected.");

    // Step 2: Create namespace (ignore if already exists)
    println!("[2/7] Creating namespace '{NAMESPACE}'...");
    let namespace = NamespaceIdent::from_vec(vec![NAMESPACE.to_string()]).unwrap();
    match catalog.create_namespace(&namespace, HashMap::new()).await {
        Ok(_) => println!("  Namespace created."),
        Err(e) if e.kind() == iceberg::ErrorKind::NamespaceAlreadyExists => {
            println!("  Namespace already exists, continuing.")
        }
        Err(e) => {
            // R2DC may return a different error code for "already exists"
            let msg = format!("{e:?}");
            if msg.contains("already exists") || msg.contains("AlreadyExists") {
                println!("  Namespace already exists, continuing.");
            } else {
                panic!("Failed to create namespace: {e}");
            }
        }
    }

    // Step 3: Define Iceberg schema
    println!("[3/7] Defining schema (id: Long, name: String, score: Double)...");
    let iceberg_schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "score", Type::Primitive(PrimitiveType::Double)).into(),
        ])
        .with_schema_id(0)
        .with_identifier_field_ids(vec![1])
        .build()
        .unwrap();
    println!("  Schema defined.");

    // Step 4: Create table (drop if it already exists)
    println!("[4/7] Creating table '{table_name}'...");
    let table_ident = TableIdent::new(namespace.clone(), table_name.clone());

    if catalog.table_exists(&table_ident).await.unwrap_or(false) {
        println!("  Table already exists, dropping...");
        catalog
            .drop_table(&table_ident)
            .await
            .expect("Failed to drop existing table");
        println!("  Dropped.");
    }

    let table_creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(iceberg_schema.clone())
        .build();

    let table = catalog
        .create_table(&namespace, table_creation)
        .await
        .expect("Failed to create table");
    println!(
        "  Table created. Location: {:?}",
        table.metadata().location()
    );

    // Step 5: Build writer pipeline
    println!("[5/7] Building writer pipeline...");
    let schema_ref = table.metadata().current_schema().clone();

    let location_generator =
        DefaultLocationGenerator::new(table.metadata().clone()).expect("location generator");
    let file_name_generator =
        DefaultFileNameGenerator::new("data".to_string(), None, DataFileFormat::Parquet);

    let parquet_writer_builder = ParquetWriterBuilder::new(WriterProperties::default(), schema_ref);

    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    let mut writer = data_file_writer_builder
        .build(None)
        .await
        .expect("Failed to build data file writer");
    println!("  Writer pipeline ready.");

    // Step 6: Write sample data
    println!("[6/7] Writing 5 rows...");
    let ids = Int64Array::from(vec![1i64, 2, 3, 4, 5]);
    let names = StringArray::from(vec![
        Some("Alice"),
        Some("Bob"),
        Some("Charlie"),
        Some("Diana"),
        Some("Eve"),
    ]);
    let scores = Float64Array::from(vec![
        Some(95.5),
        Some(87.3),
        Some(92.1),
        Some(88.7),
        Some(91.0),
    ]);

    let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), "1".to_string())]),
        ),
        arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), "2".to_string())]),
        ),
        arrow_schema::Field::new("score", arrow_schema::DataType::Float64, true).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), "3".to_string())]),
        ),
    ]));

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![Arc::new(ids), Arc::new(names), Arc::new(scores)],
    )
    .expect("Failed to create RecordBatch");

    writer.write(batch).await.expect("Failed to write batch");
    let data_files = writer.close().await.expect("Failed to close writer");
    println!(
        "  Wrote {} data file(s). Total records: {}",
        data_files.len(),
        data_files.iter().map(|f| f.record_count()).sum::<u64>()
    );
    for (i, df) in data_files.iter().enumerate() {
        println!("  File {}: {}", i, df.file_path());
    }

    // Step 7: Commit via Transaction
    println!("[7/7] Committing transaction...");
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_files);
    let tx = append_action.apply(tx).expect("Failed to apply append");
    let _table = tx
        .commit(&catalog)
        .await
        .expect("Failed to commit transaction");
    println!("  Transaction committed successfully!");

    println!();
    println!("=== Done! ===");
    println!("Table '{NAMESPACE}.{table_name}' is now registered in R2DC with data.");
    println!("You can now query it from Snowflake via the catalog integration.");
}
