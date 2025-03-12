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

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::vec;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::Expr;
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::Predicate;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;

use super::expr_to_predicate::convert_filters_to_predicate;
use super::scan::get_column_names;
use crate::to_datafusion_error;

#[derive(Debug)]
pub(crate) struct IcebergFileTaskScan {
    file_scan_tasks: Vec<FileScanTask>,
    plan_properties: PlanProperties,
    projection: Option<Vec<String>>,
    predicates: Option<Predicate>,
    table: Table,
}

impl IcebergFileTaskScan {
    pub(crate) fn new(
        file_scan_tasks: Vec<FileScanTask>,
        schema: ArrowSchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        table: Table,
    ) -> Self {
        let output_schema = match projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };
        let plan_properties = Self::compute_properties(output_schema.clone());
        let projection = get_column_names(schema.clone(), projection);
        let predicates = convert_filters_to_predicate(filters);

        Self {
            file_scan_tasks,
            plan_properties,
            projection,
            predicates,
            table,
        }
    }

    /// Computes [`PlanProperties`] used in query optimization.
    fn compute_properties(schema: ArrowSchemaRef) -> PlanProperties {
        // TODO:
        // This is more or less a placeholder, to be replaced
        // once we support output-partitioning
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl ExecutionPlan for IcebergFileTaskScan {
    fn name(&self) -> &str {
        "IcebergFileTaskScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<(dyn ExecutionPlan + 'static)>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let fut = get_batch_stream(self.table.clone(), self.file_scan_tasks.clone());
        let stream = futures::stream::once(fut).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

async fn get_batch_stream(
    table: Table,
    file_scan_tasks: Vec<FileScanTask>,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let arrow_reader_builder = ArrowReaderBuilder::new(table.file_io().clone());
    let task_stream = futures::stream::iter(file_scan_tasks.into_iter().map(Ok)).boxed();
    let stream = arrow_reader_builder
        .build()
        .read(task_stream)
        .await
        .map_err(to_datafusion_error)?
        .map_err(to_datafusion_error);
    Ok(Box::pin(stream))
}

impl DisplayAs for IcebergFileTaskScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "IcebergTableScan projection:[{}] predicate:[{}]",
            self.projection
                .clone()
                .map_or(String::new(), |v| v.join(",")),
            self.predicates
                .clone()
                .map_or(String::from(""), |p| format!("{}", p))
        )
    }
}
