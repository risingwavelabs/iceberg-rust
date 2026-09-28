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

pub(crate) fn is_truthy(value: &str) -> bool {
    ["true", "t", "1", "on"].contains(&value.to_lowercase().as_str())
}

/// Convert an opendal error into an iceberg error.
pub(crate) fn from_opendal_error(e: opendal::Error) -> iceberg::Error {
    let kind = if e.kind() == opendal::ErrorKind::RangeNotSatisfied {
        iceberg::ErrorKind::DataInvalid
    } else {
        iceberg::ErrorKind::Unexpected
    };
    iceberg::Error::new(kind, "Failure in doing io operation").with_source(e)
}

/// Errors from signed HTTP requests may include the SAS query in their context.
pub(crate) fn credential_io_error(e: opendal::Error, redact: bool) -> iceberg::Error {
    if !redact {
        return from_opendal_error(e);
    }
    iceberg::Error::new(
        iceberg::ErrorKind::Unexpected,
        "Credentialed storage operation failed",
    )
    .with_context("storage_error_kind", e.kind().to_string())
    .with_retryable(e.is_temporary())
}
