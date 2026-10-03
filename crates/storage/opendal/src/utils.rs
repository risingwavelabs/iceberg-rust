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
    let mut error = iceberg::Error::new(
        iceberg::ErrorKind::Unexpected,
        "Credentialed storage operation failed",
    )
    .with_context("storage_error_kind", e.kind().to_string())
    .with_retryable(e.is_temporary());

    // Only our typed, already-sanitized provider failure can cross the source
    // boundary. Arbitrary provider messages and nested sources may hold secrets.
    #[cfg(any(feature = "opendal-s3", feature = "opendal-azdls"))]
    {
        use std::error::Error as _;

        let mut source = e.source();
        let mut signing = false;
        for _ in 0..16 {
            let Some(cause) = source else {
                break;
            };
            if let Some(failure) = cause.downcast_ref::<credential_errors::CredentialFailure>() {
                return credential_errors::with_failure(error, *failure);
            }
            signing |= cause.downcast_ref::<reqsign_core::Error>().is_some();
            source = cause.source();
        }
        if signing {
            if let Some(failure) = credential_errors::current_failure() {
                return credential_errors::with_failure(error, failure);
            }
            return error.with_context("failure_stage", "signing");
        }
    }

    error = error.with_context("failure_stage", "storage");
    // OpenDAL 0.59 has no structured context accessor. Read only known context
    // lines from its Debug representation and emit numeric/allowlisted values.
    // Never propagate raw URIs, response headers, messages or response bodies.
    let rendered = format!("{e:?}");
    if let Some((_, context)) = rendered.split_once("\nContext:\n") {
        let context = context.split("\nSource:").next().unwrap_or_default();
        for line in context.lines().map(str::trim) {
            if let Some(response) = line.strip_prefix("response: Parts { status: ")
                && let Some(status) = response.split(',').next().and_then(http_status)
            {
                error = error.with_context("status", status.to_string());
            }
            if let Some(operation) = line.strip_prefix("service_operation: ")
                && matches!(
                    operation,
                    "GetObject"
                        | "HeadObject"
                        | "PutObject"
                        | "DeleteObject"
                        | "DeleteObjects"
                        | "ListObjectsV2"
                        | "CreateMultipartUpload"
                        | "UploadPart"
                        | "CompleteMultipartUpload"
                        | "AbortMultipartUpload"
                )
            {
                error = error.with_context("service_operation", operation);
            }
        }
    }
    // Service error codes are useful, but their messages may echo credentials.
    for code in [
        "AccessDenied",
        "ExpiredToken",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "SlowDown",
        "NoSuchKey",
        "NoSuchBucket",
        "AuthenticationFailed",
        "AuthorizationPermissionMismatch",
        "AuthorizationFailure",
    ] {
        if e.message().contains(&format!("code: \"{code}\"")) {
            error = error.with_context("service_error_code", code);
            break;
        }
    }
    error
}

/// Scope diagnostics to one asynchronous I/O operation, including open handles.
///
/// reqsign's provider chain discards provider errors. A task-local carries only
/// sanitized fields to the resulting signing error, without mixing concurrent
/// requests or retaining any credential material.
pub(crate) async fn credential_io<T>(
    future: impl Future<Output = opendal::Result<T>>,
    redact: bool,
) -> iceberg::Result<T> {
    #[cfg(any(feature = "opendal-s3", feature = "opendal-azdls"))]
    if redact {
        return credential_errors::scope(async {
            future.await.map_err(|e| credential_io_error(e, true))
        })
        .await;
    }
    future.await.map_err(|e| credential_io_error(e, redact))
}

fn http_status(value: &str) -> Option<u16> {
    value
        .parse()
        .ok()
        .filter(|status| (100..600).contains(status))
}

#[cfg(any(feature = "opendal-s3", feature = "opendal-azdls"))]
pub(crate) use credential_errors::{
    clear_credential_failure, credential_discovery_error, credential_provider_error,
};

#[cfg(any(feature = "opendal-s3", feature = "opendal-azdls"))]
mod credential_errors {
    use std::cell::RefCell;
    use std::fmt;

    tokio::task_local! {
        static FAILURE: RefCell<Option<CredentialFailure>>;
    }

    pub(super) async fn scope<T>(future: impl Future<Output = T>) -> T {
        FAILURE.scope(RefCell::new(None), future).await
    }

    pub(super) fn current_failure() -> Option<CredentialFailure> {
        FAILURE.try_with(|failure| *failure.borrow()).ok().flatten()
    }

    pub(crate) fn clear_credential_failure() {
        let _ = FAILURE.try_with(|failure| *failure.borrow_mut() = None);
    }

    pub(super) fn with_failure(
        mut error: iceberg::Error,
        failure: CredentialFailure,
    ) -> iceberg::Error {
        error = error
            .with_context("failure_stage", "credential")
            .with_context("credential_error_kind", failure.kind.to_string())
            .with_context("credential_error", failure.reason)
            .with_retryable(failure.retryable);
        if let Some(status) = failure.status {
            error = error.with_context("status", status.to_string());
        }
        error.with_source(failure)
    }

    #[derive(Clone, Copy, Debug)]
    pub(super) struct CredentialFailure {
        pub(super) kind: iceberg::ErrorKind,
        pub(super) status: Option<u16>,
        pub(super) reason: &'static str,
        pub(super) retryable: bool,
    }

    impl fmt::Display for CredentialFailure {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Storage credential provider failed: {}", self.reason)?;
            if let Some(status) = self.status {
                write!(f, " (HTTP {status})")?;
            }
            Ok(())
        }
    }

    impl std::error::Error for CredentialFailure {}

    fn failure_from_error(error: &iceberg::Error) -> CredentialFailure {
        let status = error
            .context()
            .iter()
            .find(|(key, _)| *key == "status")
            .and_then(|(_, value)| super::http_status(value));
        let reason = error
            .context()
            .iter()
            .find(|(key, _)| *key == "credential_error")
            .and_then(|(_, value)| match value.as_str() {
                "refresh_rejected" => Some("refresh_rejected"),
                "refresh_failed" => Some("refresh_failed"),
                "refresh_timeout" => Some("refresh_timeout"),
                "refresh_backoff" => Some("refresh_backoff"),
                "revoked" => Some("revoked"),
                _ => None,
            })
            .unwrap_or("provider_failed");
        CredentialFailure {
            kind: error.kind(),
            status,
            reason,
            retryable: error.retryable(),
        }
    }

    pub(crate) fn credential_discovery_error(error: iceberg::Error) -> iceberg::Error {
        let failure = failure_from_error(&error);
        with_failure(
            iceberg::Error::new(error.kind(), "Unable to obtain storage credentials"),
            failure,
        )
    }

    pub(crate) fn credential_provider_error(error: iceberg::Error) -> reqsign_core::Error {
        let failure = failure_from_error(&error);
        let _ = FAILURE.try_with(|slot| *slot.borrow_mut() = Some(failure));
        reqsign_core::Error::credential_invalid("Unable to obtain storage credentials")
            .set_retryable(error.retryable())
            .with_source(failure)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_diagnostics_keep_status_and_code_without_raw_context_or_sources() {
        let raw = opendal::Error::new(
            opendal::ErrorKind::PermissionDenied,
            r#"S3Error { code: "AccessDenied", message: "secret-body" }"#,
        )
        .with_context("uri", "https://account/path?sig=secret-query")
        .with_context("service_operation", "GetObject")
        .with_context(
            "response",
            r#"Parts { status: 403, version: HTTP/1.1, headers: {"authorization": "secret-header"} }"#,
        )
        .set_source(std::io::Error::other("secret-source"));
        let error = credential_io_error(raw, true);
        let diagnostic = format!("{error} {error:?} {error:#?}");
        assert!(diagnostic.contains("failure_stage: storage"));
        assert!(diagnostic.contains("status: 403"));
        assert!(diagnostic.contains("GetObject"));
        assert!(diagnostic.contains("AccessDenied"));
        assert!(!diagnostic.contains("secret-"));
    }

    #[cfg(any(feature = "opendal-s3", feature = "opendal-azdls"))]
    #[test]
    fn provider_diagnostics_preserve_only_typed_safe_fields() {
        let provider = iceberg::Error::new(iceberg::ErrorKind::Unexpected, "secret-message")
            .with_context("status", "503")
            .with_context("credential_error", "refresh_rejected")
            .with_context("uri", "https://catalog/path?token=secret-token")
            .with_source(std::io::Error::other("secret-source"))
            .with_retryable(true);
        let signed = credential_provider_error(provider);
        assert!(!format!("{signed:?}").contains("secret-"));
        let raw = opendal::Error::new(opendal::ErrorKind::Unexpected, "secret-opendal")
            .set_source(signed);
        let error = credential_io_error(raw, true);
        let diagnostic = format!("{error} {error:?} {error:#?}");
        assert!(diagnostic.contains("failure_stage: credential"));
        assert!(diagnostic.contains("refresh_rejected"));
        assert!(diagnostic.contains("status: 503"));
        assert!(error.retryable());
        assert!(!diagnostic.contains("secret-"));
    }

    #[cfg(any(feature = "opendal-s3", feature = "opendal-azdls"))]
    #[test]
    fn custom_provider_cannot_inject_unchecked_diagnostic_values() {
        let provider = iceberg::Error::new(iceberg::ErrorKind::Unexpected, "secret-message")
            .with_context("status", "secret-status")
            .with_context("credential_error", "secret-category");
        let raw = opendal::Error::new(opendal::ErrorKind::Unexpected, "secret-opendal")
            .set_source(credential_provider_error(provider));
        let diagnostic = format!("{:?}", credential_io_error(raw, true));
        assert!(diagnostic.contains("provider_failed"));
        assert!(!diagnostic.contains("secret-"));
    }

    #[cfg(any(feature = "opendal-s3", feature = "opendal-azdls"))]
    #[tokio::test]
    async fn discarded_provider_errors_remain_isolated_between_concurrent_operations() {
        let mut tasks = tokio::task::JoinSet::new();
        for status in [401, 403, 429, 500, 503] {
            tasks.spawn(async move {
                let error = credential_io::<()>(
                    async {
                        let _ = credential_provider_error(
                            iceberg::Error::new(iceberg::ErrorKind::Unexpected, "secret-message")
                                .with_context("status", status.to_string())
                                .with_context("credential_error", "refresh_rejected"),
                        );
                        tokio::task::yield_now().await;
                        // reqsign's chain has discarded the original source.
                        Err(opendal::Error::new(
                            opendal::ErrorKind::Unexpected,
                            "signing http request",
                        )
                        .set_source(reqsign_core::Error::credential_invalid("No credential")))
                    },
                    true,
                )
                .await
                .unwrap_err();
                assert!(
                    error
                        .context()
                        .iter()
                        .any(|(key, value)| *key == "status" && value == &status.to_string())
                );
                assert!(format!("{error:?}").contains("failure_stage: credential"));
                assert!(!format!("{error:?}").contains("secret-message"));
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }
}
