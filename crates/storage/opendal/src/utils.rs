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

/// How storage errors are reported for an I/O operation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ErrorRedaction {
    /// Static credentials: report the OpenDAL error as is.
    #[default]
    Off,
    /// A credential provider is attached: report only allowlisted fields.
    On,
    /// Like `On`, but also report the service error code and, unless the code
    /// is about signing or credentials, the service error message.
    KeepServiceDetail,
}

impl ErrorRedaction {
    pub(crate) fn new(has_provider: bool, keep_service_detail: bool) -> Self {
        match (has_provider, keep_service_detail) {
            (false, _) => Self::Off,
            (true, false) => Self::On,
            (true, true) => Self::KeepServiceDetail,
        }
    }

    fn redacts(self) -> bool {
        self != Self::Off
    }
}

/// Service error codes whose messages can echo credentials, signatures or
/// request material.
const CREDENTIAL_SERVICE_ERROR_CODES: [&str; 8] = [
    "SignatureDoesNotMatch",
    "InvalidAccessKeyId",
    "AuthorizationHeaderMalformed",
    "InvalidToken",
    "InvalidSecurity",
    "ExpiredToken",
    "TokenRefreshRequired",
    "AuthenticationFailed",
];

const MAX_SERVICE_ERROR_MESSAGE_CHARS: usize = 256;

/// Reads the string value of `field: "..."` from an OpenDAL service error
/// message, such as `S3Error { code: "AccessDenied", message: "..." }`.
fn service_error_field(text: &str, field: &str) -> Option<String> {
    let start = text.find(&format!("{field}: \""))? + field.len() + 3;
    let mut value = String::new();
    let mut chars = text[start..].chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => return Some(value),
            '\\' => value.push(chars.next()?),
            c => value.push(c),
        }
    }
    None
}

fn service_error_code(e: &opendal::Error) -> Option<String> {
    service_error_field(e.message(), "code").filter(|code| {
        !code.is_empty()
            && code.len() <= 64
            && code.chars().all(|c| c.is_ascii_alphanumeric() || c == '.')
    })
}

fn service_error_message(e: &opendal::Error, code: &str) -> Option<String> {
    if CREDENTIAL_SERVICE_ERROR_CODES.contains(&code) {
        return None;
    }
    let message = service_error_field(e.message(), "message")?;
    if ["X-Amz-", "Signature=", "Credential=", "AWSAccessKeyId"]
        .iter()
        .any(|marker| message.contains(marker))
    {
        return None;
    }
    Some(
        message
            .chars()
            .take(MAX_SERVICE_ERROR_MESSAGE_CHARS)
            .collect(),
    )
}

/// Errors from signed HTTP requests may include the SAS query in their context.
pub(crate) fn credential_io_error(e: opendal::Error, redaction: ErrorRedaction) -> iceberg::Error {
    if !redaction.redacts() {
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
    #[cfg(any(
        feature = "opendal-s3",
        feature = "opendal-azdls",
        feature = "opendal-gcs"
    ))]
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
    if redaction == ErrorRedaction::KeepServiceDetail {
        if let Some(code) = service_error_code(&e) {
            if let Some(message) = service_error_message(&e, &code) {
                error = error.with_context("service_error_message", message);
            }
            error = error.with_context("service_error_code", code);
        }
        return error;
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
    redaction: ErrorRedaction,
) -> iceberg::Result<T> {
    #[cfg(any(
        feature = "opendal-s3",
        feature = "opendal-azdls",
        feature = "opendal-gcs"
    ))]
    if redaction.redacts() {
        return credential_errors::scope(async {
            future.await.map_err(|e| credential_io_error(e, redaction))
        })
        .await;
    }
    future.await.map_err(|e| credential_io_error(e, redaction))
}

fn http_status(value: &str) -> Option<u16> {
    value
        .parse()
        .ok()
        .filter(|status| (100..600).contains(status))
}

#[cfg(any(
    feature = "opendal-s3",
    feature = "opendal-azdls",
    feature = "opendal-gcs"
))]
pub(crate) use credential_errors::{
    clear_credential_failure, credential_discovery_error, credential_provider_error,
};

#[cfg(any(
    feature = "opendal-s3",
    feature = "opendal-azdls",
    feature = "opendal-gcs"
))]
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
        let error = credential_io_error(raw, ErrorRedaction::On);
        let diagnostic = format!("{error} {error:?} {error:#?}");
        assert!(diagnostic.contains("failure_stage: storage"));
        assert!(diagnostic.contains("status: 403"));
        assert!(diagnostic.contains("GetObject"));
        assert!(diagnostic.contains("AccessDenied"));
        assert!(!diagnostic.contains("secret-"));
    }

    fn s3_storage_error(code: &str, message: &str) -> opendal::Error {
        opendal::Error::new(
            opendal::ErrorKind::PermissionDenied,
            format!(r#"S3Error {{ code: "{code}", message: "{message}", request_id: "" }}"#),
        )
        .with_context("uri", "https://account/path?sig=secret-query")
        .with_context("service_operation", "GetObject")
        .with_context(
            "response",
            r#"Parts { status: 403, version: HTTP/1.1, headers: {"authorization": "secret-header"} }"#,
        )
        .set_source(std::io::Error::other("secret-source"))
    }

    #[test]
    fn service_detail_keeps_code_and_message_without_raw_context_or_sources() {
        let raw = s3_storage_error("AccessDenied", "This account is banned.");
        let error = credential_io_error(raw, ErrorRedaction::KeepServiceDetail);
        let diagnostic = format!("{error} {error:?} {error:#?}");
        assert!(diagnostic.contains("failure_stage: storage"));
        assert!(diagnostic.contains("status: 403"));
        assert!(diagnostic.contains("service_error_code: AccessDenied"));
        assert!(diagnostic.contains("service_error_message: This account is banned."));
        assert!(!diagnostic.contains("secret-"));
    }

    #[test]
    fn service_detail_keeps_codes_outside_the_default_allowlist() {
        let kept = credential_io_error(
            s3_storage_error("NotEntitled", "R2 is not enabled for this account."),
            ErrorRedaction::KeepServiceDetail,
        );
        assert!(format!("{kept:?}").contains("service_error_code: NotEntitled"));

        let default = credential_io_error(
            s3_storage_error("NotEntitled", "R2 is not enabled for this account."),
            ErrorRedaction::On,
        );
        let diagnostic = format!("{default:?}");
        assert!(!diagnostic.contains("NotEntitled"));
        assert!(!diagnostic.contains("not enabled"));
    }

    #[test]
    fn service_detail_drops_messages_of_credential_errors() {
        for code in CREDENTIAL_SERVICE_ERROR_CODES {
            let error = credential_io_error(
                s3_storage_error(code, "secret-string-to-sign"),
                ErrorRedaction::KeepServiceDetail,
            );
            let diagnostic = format!("{error:?}");
            assert!(diagnostic.contains(&format!("service_error_code: {code}")));
            assert!(!diagnostic.contains("secret-"), "{code}: {diagnostic}");
        }
    }

    #[test]
    fn service_detail_drops_messages_with_signing_material() {
        for message in [
            "secret X-Amz-Credential=AKIA",
            "secret Signature=abc",
            "secret Credential=abc",
            "secret AWSAccessKeyId",
        ] {
            let error = credential_io_error(
                s3_storage_error("AccessDenied", message),
                ErrorRedaction::KeepServiceDetail,
            );
            let diagnostic = format!("{error:?}");
            assert!(diagnostic.contains("service_error_code: AccessDenied"));
            assert!(!diagnostic.contains("secret"), "{diagnostic}");
        }
    }

    #[test]
    fn service_detail_rejects_malformed_codes_and_truncates_messages() {
        let error = credential_io_error(
            s3_storage_error("Access Denied?", "message"),
            ErrorRedaction::KeepServiceDetail,
        );
        assert!(!format!("{error:?}").contains("service_error"));

        let long = "x".repeat(MAX_SERVICE_ERROR_MESSAGE_CHARS + 10);
        let error = credential_io_error(
            s3_storage_error("AccessDenied", &long),
            ErrorRedaction::KeepServiceDetail,
        );
        let message = format!("{error:?}")
            .lines()
            .find_map(|line| line.trim().strip_prefix("service_error_message: "))
            .map(str::to_owned)
            .unwrap();
        assert_eq!(message.chars().count(), MAX_SERVICE_ERROR_MESSAGE_CHARS);
    }

    #[test]
    fn service_error_field_unescapes_quotes() {
        assert_eq!(
            service_error_field(r#"S3Error { code: "A", message: "say \"hi\"" }"#, "message")
                .as_deref(),
            Some(r#"say "hi""#)
        );
        assert_eq!(service_error_field(r#"code: "unterminated"#, "code"), None);
    }

    #[test]
    fn static_credentials_keep_the_opendal_error() {
        let error = credential_io_error(
            s3_storage_error("AccessDenied", "This account is banned."),
            ErrorRedaction::new(false, true),
        );
        assert!(format!("{error:?}").contains("This account is banned."));
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
        let error = credential_io_error(raw, ErrorRedaction::On);
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
        let diagnostic = format!("{:?}", credential_io_error(raw, ErrorRedaction::On));
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
                    ErrorRedaction::On,
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
