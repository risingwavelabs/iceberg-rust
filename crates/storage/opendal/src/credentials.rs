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

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use iceberg::io::{
    CredentialProvider, StorageCredential, StorageCredentialKind, StorageCredentialProvider,
};
use iceberg::{Error, ErrorKind, Result};
use reqsign_core::{Context, ProvideCredential};

#[cfg(test)]
#[derive(Debug)]
struct FixedCredentialProvider(StorageCredential);

#[cfg(test)]
#[async_trait]
impl StorageCredentialProvider for FixedCredentialProvider {
    async fn load_credential(&self, _: &str) -> Result<StorageCredential> {
        Ok(self.0.clone())
    }
}

fn timestamp(time: SystemTime) -> reqsign_core::Result<reqsign_core::time::Timestamp> {
    let millis = time
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .ok_or_else(|| {
            reqsign_core::Error::credential_invalid("Invalid storage credential expiry")
        })?;
    reqsign_core::time::Timestamp::from_millisecond(millis)
}

/// Bind to a file, not the prefix selected when the operator was constructed:
/// a refreshed credential set can partition the locations differently.
#[derive(Clone)]
pub(crate) struct VendedCredentialSource {
    pub(crate) provider: CredentialProvider,
    pub(crate) location: String,
}

impl std::fmt::Debug for VendedCredentialSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VendedCredentialSource")
            .finish_non_exhaustive()
    }
}

impl VendedCredentialSource {
    async fn load(&self, minimum_validity: Duration) -> Result<StorageCredential> {
        crate::utils::clear_credential_failure();
        let credential = self
            .provider
            .0
            .load_credential_with_minimum_validity(&self.location, minimum_validity)
            .await?;
        if !credential.covers(&self.location) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Storage credential does not cover signing location",
            ));
        }
        Ok(credential)
    }
}

/// A short-lived signer shared only by paths matched to one credential scope.
///
/// Re-select every location before signing, including on retries. Looking up
/// only the prefix could miss a newly introduced, more-specific child scope.
pub(crate) struct BatchCredential {
    provider: CredentialProvider,
    prefix: String,
    locations: Vec<String>,
}

impl std::fmt::Debug for BatchCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchCredential").finish_non_exhaustive()
    }
}

impl BatchCredential {
    pub(crate) fn provider(
        provider: CredentialProvider,
        prefix: String,
        locations: Vec<String>,
    ) -> CredentialProvider {
        CredentialProvider(Arc::new(Self {
            provider,
            prefix,
            locations,
        }))
    }
}

#[async_trait]
impl StorageCredentialProvider for BatchCredential {
    async fn load_credential(&self, location: &str) -> Result<StorageCredential> {
        self.load_credential_with_minimum_validity(location, Duration::ZERO)
            .await
    }

    async fn load_credential_with_minimum_validity(
        &self,
        _: &str,
        minimum_validity: Duration,
    ) -> Result<StorageCredential> {
        let mut selected: Option<StorageCredential> = None;
        for location in &self.locations {
            let credential = self
                .provider
                .0
                .load_credential_with_minimum_validity(location, minimum_validity)
                .await?;
            if credential.prefix() != Some(self.prefix.as_str()) || !credential.covers(location) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Storage credential scope changed during bulk deletion",
                ));
            }
            if let Some(previous) = selected.take() {
                if previous.kind() != credential.kind() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Storage credentials differ within a bulk deletion scope",
                    ));
                }
                let expiry = match (previous.expires_at(), credential.expires_at()) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                };
                selected = Some(match expiry {
                    Some(expiry) => previous.with_expiration(expiry),
                    None => previous,
                });
            } else {
                selected = Some(credential);
            }
        }
        let selected =
            selected.ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Empty credential batch"))?;
        // Stay below reqsign's cache freshness windows (Azure: 20s, AWS:
        // 120s), but above AWS's 10s signing headroom. Every signing attempt
        // must re-select the batch, even if a custom provider uses a long TTL.
        let lease = if matches!(selected.kind(), StorageCredentialKind::Azdls(_)) {
            Duration::from_secs(5)
        } else {
            Duration::from_secs(30)
        };
        let now = SystemTime::now();
        let deadline = now + lease;
        let expiry = selected
            .expires_at()
            .map_or(deadline, |expiry| expiry.min(deadline));
        if expiry
            .duration_since(now)
            .map_or(true, |remaining| remaining <= minimum_validity)
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Bulk deletion credential does not meet required validity",
            ));
        }
        Ok(selected.with_expiration(expiry))
    }
}

#[cfg(feature = "opendal-azdls")]
#[derive(Debug)]
pub(crate) struct VendedAzdlsCredentialProvider(pub(crate) VendedCredentialSource);

#[cfg(feature = "opendal-azdls")]
impl ProvideCredential for VendedAzdlsCredentialProvider {
    type Credential = reqsign_azure_storage::Credential;

    async fn provide_credential(
        &self,
        _: &Context,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        let credential = self
            .0
            .load(Duration::ZERO)
            .await
            .map_err(crate::utils::credential_provider_error)?;
        let StorageCredentialKind::Azdls(material) = credential.kind() else {
            return Err(reqsign_core::Error::credential_invalid(
                "Expected ADLS credential",
            ));
        };
        if material.sas_token().trim_start_matches('?').is_empty() {
            return Err(reqsign_core::Error::credential_invalid(
                "Missing ADLS SAS credential",
            ));
        }
        Ok(Some(match credential.expires_at() {
            Some(expiry) => reqsign_azure_storage::Credential::with_sas_token_expires_at(
                material.sas_token(),
                timestamp(expiry)?,
            ),
            None => reqsign_azure_storage::Credential::with_sas_token(material.sas_token()),
        }))
    }
}

#[cfg(feature = "opendal-s3")]
#[derive(Debug)]
pub(crate) struct VendedS3CredentialProvider(pub(crate) VendedCredentialSource);

#[cfg(feature = "opendal-s3")]
impl ProvideCredential for VendedS3CredentialProvider {
    type Credential = reqsign_aws_v4::Credential;

    async fn provide_credential(
        &self,
        _: &Context,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        let credential = self
            .0
            // reqsign requires 10s for AWS signing; allow another 5s for
            // credential selection and request construction.
            .load(Duration::from_secs(15))
            .await
            .map_err(crate::utils::credential_provider_error)?;
        let StorageCredentialKind::S3(material) = credential.kind() else {
            return Err(reqsign_core::Error::credential_invalid(
                "Expected S3 credential",
            ));
        };
        if material.access_key_id().is_empty() || material.secret_access_key().is_empty() {
            return Err(reqsign_core::Error::credential_invalid(
                "Incomplete S3 credential",
            ));
        }
        Ok(Some(reqsign_aws_v4::Credential {
            access_key_id: material.access_key_id().to_string(),
            secret_access_key: material.secret_access_key().to_string(),
            session_token: material.session_token().map(str::to_string),
            expires_in: credential.expires_at().map(timestamp).transpose()?,
        }))
    }
}

#[cfg(all(test, feature = "opendal-azdls"))]
mod adls_batch_tests {
    use std::time::{Duration, SystemTime};

    use futures::StreamExt;
    use iceberg::io::{ADLS_ENDPOINT, AzdlsCredential, FileIOBuilder, StorageFactory};
    use mockito::{Matcher, Server};

    use super::*;
    use crate::{OpenDalResolvingStorageFactory, OpenDalStorageFactory};

    #[derive(Debug)]
    struct Provider;

    #[tokio::test]
    async fn adls_typed_credentials_accept_no_expiry_and_reject_wrong_backend() {
        for factory in [
            Arc::new(OpenDalStorageFactory::azdls()) as Arc<dyn StorageFactory>,
            Arc::new(OpenDalResolvingStorageFactory::new()),
        ] {
            let mut server = Server::new_async().await;
            let request = server
                .mock("GET", "/core.windows.net/one/file")
                .match_query(Matcher::UrlEncoded("sig".into(), "fixed".into()))
                .with_body("data")
                .expect(1)
                .create_async()
                .await;
            for kind in [
                StorageCredentialKind::Azdls(AzdlsCredential::new("sig=fixed")),
                StorageCredentialKind::S3(iceberg::io::S3Credential::new("wrong", "wrong", None)),
            ] {
                let is_adls = matches!(kind, StorageCredentialKind::Azdls(_));
                let io = FileIOBuilder::new(factory.clone())
                    .with_prop(ADLS_ENDPOINT, format!("{}/core.windows.net", server.url()))
                    .with_prop("io.max-retries", "0")
                    .with_credential_provider(Arc::new(FixedCredentialProvider(
                        StorageCredential::new(kind),
                    )))
                    .build();
                let result = io
                    .new_input("abfs://one@account.dfs.core.windows.net/file")
                    .unwrap()
                    .read()
                    .await;
                assert_eq!(result.is_ok(), is_adls);
            }
            request.assert_async().await;
        }
    }

    #[async_trait]
    impl StorageCredentialProvider for Provider {
        async fn load_credential(&self, location: &str) -> Result<StorageCredential> {
            let mut root = url::Url::parse(location)?;
            let filesystem = root.username().to_string();
            root.set_path("/");
            Ok(
                StorageCredential::new(StorageCredentialKind::Azdls(AzdlsCredential::new(
                    format!("sig={filesystem}"),
                )))
                .with_prefix(root.to_string())
                .with_expiration(SystemTime::now() + Duration::from_secs(30)),
            )
        }
    }

    #[tokio::test]
    async fn adls_storage_errors_keep_status_without_sas_or_response_contents() {
        let mut server = Server::new_async().await;
        let request = server
            .mock("GET", "/core.windows.net/one/file")
            .match_query(Matcher::UrlEncoded("sig".into(), "one".into()))
            .with_status(403)
            .with_header("x-ms-error-code", "AuthorizationPermissionMismatch")
            .with_header("x-secret-header", "secret-header")
            .with_body("<Error><Code>AuthorizationPermissionMismatch</Code><Message>secret-body</Message></Error>")
            .expect(1)
            .create_async()
            .await;
        let io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::azdls()))
            .with_prop(ADLS_ENDPOINT, format!("{}/core.windows.net", server.url()))
            .with_prop("io.max-retries", "0")
            .with_credential_provider(Arc::new(Provider))
            .build();
        let error = io
            .new_input("abfs://one@account.dfs.core.windows.net/file")
            .unwrap()
            .read()
            .await
            .unwrap_err();
        let diagnostic = format!("{error} {error:?} {error:#?}");
        assert!(
            diagnostic.contains("failure_stage: storage"),
            "{diagnostic}"
        );
        assert!(diagnostic.contains("status: 403"), "{diagnostic}");
        assert!(
            diagnostic.contains("AuthorizationPermissionMismatch"),
            "{diagnostic}"
        );
        assert!(!diagnostic.contains("sig="));
        assert!(!diagnostic.contains("secret-"));
        request.assert_async().await;
    }

    #[tokio::test]
    async fn scoped_adls_deletes_isolate_filesystems_without_per_file_cache_entries() {
        let direct = Arc::new(OpenDalStorageFactory::azdls());
        let factories: [Arc<dyn StorageFactory>; 2] = [
            direct.clone(),
            Arc::new(OpenDalResolvingStorageFactory::new()),
        ];
        for factory in factories {
            let mut server = Server::new_async().await;
            let mut requests = Vec::new();
            let mut paths = Vec::new();
            for (filesystem, file) in [("one", "a"), ("one", "b"), ("two", "a"), ("two", "b")] {
                requests.push(
                    server
                        .mock(
                            "DELETE",
                            format!("/core.windows.net/{filesystem}/{file}").as_str(),
                        )
                        .match_query(Matcher::UrlEncoded("sig".into(), filesystem.into()))
                        .with_status(200)
                        .expect(1)
                        .create_async()
                        .await,
                );
                paths.push(format!(
                    "abfs://{filesystem}@account.dfs.core.windows.net/{file}"
                ));
            }
            let io = FileIOBuilder::new(factory)
                .with_prop(ADLS_ENDPOINT, format!("{}/core.windows.net", server.url()))
                .with_prop("io.max-retries", "0")
                .with_credential_provider(Arc::new(Provider))
                .build();
            io.delete_stream(futures::stream::iter(paths).boxed())
                .await
                .unwrap();
            for request in requests {
                request.assert_async().await;
            }
        }
        assert_eq!(direct.operator_cache.len(), 0);
    }
}

#[cfg(all(test, feature = "opendal-s3"))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, SystemTime};

    use async_trait::async_trait;
    use futures::StreamExt;
    use iceberg::io::{
        CredentialProvider, FileIOBuilder, S3_ACCESS_KEY_ID, S3_ALLOW_ANONYMOUS, S3_ENDPOINT,
        S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY, StorageCredential,
        StorageCredentialProvider, StorageFactory,
    };
    use mockito::{Matcher, Server};
    use tokio::sync::Barrier;

    use crate::{OpenDalResolvingStorageFactory, OpenDalStorageFactory};

    fn s3_factories() -> [Arc<dyn StorageFactory>; 2] {
        [
            Arc::new(OpenDalStorageFactory::s3()),
            Arc::new(OpenDalResolvingStorageFactory::new()),
        ]
    }

    fn anonymous_s3_builder(factory: Arc<dyn StorageFactory>, endpoint: &str) -> FileIOBuilder {
        FileIOBuilder::new(factory)
            .with_prop(S3_ENDPOINT, endpoint)
            .with_prop(S3_REGION, "us-east-1")
            .with_prop(S3_PATH_STYLE_ACCESS, "true")
            .with_prop(S3_ALLOW_ANONYMOUS, "true")
            .with_prop("io.max-retries", "0")
    }

    #[tokio::test]
    async fn s3_typed_credentials_accept_no_expiry_and_reject_wrong_backend() {
        for factory in s3_factories() {
            let mut server = Server::new_async().await;
            let request = server
                .mock("GET", "/bucket/table/file")
                .match_header("authorization", Matcher::Regex("Credential=FIXED/".into()))
                .with_body("data")
                .expect(1)
                .create_async()
                .await;
            for kind in [
                iceberg::io::StorageCredentialKind::S3(iceberg::io::S3Credential::new(
                    "FIXED",
                    "dummy-secret",
                    None,
                )),
                iceberg::io::StorageCredentialKind::Azdls(iceberg::io::AzdlsCredential::new(
                    "sig=wrong",
                )),
            ] {
                let is_s3 = matches!(kind, iceberg::io::StorageCredentialKind::S3(_));
                let io = anonymous_s3_builder(factory.clone(), &server.url())
                    .with_credential_provider(Arc::new(super::FixedCredentialProvider(
                        StorageCredential::new(kind),
                    )))
                    .build();
                let result = io.new_input("s3://bucket/table/file").unwrap().read().await;
                assert_eq!(result.is_ok(), is_s3);
            }
            request.assert_async().await;
        }
    }

    #[tokio::test]
    async fn unsupported_paths_keep_static_authentication_for_reads_and_deletes() {
        #[derive(Debug)]
        struct UnsupportedProvider;
        #[async_trait]
        impl StorageCredentialProvider for UnsupportedProvider {
            fn supports_path(&self, _: &str) -> bool {
                false
            }
            async fn load_credential(&self, _: &str) -> iceberg::Result<StorageCredential> {
                panic!("unsupported paths must not load vended credentials");
            }
        }
        for factory in s3_factories() {
            let mut server = Server::new_async().await;
            let read = server
                .mock("GET", "/bucket/table/file")
                .match_header("authorization", Matcher::Regex("Credential=STATIC/".into()))
                .with_body("data")
                .expect(1)
                .create_async()
                .await;
            let delete = server
                .mock("POST", "/bucket/")
                .match_header("authorization", Matcher::Regex("Credential=STATIC/".into()))
                .match_query(Matcher::Any)
                .match_body(Matcher::Regex(r"^<Delete><Quiet>true</Quiet>(<Object><Key>table/(file|other)</Key></Object>){2}</Delete>$".into()))
                .with_body("<DeleteResult/>")
                .expect(1)
                .create_async()
                .await;
            let io = anonymous_s3_builder(factory, &server.url())
                .with_prop(S3_ALLOW_ANONYMOUS, "false")
                .with_prop(S3_ACCESS_KEY_ID, "STATIC")
                .with_prop(S3_SECRET_ACCESS_KEY, "dummy-static-secret")
                .with_credential_provider(Arc::new(UnsupportedProvider))
                .build();
            assert_eq!(
                io.new_input("s3://bucket/table/file")
                    .unwrap()
                    .read()
                    .await
                    .unwrap()
                    .as_ref(),
                b"data"
            );
            io.delete_stream(futures::stream::iter(vec![
                "s3://bucket/table/file".to_string(),
                "s3://bucket/table/other".to_string(),
            ]))
            .await
            .unwrap();
            read.assert_async().await;
            delete.assert_async().await;
        }
    }

    #[tokio::test]
    async fn batch_non_expiring_credentials_still_receive_a_short_signing_lease() {
        let credential = StorageCredential::new(iceberg::io::StorageCredentialKind::S3(
            iceberg::io::S3Credential::new("key", "dummy-secret", None),
        ))
        .with_prefix("s3://bucket/table/");
        let batch = super::BatchCredential::provider(
            CredentialProvider(Arc::new(super::FixedCredentialProvider(credential))),
            "s3://bucket/table/".into(),
            vec![
                "s3://bucket/table/one".into(),
                "s3://bucket/table/two".into(),
            ],
        );
        let start = SystemTime::now();
        let selected = batch.0.load_credential("s3://bucket/table/").await.unwrap();
        assert!(selected.expires_at().unwrap() >= start);
        assert!(selected.expires_at().unwrap() <= SystemTime::now() + Duration::from_secs(30));
    }

    #[derive(Debug)]
    struct Provider(AtomicUsize);

    #[async_trait]
    impl StorageCredentialProvider for Provider {
        async fn load_credential(&self, location: &str) -> iceberg::Result<StorageCredential> {
            assert_eq!(location, "s3://bucket/table/file");
            let generation = self.0.load(Ordering::SeqCst);
            Ok(
                StorageCredential::new(iceberg::io::StorageCredentialKind::S3(
                    iceberg::io::S3Credential::new(
                        format!("KEY{generation}"),
                        "dummy-secret",
                        Some(format!("SESSION{generation}")),
                    ),
                ))
                .with_expiration(SystemTime::now() + Duration::from_secs(30)),
            )
        }
    }

    #[derive(Debug)]
    struct RejectingProvider {
        incomplete: bool,
    }

    #[async_trait]
    impl StorageCredentialProvider for RejectingProvider {
        async fn load_credential(&self, _: &str) -> iceberg::Result<StorageCredential> {
            if self.incomplete {
                Ok(
                    StorageCredential::new(iceberg::io::StorageCredentialKind::S3(
                        iceberg::io::S3Credential::new("", "", None),
                    ))
                    .with_expiration(SystemTime::now() + Duration::from_secs(30)),
                )
            } else {
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    "No matching credential",
                ))
            }
        }
    }

    #[derive(Debug)]
    struct ScopedDeleteProvider {
        barrier: Barrier,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl StorageCredentialProvider for ScopedDeleteProvider {
        async fn load_credential(&self, location: &str) -> iceberg::Result<StorageCredential> {
            let key = match location {
                "s3://bucket/first/file" => "FIRST",
                "s3://bucket/second/file" => "SECOND",
                _ => panic!("unexpected credential location"),
            };
            // Scope discovery makes the first two calls. Synchronize the next
            // two, which come from the actual delete signers, so concurrent
            // discovery alone cannot hide serial HTTP deletion.
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if (2..4).contains(&call) {
                self.barrier.wait().await;
            }
            Ok(
                StorageCredential::new(iceberg::io::StorageCredentialKind::S3(
                    iceberg::io::S3Credential::new(key, "dummy-secret", None),
                ))
                .with_expiration(SystemTime::now() + Duration::from_secs(30)),
            )
        }
    }

    #[tokio::test]
    async fn credentialed_delete_stream_is_concurrent_and_path_bound() {
        for factory in s3_factories() {
            let mut server = Server::new_async().await;
            let first = server
                .mock("DELETE", "/bucket/first/file")
                .match_header(
                    "authorization",
                    Matcher::Regex("Credential=FIRST/".to_string()),
                )
                .with_status(204)
                .expect(1)
                .create_async()
                .await;
            let second = server
                .mock("DELETE", "/bucket/second/file")
                .match_header(
                    "authorization",
                    Matcher::Regex("Credential=SECOND/".to_string()),
                )
                .with_status(204)
                .expect(1)
                .create_async()
                .await;
            let provider = Arc::new(ScopedDeleteProvider {
                barrier: Barrier::new(2),
                calls: AtomicUsize::new(0),
            });
            let io = anonymous_s3_builder(factory, &server.url())
                .with_credential_provider(provider.clone())
                .build();
            let paths = ["s3://bucket/first/file", "s3://bucket/second/file"];
            tokio::time::timeout(
                Duration::from_secs(5),
                io.delete_stream(futures::stream::iter(paths.map(str::to_string)).boxed()),
            )
            .await
            .expect("credentialed deletes must make concurrent progress")
            .unwrap();
            assert!(provider.calls.load(Ordering::SeqCst) >= 2);
            first.assert_async().await;
            second.assert_async().await;
        }
    }

    #[derive(Debug)]
    struct BulkDeleteProvider {
        repartition: AtomicUsize,
    }

    #[async_trait]
    impl StorageCredentialProvider for BulkDeleteProvider {
        async fn load_credential(&self, location: &str) -> iceberg::Result<StorageCredential> {
            let (prefix, key) = [
                ("s3://bucket/first/", "FIRST"),
                ("s3://bucket/second/", "SECOND"),
                ("s3://other/first/", "OTHER"),
            ]
            .into_iter()
            .find(|(prefix, _)| location.starts_with(prefix))
            .expect("unexpected credential location");
            let prefix =
                if self.repartition.load(Ordering::SeqCst) != 0 && location.ends_with("/private") {
                    location.to_string()
                } else {
                    prefix.to_string()
                };
            Ok(
                StorageCredential::new(iceberg::io::StorageCredentialKind::S3(
                    iceberg::io::S3Credential::new(key, "dummy-secret", None),
                ))
                .with_prefix(prefix)
                .with_expiration(SystemTime::now() + Duration::from_secs(3600)),
            )
        }
    }

    #[tokio::test]
    async fn credentialed_delete_stream_batches_by_bucket_and_scope() {
        let direct = Arc::new(OpenDalStorageFactory::s3());
        let resolving = Arc::new(OpenDalResolvingStorageFactory::new());
        let factories: [Arc<dyn StorageFactory>; 2] = [direct.clone(), resolving];
        for factory in factories {
            let mut server = Server::new_async().await;
            let first = server
                .mock("POST", "/bucket/")
                .match_query(Matcher::Any)
                .match_header("authorization", Matcher::Regex("Credential=FIRST/".into()))
                .match_body(Matcher::Regex(
                    r"^<Delete><Quiet>true</Quiet>(<Object><Key>first/[0-9]+</Key></Object>){1000}</Delete>$".into(),
                ))
                .with_body("<DeleteResult/>")
                .expect(1)
                .create_async()
                .await;
            let remainder = server
                .mock("POST", "/bucket/")
                .match_query(Matcher::Any)
                .match_header("authorization", Matcher::Regex("Credential=FIRST/".into()))
                .match_body(Matcher::Regex(
                    r"^<Delete><Quiet>true</Quiet>(<Object><Key>first/100[01]</Key></Object>){2}</Delete>$".into(),
                ))
                .with_body("<DeleteResult/>")
                .expect(1)
                .create_async()
                .await;
            let second = server
                .mock("POST", "/bucket/")
                .match_query(Matcher::Any)
                .match_header("authorization", Matcher::Regex("Credential=SECOND/".into()))
                .match_body(Matcher::Regex(r"<Key>second/file</Key>".into()))
                .with_body("<DeleteResult/>")
                .expect(1)
                .create_async()
                .await;
            let other = server
                .mock("POST", "/other/")
                .match_query(Matcher::Any)
                .match_header("authorization", Matcher::Regex("Credential=OTHER/".into()))
                .match_body(Matcher::Regex(r"<Key>first/file</Key>".into()))
                .with_body("<DeleteResult/>")
                .expect(1)
                .create_async()
                .await;
            let serial = server
                .mock("DELETE", Matcher::Any)
                .expect(0)
                .create_async()
                .await;
            let io = anonymous_s3_builder(factory, &server.url())
                .with_credential_provider(Arc::new(BulkDeleteProvider {
                    repartition: AtomicUsize::new(0),
                }))
                .build();
            let mut paths: Vec<_> = (0..1002)
                .map(|index| format!("s3://bucket/first/{index}"))
                .collect();
            paths.extend([
                "s3://bucket/second/file".to_string(),
                "s3://bucket/second/other".to_string(),
                "s3://other/first/file".to_string(),
                "s3://other/first/other".to_string(),
            ]);
            let result = io.delete_stream(futures::stream::iter(paths).boxed()).await;
            first.assert_async().await;
            remainder.assert_async().await;
            second.assert_async().await;
            other.assert_async().await;
            serial.assert_async().await;
            result.unwrap();
        }
        // Bulk operators are scope-local, not one cached operator per file.
        assert_eq!(direct.operator_cache.len(), 0);
    }

    #[tokio::test]
    async fn delegated_s3_allow_anonymous_does_not_bypass_provider_errors() {
        for factory in s3_factories() {
            for incomplete in [false, true] {
                let mut server = Server::new_async().await;
                let request = server
                    .mock("GET", Matcher::Any)
                    .with_status(200)
                    .with_body("anonymous data")
                    .expect(0)
                    .create_async()
                    .await;
                let io = anonymous_s3_builder(factory.clone(), &server.url())
                    // A rejected provider must not fall back to static credentials either.
                    .with_prop(S3_ACCESS_KEY_ID, "STATIC")
                    .with_prop(S3_SECRET_ACCESS_KEY, "dummy-static-secret")
                    .with_credential_provider(Arc::new(RejectingProvider { incomplete }))
                    .build();

                assert!(
                    io.new_input("s3://bucket/table/file")
                        .unwrap()
                        .read()
                        .await
                        .is_err()
                );
                request.assert_async().await;
            }
        }
    }

    #[tokio::test]
    async fn credentialed_s3_disables_legacy_anonymous_config() {
        use iceberg::io::Storage;
        use opendal::services::S3Config;

        use crate::OpenDalStorage;

        let mut server = Server::new_async().await;
        let request = server
            .mock("GET", "/bucket/table/file")
            .match_header(
                "authorization",
                Matcher::Regex("Credential=KEY0/".to_string()),
            )
            .with_body("data")
            .expect(1)
            .create_async()
            .await;
        // Public enum users can supply OpenDAL's deprecated alias directly.
        let mut config = S3Config::default();
        config.endpoint = Some(server.url());
        config.region = Some("us-east-1".to_string());
        #[allow(deprecated)]
        {
            config.allow_anonymous = true;
        }
        let storage = OpenDalStorage::Credentialed {
            storage: Box::new(OpenDalStorage::S3 {
                config: Arc::new(config),
            }),
            provider: CredentialProvider(Arc::new(Provider(AtomicUsize::new(0)))),
        };

        assert_eq!(
            storage
                .read("s3://bucket/table/file")
                .await
                .unwrap()
                .as_ref(),
            b"data"
        );
        request.assert_async().await;
    }

    #[tokio::test]
    async fn batch_signer_does_not_cache_long_lived_credentials_across_scope_changes() {
        for factory in s3_factories() {
            let mut server = Server::new_async().await;
            let request = server
                .mock("GET", "/bucket/first/file")
                .match_header("authorization", Matcher::Regex("Credential=FIRST/".into()))
                .with_body("data")
                .expect(1)
                .create_async()
                .await;
            let provider = Arc::new(BulkDeleteProvider {
                repartition: AtomicUsize::new(0),
            });
            let batch = super::BatchCredential::provider(
                CredentialProvider(provider.clone()),
                "s3://bucket/first/".to_string(),
                vec![
                    "s3://bucket/first/file".to_string(),
                    "s3://bucket/first/private".to_string(),
                ],
            );
            let location = "s3://bucket/first/file";
            batch.0.load_credential(location).await.unwrap();
            let io = anonymous_s3_builder(factory, &server.url())
                .with_credential_provider(batch.0.clone())
                .build();
            let file = io.new_input(location).unwrap();
            assert_eq!(file.read().await.unwrap().as_ref(), b"data");
            provider.repartition.store(1, Ordering::SeqCst);
            // Revalidate the child both directly and through the cached signer.
            assert!(batch.0.load_credential(location).await.is_err());
            // The cached operator must not hide a new child scope on the next
            // signing attempt, even when the underlying key has a long TTL.
            assert!(file.read().await.is_err());
            request.assert_async().await;
        }
    }

    #[tokio::test]
    async fn credentialed_s3_storage_errors_keep_status_and_service_code() {
        for factory in s3_factories() {
            let mut server = Server::new_async().await;
            let request = server
                .mock("GET", "/bucket/table/file")
                .with_status(403)
                .with_header("x-secret-header", "secret-header")
                .with_body("<Error><Code>AccessDenied</Code><Message>secret-body</Message></Error>")
                .expect(1)
                .create_async()
                .await;
            let io = anonymous_s3_builder(factory, &server.url())
                .with_credential_provider(Arc::new(Provider(AtomicUsize::new(0))))
                .build();
            let error = io
                .new_input("s3://bucket/table/file")
                .unwrap()
                .read()
                .await
                .unwrap_err();
            let diagnostic = format!("{error} {error:?} {error:#?}");
            assert!(
                diagnostic.contains("failure_stage: storage"),
                "{diagnostic}"
            );
            assert!(diagnostic.contains("status: 403"), "{diagnostic}");
            assert!(diagnostic.contains("AccessDenied"), "{diagnostic}");
            assert!(!diagnostic.contains("secret-"));
            request.assert_async().await;
        }
    }

    #[tokio::test]
    async fn static_s3_allow_anonymous_still_sends_unsigned_requests() {
        for factory in s3_factories() {
            let mut server = Server::new_async().await;
            let request = server
                .mock("GET", "/bucket/table/file")
                .match_header("authorization", Matcher::Missing)
                .match_header("x-amz-security-token", Matcher::Missing)
                .with_body("public data")
                .expect(1)
                .create_async()
                .await;
            let io = anonymous_s3_builder(factory, &server.url()).build();

            assert_eq!(
                io.new_input("s3://bucket/table/file")
                    .unwrap()
                    .read()
                    .await
                    .unwrap()
                    .as_ref(),
                b"public data"
            );
            request.assert_async().await;
        }
    }

    #[tokio::test]
    async fn an_open_s3_reader_resigns_with_the_current_complete_credential() {
        for factory in s3_factories() {
            let mut server = Server::new_async().await;
            let provider = Arc::new(Provider(AtomicUsize::new(0)));
            let io = anonymous_s3_builder(factory, &server.url())
                .with_credential_provider(provider.clone())
                .build();
            let reader = io
                .new_input("s3://bucket/table/file")
                .unwrap()
                .reader()
                .await
                .unwrap();
            for generation in 0..2 {
                provider.0.store(generation, Ordering::SeqCst);
                let request = server
                    .mock("GET", "/bucket/table/file")
                    .match_header(
                        "authorization",
                        Matcher::Regex(format!("Credential=KEY{generation}/")),
                    )
                    .match_header(
                        "x-amz-security-token",
                        format!("SESSION{generation}").as_str(),
                    )
                    .with_status(206)
                    .with_header("content-range", "bytes 0-3/4")
                    .with_body("data")
                    .expect(1)
                    .create_async()
                    .await;
                let result = reader.read(0..4).await;
                request.assert_async().await;
                assert_eq!(result.unwrap().as_ref(), b"data");
            }
        }
    }
}
