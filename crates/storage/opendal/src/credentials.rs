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
use iceberg::io::{CredentialProvider, FileIOCredential, FileIOCredentialProvider};
use iceberg::{Error, ErrorKind, Result};
use reqsign_core::{Context, ProvideCredential};

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
pub(crate) struct PathCredential {
    pub(crate) provider: CredentialProvider,
    pub(crate) location: String,
}

impl std::fmt::Debug for PathCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PathCredential").finish_non_exhaustive()
    }
}

impl PathCredential {
    async fn load(&self) -> Result<FileIOCredential> {
        crate::utils::clear_credential_failure();
        let credential = self.provider.0.credential(&self.location).await?;
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
impl FileIOCredentialProvider for BatchCredential {
    async fn credential(&self, _: &str) -> Result<FileIOCredential> {
        let mut selected: Option<FileIOCredential> = None;
        for location in &self.locations {
            let credential = self.provider.0.credential(location).await?;
            if credential.prefix() != Some(self.prefix.as_str()) || !credential.covers(location) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Storage credential scope changed during bulk deletion",
                ));
            }
            if let Some(selected) = &mut selected {
                if selected.properties != credential.properties {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Storage credentials differ within a bulk deletion scope",
                    ));
                }
                selected.expires_at = selected.expires_at.min(credential.expires_at);
            } else {
                selected = Some(credential);
            }
        }
        let mut selected =
            selected.ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Empty credential batch"))?;
        // Stay below reqsign's cache freshness windows (Azure: 20s, AWS:
        // 120s), but above AWS's 10s signing headroom. Every signing attempt
        // must re-select the batch, even if a custom provider uses a long TTL.
        let lease = if selected
            .properties
            .contains_key(iceberg::io::ADLS_SAS_TOKEN)
        {
            Duration::from_secs(5)
        } else {
            Duration::from_secs(30)
        };
        selected.expires_at = selected.expires_at.min(SystemTime::now() + lease);
        Ok(selected)
    }
}

#[cfg(feature = "opendal-azdls")]
#[derive(Debug)]
pub(crate) struct AzdlsPathCredential(pub(crate) PathCredential);

#[cfg(feature = "opendal-azdls")]
impl ProvideCredential for AzdlsPathCredential {
    type Credential = reqsign_azure_storage::Credential;

    async fn provide_credential(
        &self,
        _: &Context,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        let credential = self
            .0
            .load()
            .await
            .map_err(crate::utils::credential_provider_error)?;
        let token = credential
            .properties
            .get(iceberg::io::ADLS_SAS_TOKEN)
            .filter(|token| !token.is_empty())
            .ok_or_else(|| {
                reqsign_core::Error::credential_invalid("Missing ADLS SAS credential")
            })?;
        Ok(Some(
            reqsign_azure_storage::Credential::with_sas_token_expires_at(
                token,
                timestamp(credential.expires_at)?,
            ),
        ))
    }
}

#[cfg(feature = "opendal-s3")]
#[derive(Debug)]
pub(crate) struct AwsPathCredential(pub(crate) PathCredential);

#[cfg(feature = "opendal-s3")]
impl ProvideCredential for AwsPathCredential {
    type Credential = reqsign_aws_v4::Credential;

    async fn provide_credential(
        &self,
        _: &Context,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        let credential = self
            .0
            .load()
            .await
            .map_err(crate::utils::credential_provider_error)?;
        let required = |key| {
            credential
                .properties
                .get(key)
                .filter(|v| !v.is_empty())
                .cloned()
                .ok_or_else(|| reqsign_core::Error::credential_invalid("Incomplete S3 credential"))
        };
        Ok(Some(reqsign_aws_v4::Credential {
            access_key_id: required(iceberg::io::S3_ACCESS_KEY_ID)?,
            secret_access_key: required(iceberg::io::S3_SECRET_ACCESS_KEY)?,
            session_token: credential
                .properties
                .get(iceberg::io::S3_SESSION_TOKEN)
                .cloned(),
            expires_in: Some(timestamp(credential.expires_at)?),
        }))
    }
}

#[cfg(all(test, feature = "opendal-azdls"))]
mod adls_batch_tests {
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime};

    use futures::StreamExt;
    use iceberg::io::{ADLS_ENDPOINT, ADLS_SAS_TOKEN, FileIOBuilder, StorageFactory};
    use mockito::{Matcher, Server};

    use super::*;
    use crate::{OpenDalResolvingStorageFactory, OpenDalStorageFactory};

    #[derive(Debug)]
    struct Provider;

    #[async_trait]
    impl FileIOCredentialProvider for Provider {
        async fn credential(&self, location: &str) -> Result<FileIOCredential> {
            let mut root = url::Url::parse(location)?;
            let filesystem = root.username().to_string();
            root.set_path("/");
            Ok(FileIOCredential {
                prefix: Some(root.to_string()),
                properties: HashMap::from([(
                    ADLS_SAS_TOKEN.to_string(),
                    format!("sig={filesystem}"),
                )]),
                expires_at: SystemTime::now() + Duration::from_secs(30),
            })
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
            .with_credentials(CredentialProvider(Arc::new(Provider)))
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
                .with_credentials(CredentialProvider(Arc::new(Provider)))
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
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, SystemTime};

    use async_trait::async_trait;
    use futures::StreamExt;
    use iceberg::io::{
        CredentialProvider, FileIOBuilder, FileIOCredential, FileIOCredentialProvider,
        S3_ACCESS_KEY_ID, S3_ALLOW_ANONYMOUS, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
        S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN, StorageFactory,
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

    #[derive(Debug)]
    struct Provider(AtomicUsize);

    #[async_trait]
    impl FileIOCredentialProvider for Provider {
        async fn credential(&self, location: &str) -> iceberg::Result<FileIOCredential> {
            assert_eq!(location, "s3://bucket/table/file");
            let generation = self.0.load(Ordering::SeqCst);
            Ok(FileIOCredential {
                prefix: None,
                properties: HashMap::from([
                    (S3_ACCESS_KEY_ID.to_string(), format!("KEY{generation}")),
                    (S3_SECRET_ACCESS_KEY.to_string(), "dummy-secret".to_string()),
                    (S3_SESSION_TOKEN.to_string(), format!("SESSION{generation}")),
                ]),
                expires_at: SystemTime::now() + Duration::from_secs(30),
            })
        }
    }

    #[derive(Debug)]
    struct RejectingProvider {
        incomplete: bool,
    }

    #[async_trait]
    impl FileIOCredentialProvider for RejectingProvider {
        async fn credential(&self, _: &str) -> iceberg::Result<FileIOCredential> {
            if self.incomplete {
                Ok(FileIOCredential {
                    prefix: None,
                    properties: HashMap::new(),
                    expires_at: SystemTime::now() + Duration::from_secs(30),
                })
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
    impl FileIOCredentialProvider for ScopedDeleteProvider {
        async fn credential(&self, location: &str) -> iceberg::Result<FileIOCredential> {
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
            Ok(FileIOCredential {
                prefix: None,
                properties: HashMap::from([
                    (S3_ACCESS_KEY_ID.to_string(), key.to_string()),
                    (S3_SECRET_ACCESS_KEY.to_string(), "dummy-secret".to_string()),
                ]),
                expires_at: SystemTime::now() + Duration::from_secs(30),
            })
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
                .with_credentials(CredentialProvider(provider.clone()))
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
    impl FileIOCredentialProvider for BulkDeleteProvider {
        async fn credential(&self, location: &str) -> iceberg::Result<FileIOCredential> {
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
            Ok(FileIOCredential {
                prefix: Some(prefix),
                properties: HashMap::from([
                    (S3_ACCESS_KEY_ID.to_string(), key.to_string()),
                    (S3_SECRET_ACCESS_KEY.to_string(), "dummy-secret".to_string()),
                ]),
                expires_at: SystemTime::now() + Duration::from_secs(3600),
            })
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
                .with_credentials(CredentialProvider(Arc::new(BulkDeleteProvider {
                    repartition: AtomicUsize::new(0),
                })))
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
    async fn batch_signer_revalidates_child_scopes_after_refresh() {
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
        batch.0.credential("s3://bucket/first/file").await.unwrap();
        provider.repartition.store(1, Ordering::SeqCst);
        // The representative file still has the old scope, but the child does
        // not. Re-signing the batch must fail rather than use the parent's key.
        assert!(batch.0.credential("s3://bucket/first/file").await.is_err());
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
                    .with_credentials(CredentialProvider(Arc::new(RejectingProvider {
                        incomplete,
                    })))
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
            let io = anonymous_s3_builder(factory, &server.url())
                .with_credentials(batch)
                .build();
            let file = io.new_input("s3://bucket/first/file").unwrap();
            assert_eq!(file.read().await.unwrap().as_ref(), b"data");
            provider.repartition.store(1, Ordering::SeqCst);
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
                .with_credentials(CredentialProvider(Arc::new(Provider(AtomicUsize::new(0)))))
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
                .with_credentials(CredentialProvider(provider.clone()))
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
