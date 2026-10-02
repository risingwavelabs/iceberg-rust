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
use reqsign_core::{Context, ProvideCredential};

use crate::io::{
    CredentialProvider, StorageCredential, StorageCredentialKind, StorageCredentialProvider,
};
#[cfg(test)]
use crate::io::{OpenDalResolvingStorageFactory, OpenDalStorageFactory};
use crate::{Error, ErrorKind, Result};

// reqsign's cache freshness windows are 20s for Azure and 120s for AWS.
// Keep signing leases below those windows so every signing attempt loads the
// current scopes, without changing the provider's actual credential lifetime.
#[cfg(feature = "storage-azdls")]
const ADLS_SIGNING_LEASE: Duration = Duration::from_secs(5);
#[cfg(feature = "storage-s3")]
const S3_SIGNING_LEASE: Duration = Duration::from_secs(30);
// AWS signing needs 10s; allow another 5s for selection and request construction.
#[cfg(feature = "storage-s3")]
const S3_MINIMUM_SIGNING_VALIDITY: Duration = Duration::from_secs(15);

#[cfg(test)]
pub(super) struct TestCredentialProvider<F> {
    load: F,
    supports: fn(&str) -> bool,
}

#[cfg(test)]
impl<F> std::fmt::Debug for TestCredentialProvider<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestCredentialProvider")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
pub(super) fn test_provider<F>(load: F) -> TestCredentialProvider<F>
where F: Fn(&str, Duration) -> Result<StorageCredential> + Send + Sync {
    TestCredentialProvider {
        load,
        supports: |_| true,
    }
}

#[cfg(all(test, feature = "storage-s3"))]
impl<F> TestCredentialProvider<F> {
    fn with_supports(mut self, supports: fn(&str) -> bool) -> Self {
        self.supports = supports;
        self
    }
}

#[cfg(test)]
#[async_trait]
impl<F> StorageCredentialProvider for TestCredentialProvider<F>
where F: Fn(&str, Duration) -> Result<StorageCredential> + Send + Sync
{
    fn supports_path(&self, path: &str) -> bool {
        (self.supports)(path)
    }

    async fn load_credential_with_minimum_validity(
        &self,
        path: &str,
        minimum_validity: Duration,
    ) -> Result<StorageCredential> {
        (self.load)(path, minimum_validity)
    }
}

#[cfg(test)]
fn fixed_provider(credential: StorageCredential) -> impl StorageCredentialProvider {
    test_provider(move |_, _| Ok(credential.clone()))
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

fn signing_expiry(
    credential: &StorageCredential,
    lease: Duration,
) -> reqsign_core::Result<reqsign_core::time::Timestamp> {
    // reqsign checks cache freshness only on cached credentials. A newly
    // loaded credential needs only the signing operation's exact validity.
    // Cap this signing copy so reqsign cannot bypass the provider on the
    // next attempt, including for long-lived or non-expiring credentials.
    let deadline = SystemTime::now() + lease;
    timestamp(
        credential
            .expires_at()
            .map_or(deadline, |expiry| expiry.min(deadline)),
    )
}

// Check custom providers at the consumer boundary, even when they ignore the
// required lifetime. Do not let an expired freshly loaded credential be signed.
fn validate_credential_validity(
    credential: &StorageCredential,
    minimum_validity: Duration,
) -> Result<()> {
    if credential.expires_at().is_some_and(|expiry| {
        expiry
            .duration_since(SystemTime::now())
            .map_or(true, |remaining| remaining <= minimum_validity)
    }) {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Storage credential does not meet required validity",
        ));
    }
    Ok(())
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
        super::utils::clear_credential_failure();
        let credential = self
            .provider
            .0
            .load_credential_with_minimum_validity(&self.location, minimum_validity)
            .await?;
        validate_credential_validity(&credential, minimum_validity)?;
        if !credential.covers(&self.location) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Storage credential does not cover signing location",
            ));
        }
        Ok(credential)
    }
}

/// Revalidates a batch of paths matched to one credential scope.
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
            validate_credential_validity(&credential, minimum_validity)?;
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
        selected.ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Empty credential batch"))
    }
}

#[cfg(test)]
mod batch_tests {
    use super::*;

    #[tokio::test]
    async fn consumer_boundaries_reject_providers_ignoring_required_validity() {
        let prefix = "s3://bucket/table/";
        let path = format!("{prefix}one");
        for (expiry, minimum, valid) in [
            (None, Duration::from_secs(15), true),
            (
                Some(SystemTime::now() - Duration::from_secs(1)),
                Duration::ZERO,
                false,
            ),
            (
                Some(SystemTime::now() + Duration::from_secs(5)),
                Duration::from_secs(15),
                false,
            ),
            (
                Some(SystemTime::now() + Duration::from_secs(60)),
                Duration::from_secs(15),
                true,
            ),
        ] {
            let mut credential = StorageCredential::new(StorageCredentialKind::S3(
                crate::io::S3Credential::new("key", "dummy-secret", None),
            ))
            .with_prefix(prefix);
            if let Some(expiry) = expiry {
                credential = credential.with_expiration(expiry);
            }
            // This fixture deliberately ignores minimum validity, like a buggy
            // custom provider. Neither a file signer nor a batch may trust it.
            let provider = CredentialProvider(Arc::new(fixed_provider(credential)));
            let source = VendedCredentialSource {
                provider: provider.clone(),
                location: path.clone(),
            };
            assert_eq!(source.load(minimum).await.is_ok(), valid);
            let batch = BatchCredential::provider(provider, prefix.to_string(), vec![path.clone()]);
            assert_eq!(
                batch
                    .0
                    .load_credential_with_minimum_validity(&path, minimum)
                    .await
                    .is_ok(),
                valid
            );
        }
    }

    #[tokio::test]
    async fn batch_forwards_required_validity_to_every_path() {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let provider =
            Arc::new(test_provider(move |path, minimum| {
                assert_eq!(minimum, Duration::from_secs(17));
                recorded.lock().unwrap().push(path.to_string());
                Ok(
                    StorageCredential::new(StorageCredentialKind::S3(
                        crate::io::S3Credential::new("key", "dummy-secret", None),
                    ))
                    .with_prefix("s3://bucket/table/"),
                )
            }));
        let paths = vec![
            "s3://bucket/table/one".to_string(),
            "s3://bucket/table/two".to_string(),
        ];
        let batch = BatchCredential::provider(
            CredentialProvider(provider.clone()),
            "s3://bucket/table/".to_string(),
            paths.clone(),
        );
        batch
            .0
            .load_credential_with_minimum_validity("s3://bucket/table/", Duration::from_secs(17))
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), paths);
    }

    #[tokio::test]
    async fn batch_preserves_the_earliest_provider_expiry() {
        let prefix = "s3://bucket/table/";
        let expiry = SystemTime::now() + Duration::from_secs(60);
        for expiries in [
            [None, None],
            [Some(expiry + Duration::from_secs(300)), Some(expiry)],
            [Some(expiry), None],
        ] {
            let credentials = expiries.map(|expiry| {
                let credential = StorageCredential::new(StorageCredentialKind::S3(
                    crate::io::S3Credential::new("key", "dummy-secret", None),
                ))
                .with_prefix(prefix);
                match expiry {
                    Some(expiry) => credential.with_expiration(expiry),
                    None => credential,
                }
            });
            let batch = BatchCredential::provider(
                CredentialProvider(Arc::new(test_provider(move |location, _| {
                    Ok(credentials[usize::from(location.ends_with("/two"))].clone())
                }))),
                prefix.into(),
                vec![format!("{prefix}one"), format!("{prefix}two")],
            );
            let selected = batch.0.load_credential(prefix).await.unwrap();
            assert_eq!(selected.expires_at(), expiries.into_iter().flatten().min());
        }
    }
}

#[cfg(feature = "storage-azdls")]
#[derive(Debug)]
pub(crate) struct VendedAzdlsCredentialProvider(pub(crate) VendedCredentialSource);

#[cfg(feature = "storage-azdls")]
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
            .map_err(super::utils::credential_provider_error)?;
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
        Ok(Some(
            reqsign_azure_storage::Credential::with_sas_token_expires_at(
                material.sas_token(),
                signing_expiry(&credential, ADLS_SIGNING_LEASE)?,
            ),
        ))
    }
}

#[cfg(feature = "storage-s3")]
#[derive(Debug)]
pub(crate) struct VendedS3CredentialProvider(pub(crate) VendedCredentialSource);

#[cfg(feature = "storage-s3")]
impl ProvideCredential for VendedS3CredentialProvider {
    type Credential = reqsign_aws_v4::Credential;

    async fn provide_credential(
        &self,
        _: &Context,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        let credential = self
            .0
            .load(S3_MINIMUM_SIGNING_VALIDITY)
            .await
            .map_err(super::utils::credential_provider_error)?;
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
            expires_in: Some(signing_expiry(&credential, S3_SIGNING_LEASE)?),
        }))
    }
}

#[cfg(all(test, feature = "storage-azdls"))]
mod adls_batch_tests {
    use std::time::{Duration, SystemTime};

    use futures::StreamExt;
    use mockito::{Matcher, Server};

    use super::{OpenDalResolvingStorageFactory, OpenDalStorageFactory, *};
    use crate::io::{ADLS_ENDPOINT, AzdlsCredential, FileIOBuilder, StorageFactory};

    #[tokio::test]
    async fn signing_adapter_bypasses_azure_cache_without_extending_provider_expiry() {
        use reqsign_core::SigningCredential;

        let prefix = "abfss://fs@acct.dfs.core.windows.net/table/";
        for expiry in [
            None,
            Some(SystemTime::now() + Duration::from_secs(3600)),
            Some(SystemTime::now() + Duration::from_secs(3)),
        ] {
            let mut credential = StorageCredential::new(StorageCredentialKind::Azdls(
                AzdlsCredential::new("sig=test"),
            ))
            .with_prefix(prefix);
            if let Some(expiry) = expiry {
                credential = credential.with_expiration(expiry);
            }
            let provider = CredentialProvider(Arc::new(fixed_provider(credential)));
            let adapter = VendedAzdlsCredentialProvider(VendedCredentialSource {
                provider: provider.clone(),
                location: format!("{prefix}file"),
            });
            let start = SystemTime::now();
            let signing_credential = adapter
                .provide_credential(&Context::new())
                .await
                .unwrap()
                .unwrap();
            let reqsign_azure_storage::Credential::SasToken {
                expires_at: Some(signing_expiry),
                ..
            } = &signing_credential
            else {
                panic!("expected an expiring signing copy");
            };
            let deadline = start + ADLS_SIGNING_LEASE;
            assert!(
                *signing_expiry >= timestamp(expiry.map_or(deadline, |e| e.min(deadline))).unwrap()
            );
            assert!(*signing_expiry <= timestamp(SystemTime::now() + ADLS_SIGNING_LEASE).unwrap());
            if let Some(expiry) = expiry {
                assert!(*signing_expiry <= timestamp(expiry).unwrap());
            }
            // A fresh SAS is usable, but must not be reused from reqsign's cache.
            assert!(signing_credential.is_valid_at(timestamp(SystemTime::now()).unwrap()));
            assert!(!signing_credential.is_valid());
            assert_eq!(
                provider
                    .0
                    .load_credential(prefix)
                    .await
                    .unwrap()
                    .expires_at(),
                expiry
            );
        }
    }

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
                StorageCredentialKind::S3(crate::io::S3Credential::new("wrong", "wrong", None)),
            ] {
                let is_adls = matches!(kind, StorageCredentialKind::Azdls(_));
                let io = FileIOBuilder::from_storage_factory(factory.clone())
                    .with_prop(ADLS_ENDPOINT, format!("{}/core.windows.net", server.url()))
                    .with_prop("io.max-retries", "0")
                    .with_credential_provider(Arc::new(fixed_provider(StorageCredential::new(
                        kind,
                    ))))
                    .build()
                    .unwrap();
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

    fn filesystem_provider() -> impl StorageCredentialProvider {
        test_provider(|location, _| {
            let mut root = url::Url::parse(location)?;
            let filesystem = root.username().to_string();
            root.set_path("/");
            Ok(
                StorageCredential::new(StorageCredentialKind::Azdls(AzdlsCredential::new(
                    format!("sig={filesystem}"),
                )))
                .with_prefix(root.to_string())
                .with_expiration(SystemTime::now() + Duration::from_secs(3600)),
            )
        })
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
        let io = FileIOBuilder::from_storage_factory(Arc::new(OpenDalStorageFactory::azdls()))
            .with_prop(ADLS_ENDPOINT, format!("{}/core.windows.net", server.url()))
            .with_prop("io.max-retries", "0")
            .with_credential_provider(Arc::new(filesystem_provider()))
            .build()
            .unwrap();
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
            let io = FileIOBuilder::from_storage_factory(factory)
                .with_prop(ADLS_ENDPOINT, format!("{}/core.windows.net", server.url()))
                .with_prop("io.max-retries", "0")
                .with_credential_provider(Arc::new(filesystem_provider()))
                .build()
                .unwrap();
            io.delete_stream(futures::stream::iter(paths).boxed())
                .await
                .unwrap();
            for request in requests {
                request.assert_async().await;
            }
        }
        assert_eq!(direct.operator_cache.len(), 0);
    }
    #[tokio::test]
    async fn adls_deletion_loads_each_path_once() {
        let mut server = Server::new_async().await;
        let deletes = server
            .mock("DELETE", Matcher::Any)
            .with_status(200)
            .expect(32)
            .create_async()
            .await;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recorded = calls.clone();
        let provider = Arc::new(test_provider(move |_, _| {
            recorded.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(
                StorageCredential::new(StorageCredentialKind::Azdls(AzdlsCredential::new(
                    "sig=probe",
                )))
                .with_prefix("abfs://fs@account.dfs.core.windows.net/table/")
                .with_expiration(SystemTime::now() + Duration::from_secs(3600)),
            )
        }));
        let io = FileIOBuilder::from_storage_factory(Arc::new(OpenDalStorageFactory::azdls()))
            .with_prop(ADLS_ENDPOINT, format!("{}/core.windows.net", server.url()))
            .with_prop("io.max-retries", "0")
            .with_credential_provider(provider.clone())
            .build()
            .unwrap();
        let paths = (0..32)
            .map(|n| format!("abfs://fs@account.dfs.core.windows.net/table/{n}"))
            .collect::<Vec<_>>();
        io.delete_stream(futures::stream::iter(paths))
            .await
            .unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 32);
        deletes.assert_async().await;
    }
}

#[cfg(all(test, feature = "storage-s3"))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, SystemTime};

    use async_trait::async_trait;
    use futures::StreamExt;
    use mockito::{Matcher, Server};
    use tokio::sync::Barrier;

    use super::{
        OpenDalResolvingStorageFactory, OpenDalStorageFactory, S3_MINIMUM_SIGNING_VALIDITY,
        S3_SIGNING_LEASE, timestamp,
    };
    use crate::io::{
        CredentialProvider, FileIOBuilder, S3_ACCESS_KEY_ID, S3_ALLOW_ANONYMOUS, S3_ENDPOINT,
        S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY, StorageCredential,
        StorageCredentialProvider, StorageFactory,
    };

    fn s3_factories() -> [Arc<dyn StorageFactory>; 2] {
        [
            Arc::new(OpenDalStorageFactory::s3()),
            Arc::new(OpenDalResolvingStorageFactory::new()),
        ]
    }

    fn anonymous_s3_builder(factory: Arc<dyn StorageFactory>, endpoint: &str) -> FileIOBuilder {
        FileIOBuilder::from_storage_factory(factory)
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
                crate::io::StorageCredentialKind::S3(crate::io::S3Credential::new(
                    "FIXED",
                    "dummy-secret",
                    None,
                )),
                crate::io::StorageCredentialKind::Azdls(crate::io::AzdlsCredential::new(
                    "sig=wrong",
                )),
            ] {
                let is_s3 = matches!(kind, crate::io::StorageCredentialKind::S3(_));
                let io = anonymous_s3_builder(factory.clone(), &server.url())
                    .with_credential_provider(Arc::new(super::fixed_provider(
                        StorageCredential::new(kind),
                    )))
                    .build()
                    .unwrap();
                let result = io.new_input("s3://bucket/table/file").unwrap().read().await;
                assert_eq!(result.is_ok(), is_s3);
            }
            request.assert_async().await;
        }
    }

    #[tokio::test]
    async fn unsupported_paths_keep_static_authentication_for_reads_and_deletes() {
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
                .with_credential_provider(Arc::new(
                    super::test_provider(|_, _| {
                        panic!("unsupported paths must not load vended credentials")
                    })
                    .with_supports(|_| false),
                ))
                .build()
                .unwrap();
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
    async fn batch_signing_adapter_bypasses_aws_cache_without_extending_provider_expiry() {
        use reqsign_core::{Context, ProvideCredential, SigningCredential};

        let prefix = "s3://bucket/table/";
        for expiry in [
            None,
            Some(SystemTime::now() + Duration::from_secs(3600)),
            Some(SystemTime::now() + Duration::from_secs(20)),
        ] {
            let mut credential = StorageCredential::new(crate::io::StorageCredentialKind::S3(
                crate::io::S3Credential::new("key", "dummy-secret", None),
            ))
            .with_prefix(prefix);
            if let Some(expiry) = expiry {
                credential = credential.with_expiration(expiry);
            }
            let batch = super::BatchCredential::provider(
                CredentialProvider(Arc::new(super::fixed_provider(credential))),
                prefix.into(),
                vec![format!("{prefix}one"), format!("{prefix}two")],
            );
            let selected = batch
                .0
                .load_credential_with_minimum_validity(prefix, S3_MINIMUM_SIGNING_VALIDITY)
                .await
                .unwrap();
            assert_eq!(selected.expires_at(), expiry);

            let adapter = super::VendedS3CredentialProvider(super::VendedCredentialSource {
                provider: batch,
                location: format!("{prefix}one"),
            });
            let start = SystemTime::now();
            let signing_credential = adapter
                .provide_credential(&Context::new())
                .await
                .unwrap()
                .unwrap();
            let signing_expiry = signing_credential.expires_in.unwrap();
            let deadline = start + S3_SIGNING_LEASE;
            assert!(
                signing_expiry >= timestamp(expiry.map_or(deadline, |e| e.min(deadline))).unwrap()
            );
            assert!(signing_expiry <= timestamp(SystemTime::now() + S3_SIGNING_LEASE).unwrap());
            if let Some(expiry) = expiry {
                assert!(signing_expiry <= timestamp(expiry).unwrap());
            }
            // Allow AWS's signing headroom and our selection margin, but never
            // reuse a signing copy without checking the current credential scopes.
            assert!(
                signing_credential.is_valid_at(
                    timestamp(SystemTime::now() + S3_MINIMUM_SIGNING_VALIDITY).unwrap()
                )
            );
            assert!(!signing_credential.is_valid());
        }
    }

    #[tokio::test]
    async fn empty_credentialed_deletion_does_not_resolve_an_operator_or_provider() {
        for factory in s3_factories() {
            let io = FileIOBuilder::from_storage_factory(factory)
                .with_credential_provider(Arc::new(
                    super::test_provider(|_, _| panic!("empty deletion must not load credentials"))
                        .with_supports(|_| panic!("empty deletion must not select credentials")),
                ))
                .build()
                .unwrap();
            io.delete_stream(futures::stream::empty()).await.unwrap();
        }
    }

    #[derive(Debug)]
    struct RefreshingProvider(AtomicUsize);

    #[async_trait]
    impl StorageCredentialProvider for RefreshingProvider {
        async fn load_credential_with_minimum_validity(
            &self,
            location: &str,
            _minimum_validity: Duration,
        ) -> crate::Result<StorageCredential> {
            assert_eq!(location, "s3://bucket/table/file");
            let generation = self.0.load(Ordering::SeqCst);
            Ok(StorageCredential::new(crate::io::StorageCredentialKind::S3(
                crate::io::S3Credential::new(
                    format!("KEY{generation}"),
                    "dummy-secret",
                    Some(format!("SESSION{generation}")),
                ),
            ))
            .with_expiration(SystemTime::now() + Duration::from_secs(3600)))
        }
    }

    #[derive(Debug)]
    struct ScopedDeleteProvider {
        barrier: Barrier,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl StorageCredentialProvider for ScopedDeleteProvider {
        async fn load_credential_with_minimum_validity(
            &self,
            location: &str,
            _minimum_validity: Duration,
        ) -> crate::Result<StorageCredential> {
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
            Ok(StorageCredential::new(crate::io::StorageCredentialKind::S3(
                crate::io::S3Credential::new(key, "dummy-secret", None),
            ))
            .with_expiration(SystemTime::now() + Duration::from_secs(30)))
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
                .build()
                .unwrap();
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
        async fn load_credential_with_minimum_validity(
            &self,
            location: &str,
            _minimum_validity: Duration,
        ) -> crate::Result<StorageCredential> {
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
            Ok(StorageCredential::new(crate::io::StorageCredentialKind::S3(
                crate::io::S3Credential::new(key, "dummy-secret", None),
            ))
            .with_prefix(prefix)
            .with_expiration(SystemTime::now() + Duration::from_secs(3600)))
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
                .build()
                .unwrap();
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
                    .with_credential_provider(Arc::new(super::test_provider(move |_, _| {
                        if incomplete {
                            Ok(StorageCredential::new(
                                crate::io::StorageCredentialKind::S3(crate::io::S3Credential::new(
                                    "", "", None,
                                )),
                            ))
                        } else {
                            Err(crate::Error::new(
                                crate::ErrorKind::DataInvalid,
                                "No matching credential",
                            ))
                        }
                    })))
                    .build()
                    .unwrap();

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
        use opendal::services::S3Config;

        use crate::io::{OpenDalStorage, Storage};

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
        // OpenDAL configuration can still contain its deprecated alias.
        let mut config = S3Config::default();
        config.endpoint = Some(server.url());
        config.region = Some("us-east-1".to_string());
        #[allow(deprecated)]
        {
            config.allow_anonymous = true;
        }
        let mut storage = crate::io::storage::opendal::ConfiguredOpenDalStorage::new(
            OpenDalStorage::S3 {
                config: Arc::new(config),
            },
            &crate::io::StorageConfig::new(),
            crate::io::storage::opendal::default_operator_cache(),
        )
        .unwrap();
        storage.storage.provider = Some(CredentialProvider(Arc::new(RefreshingProvider(
            AtomicUsize::new(0),
        ))));
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
                .build()
                .unwrap();
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
                .with_credential_provider(Arc::new(RefreshingProvider(AtomicUsize::new(0))))
                .build()
                .unwrap();
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
            let io = anonymous_s3_builder(factory, &server.url())
                .build()
                .unwrap();

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
            let provider = Arc::new(RefreshingProvider(AtomicUsize::new(0)));
            let io = anonymous_s3_builder(factory, &server.url())
                .with_credential_provider(provider.clone())
                .build()
                .unwrap();
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
    #[tokio::test]
    async fn delete_discovery_redacts_provider_messages_and_sources() {
        let mut server = Server::new_async().await;
        let no_request = server
            .mock("GET", Matcher::Any)
            .expect(0)
            .create_async()
            .await;
        let io = FileIOBuilder::from_storage_factory(Arc::new(OpenDalStorageFactory::s3()))
            .with_prop("s3.region", "us-east-1")
            .with_prop("s3.endpoint", server.url())
            .with_prop("s3.path-style-access", "true")
            .with_prop("io.max-retries", "0")
            .with_credential_provider(Arc::new(super::test_provider(|_, _| {
                Err(
                    crate::Error::new(crate::ErrorKind::Unexpected, "probe-sensitive-value")
                        .with_source(std::io::Error::other("probe-sensitive-source"))
                        .with_context("status", "403")
                        .with_context("credential_error", "refresh_rejected"),
                )
            })))
            .build()
            .unwrap();
        let location = "s3://bucket/table/file";
        let read_error = io.new_input(location).unwrap().read().await.unwrap_err();
        assert!(!format!("{read_error:?}").contains("probe-sensitive-value"));
        let delete_error = io
            .delete_stream(futures::stream::iter(vec![location.to_string()]))
            .await
            .unwrap_err();
        assert!(
            !format!("{delete_error} {delete_error:?} {delete_error:#?}")
                .contains("probe-sensitive-value")
        );
        assert!(format!("{delete_error:?}").contains("failure_stage: credential"));
        assert!(!format!("{delete_error:#?}").contains("probe-sensitive-source"));
        assert!(format!("{delete_error:?}").contains("status: 403"));
        no_request.assert_async().await;
    }
}
