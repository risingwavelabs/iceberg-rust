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

use iceberg::io::CredentialProvider;
use reqsign_core::{Context, ProvideCredential};

fn timestamp(time: std::time::SystemTime) -> reqsign_core::Result<reqsign_core::time::Timestamp> {
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
            .provider
            .0
            .credential(&self.0.location)
            .await
            .map_err(|_| {
                reqsign_core::Error::credential_invalid("Unable to obtain ADLS storage credentials")
            })?;
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
            .provider
            .0
            .credential(&self.0.location)
            .await
            .map_err(|_| {
                reqsign_core::Error::credential_invalid("Unable to obtain S3 storage credentials")
            })?;
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

#[cfg(all(test, feature = "opendal-s3"))]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, SystemTime};

    use async_trait::async_trait;
    use iceberg::io::{
        CredentialProvider, FileIOBuilder, FileIOCredential, FileIOCredentialProvider,
        S3_ACCESS_KEY_ID, S3_ALLOW_ANONYMOUS, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
        S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN, StorageFactory,
    };
    use mockito::{Matcher, Server};

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
