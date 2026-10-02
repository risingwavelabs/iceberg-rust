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

//! Runtime credentials shared by catalogs and storage implementations.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Result;

/// One complete authentication configuration, with optional scope and expiration.
///
/// Providers must not combine fields belonging to different credentials.
#[derive(Clone)]
pub struct StorageCredential {
    /// Matched storage-location prefix. `None` provides no reusable scope.
    ///
    /// This is the selected match, not just an enclosing permission boundary.
    prefix: Option<String>,
    /// Backend authentication material. Never log these values.
    kind: StorageCredentialKind,
    /// Deadline for the signer's cached copy, including any refresh lease.
    expires_at: Option<SystemTime>,
}

impl Debug for StorageCredential {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageCredential")
            .field("kind", &self.kind)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

impl StorageCredential {
    /// Create a credential with no declared scope or expiration.
    pub fn new(kind: StorageCredentialKind) -> Self {
        Self {
            prefix: None,
            kind,
            expires_at: None,
        }
    }

    /// Set the selected storage-location prefix.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Set the expiration or the deadline of the signer's cached lease.
    pub fn with_expiration(mut self, expires_at: SystemTime) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Return the backend-specific credential material.
    pub fn kind(&self) -> &StorageCredentialKind {
        &self.kind
    }

    /// Consume the credential and return its backend-specific material.
    pub fn into_kind(self) -> StorageCredentialKind {
        self.kind
    }

    /// Return the expiration, if known. `None` means non-expiring.
    pub fn expires_at(&self) -> Option<SystemTime> {
        self.expires_at
    }

    /// Return the matched storage-location prefix.
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// Check URI authority and path-segment boundaries for a declared scope.
    ///
    /// An undeclared scope does not constrain an individual credential, but
    /// must not be used to share a signer across different file locations.
    pub fn covers(&self, location: &str) -> bool {
        let Some(prefix) = self.prefix() else {
            return true;
        };
        storage_prefix_covers(prefix, location)
    }
}

/// Check a declared URI scope using exact scheme, authority and path boundaries.
///
/// Unlike scheme aliases in storage routing, credential scopes remain exact:
/// changing transport schemes must not broaden a catalog's selected scope.
pub fn storage_prefix_covers(prefix: &str, location: &str) -> bool {
    let (Ok(prefix), Ok(location)) = (url::Url::parse(prefix), url::Url::parse(location)) else {
        return false;
    };
    storage_prefix_covers_url(&prefix, &location)
}

/// Check a declared URI scope without reparsing already validated URLs.
pub fn storage_prefix_covers_url(prefix: &url::Url, location: &url::Url) -> bool {
    prefix.scheme() == location.scheme()
        && prefix.host_str() == location.host_str()
        && prefix.port() == location.port()
        && prefix.username() == location.username()
        && prefix.password().is_none()
        && location.password().is_none()
        && prefix.query().is_none()
        && location.query().is_none()
        && prefix.fragment().is_none()
        && location.fragment().is_none()
        && (prefix.path() == location.path()
            || location
                .path()
                .strip_prefix(prefix.path())
                .is_some_and(|suffix| prefix.path().ends_with('/') || suffix.starts_with('/')))
}

/// Backend-specific authentication material.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageCredentialKind {
    /// Amazon S3 credentials.
    S3(S3Credential),
    /// Azure Data Lake Storage credentials.
    Azdls(AzdlsCredential),
}

/// Amazon S3 access keys and an optional session token.
#[derive(Clone, PartialEq, Eq)]
pub struct S3Credential {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

impl S3Credential {
    /// Create Amazon S3 credentials.
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token,
        }
    }

    /// Return the AWS access key ID.
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// Return the AWS secret access key.
    pub fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }

    /// Return the AWS session token, if present.
    pub fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref()
    }

    /// Consume the credentials and return their component values.
    pub fn into_parts(self) -> (String, String, Option<String>) {
        (
            self.access_key_id,
            self.secret_access_key,
            self.session_token,
        )
    }
}

impl Debug for S3Credential {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Credential").finish_non_exhaustive()
    }
}

/// Azure Data Lake Storage shared access signature.
#[derive(Clone, PartialEq, Eq)]
pub struct AzdlsCredential {
    sas_token: String,
}

impl AzdlsCredential {
    /// Create Azure Data Lake Storage credentials.
    pub fn new(sas_token: impl Into<String>) -> Self {
        Self {
            sas_token: sas_token.into(),
        }
    }

    /// Return the Azure shared access signature.
    pub fn sas_token(&self) -> &str {
        &self.sas_token
    }

    /// Consume the credential and return its shared access signature.
    pub fn into_sas_token(self) -> String {
        self.sas_token
    }
}

impl Debug for AzdlsCredential {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzdlsCredential").finish_non_exhaustive()
    }
}

/// Supplies credentials for the actual file URI, including for open handles.
///
/// Implementations must cache internally: credential loading can run on every
/// signing attempt and must not perform network I/O for each cache hit.
#[async_trait]
pub trait StorageCredentialProvider: Debug + Send + Sync {
    /// Whether this provider is configured for a path.
    ///
    /// Unsupported paths retain normal authentication. Supported paths must
    /// fail closed if credential loading fails.
    fn supports_path(&self, _path: &str) -> bool {
        true
    }

    /// Return a usable credential, refreshing it if necessary.
    ///
    /// Providers may declare the selected prefix to enable scope-local bulk
    /// operations. Consumers must revalidate all batch locations on refresh,
    /// since the selected prefixes can change.
    async fn load_credential(&self, path: &str) -> Result<StorageCredential> {
        self.load_credential_with_minimum_validity(path, Duration::ZERO)
            .await
    }

    /// Return a credential valid for longer than `minimum_validity` from now.
    ///
    /// Consumers supply their signing-operation headroom here. Implementations
    /// must honor it or return an error. Refreshable providers should renew
    /// credentials that cannot meet it, including when considering cached
    /// credentials after a failed refresh. A credential without an expiration
    /// has no declared validity limit.
    async fn load_credential_with_minimum_validity(
        &self,
        path: &str,
        minimum_validity: Duration,
    ) -> Result<StorageCredential>;
}

/// An identity-bearing, redacted runtime provider.
///
/// Runtime providers cannot be serialized: silently dropping one would enable
/// an unintended fallback to static or ambient credentials after deserialization.
#[derive(Clone)]
pub struct CredentialProvider(pub Arc<dyn StorageCredentialProvider>);

impl Debug for CredentialProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialProvider").finish_non_exhaustive()
    }
}

impl PartialEq for CredentialProvider {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for CredentialProvider {}

impl Serialize for CredentialProvider {
    fn serialize<S: Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "Runtime credentials must be reconstructed from the catalog",
        ))
    }
}

impl<'de> Deserialize<'de> for CredentialProvider {
    fn deserialize<D: Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "Runtime credentials must be reconstructed from the catalog",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct RecordingProvider(std::sync::Mutex<Vec<(String, Duration)>>);
    #[async_trait]
    impl StorageCredentialProvider for RecordingProvider {
        async fn load_credential_with_minimum_validity(
            &self,
            path: &str,
            minimum_validity: Duration,
        ) -> Result<StorageCredential> {
            self.0
                .lock()
                .unwrap()
                .push((path.to_string(), minimum_validity));
            Ok(StorageCredential::new(StorageCredentialKind::S3(
                S3Credential::new("key", "dummy-secret", None),
            )))
        }
    }

    #[tokio::test]
    async fn default_load_forwards_the_path_with_zero_minimum_validity() {
        let provider = RecordingProvider::default();
        let dynamic: &dyn StorageCredentialProvider = &provider;
        let path = "s3://bucket/table/file";
        dynamic.load_credential(path).await.unwrap();
        dynamic
            .load_credential_with_minimum_validity(path, Duration::from_secs(17))
            .await
            .unwrap();
        assert_eq!(*provider.0.lock().unwrap(), vec![
            (path.to_string(), Duration::ZERO),
            (path.to_string(), Duration::from_secs(17)),
        ]);
    }

    #[test]
    fn typed_credentials_round_trip_and_redact_debug() {
        for kind in [
            StorageCredentialKind::S3(S3Credential::new(
                "secret-key",
                "secret-value",
                Some("secret-session".into()),
            )),
            StorageCredentialKind::Azdls(AzdlsCredential::new("sig=secret-token")),
        ] {
            let credential = StorageCredential::new(kind);
            assert_eq!(credential.prefix(), None);
            assert_eq!(credential.expires_at(), None);
            assert!(credential.covers("s3://bucket/file"));
            let credential = credential.with_prefix("s3://secret-bucket/secret-prefix/");
            for diagnostic in [format!("{credential:?}"), format!("{credential:#?}")] {
                assert!(!diagnostic.contains("secret-"));
                assert!(!diagnostic.contains("sig="));
            }
            match credential.into_kind() {
                StorageCredentialKind::S3(material) => assert_eq!(
                    material.into_parts(),
                    (
                        "secret-key".into(),
                        "secret-value".into(),
                        Some("secret-session".into())
                    )
                ),
                StorageCredentialKind::Azdls(material) => {
                    assert_eq!(material.into_sas_token(), "sig=secret-token");
                }
            }
        }
    }

    #[test]
    fn default_factory_builds_static_storage_but_rejects_runtime_credentials() {
        use crate::io::{MemoryStorageFactory, StorageConfig, StorageFactory};

        let provider = Arc::new(RecordingProvider::default());
        let config = StorageConfig::new();
        assert!(MemoryStorageFactory.build(&config).is_ok());
        let error = MemoryStorageFactory
            .build_with_credentials(&config, provider.clone())
            .unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::FeatureUnsupported);
        assert!(
            provider.0.lock().unwrap().is_empty(),
            "unsupported factories must not fetch credentials"
        );
    }

    #[test]
    fn credential_scope_checks_authority_and_path_boundaries() {
        let credential = StorageCredential::new(StorageCredentialKind::Azdls(
            AzdlsCredential::new("sig=test"),
        ))
        .with_prefix("abfss://fs@account.dfs.core.windows.net/table/");
        assert!(credential.covers("abfss://fs@account.dfs.core.windows.net/table/file"));
        for location in [
            "abfss://other@account.dfs.core.windows.net/table/file",
            "abfss://fs@other.dfs.core.windows.net/table/file",
            "abfs://fs@account.dfs.core.windows.net/table/file",
            "abfss://fs@account.dfs.core.windows.net/table-other/file",
            "abfss://fs@account.dfs.core.windows.net/table",
            "abfss://fs@account.dfs.core.windows.net/table/file?sig=secret",
        ] {
            assert!(!credential.covers(location));
        }
        let credential = credential.with_prefix("s3://bucket/table");
        assert!(credential.covers("s3://bucket/table"));
        assert!(credential.covers("s3://bucket/table/file"));
        assert!(!credential.covers("s3://bucket/table-other/file"));
    }
}
