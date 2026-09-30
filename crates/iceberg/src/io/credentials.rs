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

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Result;

/// One complete authentication configuration, valid until `expires_at`.
///
/// Providers must not combine fields belonging to different credentials.
#[derive(Clone)]
pub struct FileIOCredential {
    /// Matched storage-location prefix. `None` provides no reusable scope.
    ///
    /// This is the selected match, not just an enclosing permission boundary.
    pub prefix: Option<String>,
    /// Backend authentication properties. Never log these values.
    pub properties: HashMap<String, String>,
    /// Deadline for the signer's cached copy, including any refresh lease.
    pub expires_at: SystemTime,
}

impl FileIOCredential {
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
        let (Ok(prefix), Ok(location)) = (url::Url::parse(prefix), url::Url::parse(location))
        else {
            return false;
        };
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
}

impl Debug for FileIOCredential {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileIOCredential")
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

/// Supplies credentials for the actual file URI, including for open handles.
#[async_trait]
pub trait FileIOCredentialProvider: Debug + Send + Sync + 'static {
    /// Return a usable credential, refreshing it if necessary.
    ///
    /// Providers may declare the selected prefix to enable scope-local bulk
    /// operations. Consumers must revalidate all batch locations on refresh,
    /// since the selected prefixes can change.
    async fn credential(&self, location: &str) -> Result<FileIOCredential>;
}

/// An identity-bearing, redacted runtime provider.
///
/// Runtime providers cannot be serialized: silently dropping one would enable
/// an unintended fallback to static or ambient credentials after deserialization.
#[derive(Clone)]
pub struct CredentialProvider(pub Arc<dyn FileIOCredentialProvider>);

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

    #[test]
    fn credential_scope_checks_authority_and_path_boundaries() {
        let credential = FileIOCredential {
            prefix: Some("abfss://fs@account.dfs.core.windows.net/table/".into()),
            properties: HashMap::new(),
            expires_at: SystemTime::now(),
        };
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
        let credential = FileIOCredential {
            prefix: Some("s3://bucket/table".into()),
            ..credential
        };
        assert!(credential.covers("s3://bucket/table"));
        assert!(credential.covers("s3://bucket/table/file"));
        assert!(!credential.covers("s3://bucket/table-other/file"));
    }
}
