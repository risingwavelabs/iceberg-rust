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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use opendal::services::S3Config;
use opendal::{Configurator, Operator};
pub use reqsign::{AwsCredential, AwsCredentialLoad};
use reqsign_core::ProvideCredentialChain;
use reqwest::Client;
use url::Url;

use super::credentials::{VendedCredentialSource, VendedS3CredentialProvider};
use super::utils::{from_opendal_error, is_truthy};
use crate::io::{
    CLIENT_REGION, CredentialProvider, S3_ACCESS_KEY_ID, S3_ALLOW_ANONYMOUS, S3_ASSUME_ROLE_ARN,
    S3_ASSUME_ROLE_EXTERNAL_ID, S3_ASSUME_ROLE_SESSION_NAME, S3_DISABLE_CONFIG_LOAD,
    S3_DISABLE_EC2_METADATA, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN, S3_SSE_KEY, S3_SSE_MD5, S3_SSE_TYPE,
};
use crate::{Error, ErrorKind, Result};

/// Parse iceberg props to s3 config.
pub(crate) fn s3_config_parse(mut m: HashMap<String, String>) -> Result<S3Config> {
    let mut cfg = S3Config::default();
    cfg.enable_virtual_host_style = false;
    // Preserve the release line's path-style default unless explicitly overridden.
    if let Some(endpoint) = m.remove(S3_ENDPOINT) {
        cfg.endpoint = Some(endpoint);
    };
    if let Some(access_key_id) = m.remove(S3_ACCESS_KEY_ID) {
        cfg.access_key_id = Some(access_key_id);
    };
    if let Some(secret_access_key) = m.remove(S3_SECRET_ACCESS_KEY) {
        cfg.secret_access_key = Some(secret_access_key);
    };
    if let Some(session_token) = m.remove(S3_SESSION_TOKEN) {
        cfg.session_token = Some(session_token);
    };
    if let Some(region) = m.remove(S3_REGION) {
        cfg.region = Some(region);
    };
    if let Some(region) = m.remove(CLIENT_REGION) {
        cfg.region = Some(region);
    };
    if let Some(path_style_access) = m.remove(S3_PATH_STYLE_ACCESS) {
        cfg.enable_virtual_host_style = !is_truthy(path_style_access.to_lowercase().as_str());
    };
    if let Some(arn) = m.remove(S3_ASSUME_ROLE_ARN) {
        cfg.role_arn = Some(arn);
    }
    if let Some(external_id) = m.remove(S3_ASSUME_ROLE_EXTERNAL_ID) {
        cfg.external_id = Some(external_id);
    };
    if let Some(session_name) = m.remove(S3_ASSUME_ROLE_SESSION_NAME) {
        cfg.role_session_name = Some(session_name);
    };
    let s3_sse_key = m.remove(S3_SSE_KEY);
    if let Some(sse_type) = m.remove(S3_SSE_TYPE) {
        match sse_type.to_lowercase().as_str() {
            // No Server Side Encryption
            "none" => {}
            // S3 SSE-S3 encryption (S3 managed keys). https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html
            "s3" => {
                cfg.server_side_encryption = Some("AES256".to_string());
            }
            // S3 SSE KMS, either using default or custom KMS key. https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
            "kms" => {
                cfg.server_side_encryption = Some("aws:kms".to_string());
                cfg.server_side_encryption_aws_kms_key_id = s3_sse_key;
            }
            // S3 SSE-C, using customer managed keys. https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
            "custom" => {
                cfg.server_side_encryption_customer_algorithm = Some("AES256".to_string());
                cfg.server_side_encryption_customer_key = s3_sse_key;
                cfg.server_side_encryption_customer_key_md5 = m.remove(S3_SSE_MD5);
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Invalid {S3_SSE_TYPE}: {sse_type}. Expected one of (custom, kms, s3, none)"
                    ),
                ));
            }
        }
    };

    if let Some(allow_anonymous) = m.remove(S3_ALLOW_ANONYMOUS)
        && is_truthy(allow_anonymous.to_lowercase().as_str())
    {
        cfg.skip_signature = true;
    }
    if let Some(disable_ec2_metadata) = m.remove(S3_DISABLE_EC2_METADATA)
        && is_truthy(disable_ec2_metadata.to_lowercase().as_str())
    {
        cfg.disable_ec2_metadata = true;
    };
    if let Some(disable_config_load) = m.remove(S3_DISABLE_CONFIG_LOAD)
        && is_truthy(disable_config_load.to_lowercase().as_str())
    {
        cfg.disable_config_load = true;
    };

    Ok(cfg)
}

/// Build new opendal operator from give path.
pub(crate) fn s3_config_build(
    cfg: &S3Config,
    path: &str,
    credentials: Option<&CredentialProvider>,
) -> Result<Operator> {
    let url = Url::parse(path)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid s3 url: {path}, missing bucket"),
        )
    })?;

    let mut cfg = cfg.clone();
    if credentials.is_some() {
        // Delegated authentication must never bypass its provider.
        cfg.skip_signature = false;
        #[allow(deprecated)]
        {
            cfg.allow_anonymous = false;
        }
    }
    let mut builder = cfg
        .into_builder()
        // Set bucket name.
        .bucket(bucket);

    if let Some(provider) = credentials {
        builder = builder.credential_provider_chain(ProvideCredentialChain::new().push(
            VendedS3CredentialProvider(VendedCredentialSource {
                provider: provider.clone(),
                location: path.to_string(),
            }),
        ));
    }

    Operator::new(builder).map_err(from_opendal_error)
}

/// Custom AWS credential loader.
/// This can be used to load credentials from a custom source, such as the AWS SDK.
///
/// This should be set as an extension on `FileIOBuilder`.
#[derive(Clone)]
pub struct CustomAwsCredentialLoader(Arc<dyn AwsCredentialLoad>);

impl std::fmt::Debug for CustomAwsCredentialLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomAwsCredentialLoader")
            .finish_non_exhaustive()
    }
}

impl CustomAwsCredentialLoader {
    /// Create a new custom AWS credential loader.
    pub fn new(loader: Arc<dyn AwsCredentialLoad>) -> Self {
        Self(loader)
    }

    /// Convert this loader into an opendal compatible loader for customized AWS credentials.
    pub fn into_opendal_loader(self) -> Box<dyn AwsCredentialLoad> {
        Box::new(self)
    }
}

#[async_trait]
impl AwsCredentialLoad for CustomAwsCredentialLoader {
    async fn load_credential(&self, client: Client) -> anyhow::Result<Option<AwsCredential>> {
        self.0.load_credential(client).await
    }
}

#[async_trait]
impl crate::io::StorageCredentialProvider for CustomAwsCredentialLoader {
    fn supports_path(&self, path: &str) -> bool {
        Url::parse(path).is_ok_and(|url| matches!(url.scheme(), "s3" | "s3a" | "s3n"))
    }

    async fn load_credential_with_minimum_validity(
        &self,
        _: &str,
        _: std::time::Duration,
    ) -> Result<crate::io::StorageCredential> {
        let credential = self
            .0
            .load_credential(Client::new())
            .await
            .map_err(|_| Error::new(ErrorKind::Unexpected, "Custom AWS credential loader failed"))?
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Custom AWS credentials missing"))?;
        let expiry = credential.expires_in.map(std::time::SystemTime::from);
        let mut credential = crate::io::StorageCredential::new(
            crate::io::StorageCredentialKind::S3(crate::io::S3Credential::new(
                credential.access_key_id,
                credential.secret_access_key,
                credential.session_token,
            )),
        );
        if let Some(expiry) = expiry {
            credential = credential.with_expiration(expiry);
        }
        Ok(credential)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::s3_config_parse;
    use crate::io::S3_PATH_STYLE_ACCESS;

    fn parse_with(prop: Option<&str>) -> bool {
        let mut props = HashMap::new();
        if let Some(v) = prop {
            props.insert(S3_PATH_STYLE_ACCESS.to_string(), v.to_string());
        }
        s3_config_parse(props).unwrap().enable_virtual_host_style
    }

    #[test]
    fn s3_config_parse_path_style_access() {
        // Match Iceberg S3FileIOProperties.PATH_STYLE_ACCESS_DEFAULT = false.
        assert!(!parse_with(None));
        assert!(parse_with(Some("false")));
        assert!(!parse_with(Some("true")));
    }
}
