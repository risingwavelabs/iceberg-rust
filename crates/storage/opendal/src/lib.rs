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

//! OpenDAL-based storage implementation for Apache Iceberg.
//!
//! This crate provides [`OpenDalStorage`] and [`OpenDalStorageFactory`],
//! which implement the [`Storage`](Storage) and
//! [`StorageFactory`](StorageFactory) traits from the `iceberg` crate
//! using [OpenDAL](https://opendal.apache.org/) as the backend.

mod utils;

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use cfg_if::cfg_if;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, IO_CHUNK_SIZE, IO_MAX_RETRIES, IO_RETRY_MAX_DELAY_MS,
    IO_RETRY_MIN_DELAY_MS, IO_TIMEOUT_SECONDS, InputFile, ListEntry, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::layers::RetryLayer;
#[cfg(not(madsim))]
use opendal::layers::TimeoutLayer;
use serde::{Deserialize, Serialize};
use utils::from_opendal_error;

cfg_if! {
    if #[cfg(feature = "opendal-azdls")] {
        mod azdls;
        use azdls::*;
        use opendal::services::AzdlsConfig;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-azblob")] {
        mod azblob;
        use azblob::*;
        use opendal::services::AzblobConfig;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-hf")] {
        mod hf;
        use hf::*;
        use opendal::services::HfConfig;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-fs")] {
        mod fs;
        use fs::*;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-gcs")] {
        mod gcs;
        use gcs::*;
        use opendal::services::GcsConfig;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-memory")] {
        mod memory;
        use memory::*;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-oss")] {
        mod oss;
        use opendal::services::OssConfig;
        use oss::*;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-s3")] {
        mod s3;
        use opendal::services::S3Config;
        pub use s3::*;
    }
}

mod resolving;
pub use resolving::{OpenDalResolvingStorage, OpenDalResolvingStorageFactory};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct OpenDalStorageOptions {
    write_chunk_size: Option<usize>,
    timeout_seconds: Option<u64>,
    max_retries: Option<usize>,
    retry_min_delay_ms: Option<u64>,
    retry_max_delay_ms: Option<u64>,
}

impl TryFrom<&StorageConfig> for OpenDalStorageOptions {
    type Error = Error;

    fn try_from(config: &StorageConfig) -> Result<Self> {
        let options = Self {
            write_chunk_size: parse_config(config, IO_CHUNK_SIZE)?,
            timeout_seconds: parse_config(config, IO_TIMEOUT_SECONDS)?,
            max_retries: parse_config(config, IO_MAX_RETRIES)?,
            retry_min_delay_ms: parse_config(config, IO_RETRY_MIN_DELAY_MS)?,
            retry_max_delay_ms: parse_config(config, IO_RETRY_MAX_DELAY_MS)?,
        };
        if options.write_chunk_size == Some(0) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("{IO_CHUNK_SIZE} must be greater than zero"),
            ));
        }
        if options
            .retry_min_delay_ms
            .zip(options.retry_max_delay_ms)
            .is_some_and(|(min, max)| min > max)
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("{IO_RETRY_MIN_DELAY_MS} must not exceed {IO_RETRY_MAX_DELAY_MS}"),
            ));
        }
        Ok(options)
    }
}

fn parse_config<T>(config: &StorageConfig, key: &str) -> Result<Option<T>>
where T: std::str::FromStr {
    config
        .get(key)
        .map(|value| {
            value.parse::<T>().map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid {key}: '{value}' must be a non-negative integer"),
                )
            })
        })
        .transpose()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ConfiguredOpenDalStorage {
    storage: OpenDalStorage,
    options: OpenDalStorageOptions,
}

impl ConfiguredOpenDalStorage {
    fn new(storage: OpenDalStorage, config: &StorageConfig) -> Result<Self> {
        Ok(Self {
            storage,
            options: OpenDalStorageOptions::try_from(config)?,
        })
    }
}

/// OpenDAL-based storage factory.
///
/// Maps scheme to the corresponding OpenDalStorage storage variant.
/// Use this factory with `FileIOBuilder::new(factory)` to create FileIO instances.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpenDalStorageFactory {
    /// Memory storage factory.
    #[cfg(feature = "opendal-memory")]
    Memory,
    /// Local filesystem storage factory.
    #[cfg(feature = "opendal-fs")]
    Fs,
    /// S3 storage factory.
    #[cfg(feature = "opendal-s3")]
    S3 {
        /// Custom AWS credential loader.
        #[serde(skip)]
        customized_credential_load: Option<CustomAwsCredentialLoader>,
    },
    /// GCS storage factory.
    #[cfg(feature = "opendal-gcs")]
    Gcs,
    /// OSS storage factory.
    #[cfg(feature = "opendal-oss")]
    Oss,
    /// Azure Data Lake Storage factory.
    #[cfg(feature = "opendal-azdls")]
    Azdls,
    /// Azure Blob Storage factory.
    #[cfg(feature = "opendal-azblob")]
    Azblob,
    /// HuggingFace Hub storage factory.
    #[cfg(feature = "opendal-hf")]
    Hf,
}

#[typetag::serde(name = "OpenDalStorageFactory")]
impl StorageFactory for OpenDalStorageFactory {
    #[allow(unused_variables)]
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let storage = match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorageFactory::Memory => OpenDalStorage::Memory(memory_config_build()?),
            #[cfg(feature = "opendal-fs")]
            OpenDalStorageFactory::Fs => OpenDalStorage::LocalFs,
            #[cfg(feature = "opendal-s3")]
            OpenDalStorageFactory::S3 {
                customized_credential_load,
            } => OpenDalStorage::S3 {
                config: s3_config_parse(config.props().clone())?.into(),
                customized_credential_load: customized_credential_load.clone(),
            },
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorageFactory::Gcs => OpenDalStorage::Gcs {
                config: gcs_config_parse(config.props().clone())?.into(),
            },
            #[cfg(feature = "opendal-oss")]
            OpenDalStorageFactory::Oss => OpenDalStorage::Oss {
                config: oss_config_parse(config.props().clone())?.into(),
            },
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorageFactory::Azdls => OpenDalStorage::Azdls {
                config: azdls_config_parse(config.props().clone())?.into(),
            },
            #[cfg(feature = "opendal-azblob")]
            OpenDalStorageFactory::Azblob => OpenDalStorage::Azblob {
                config: azblob_config_parse(config.props().clone()).into(),
            },
            #[cfg(feature = "opendal-hf")]
            OpenDalStorageFactory::Hf => OpenDalStorage::Hf {
                config: hf_config_parse(config.props().clone())?.into(),
            },
            #[cfg(all(
                not(feature = "opendal-memory"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-s3"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
                not(feature = "opendal-azblob"),
                not(feature = "opendal-hf"),
            ))]
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "No storage service has been enabled",
                ));
            }
        };
        Ok(Arc::new(ConfiguredOpenDalStorage::new(storage, config)?))
    }
}

/// Default memory operator for serde deserialization.
#[cfg(feature = "opendal-memory")]
fn default_memory_operator() -> Operator {
    memory_config_build().expect("Failed to create default memory operator")
}

/// OpenDAL-based storage implementation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpenDalStorage {
    /// Memory storage variant.
    #[cfg(feature = "opendal-memory")]
    Memory(#[serde(skip, default = "self::default_memory_operator")] Operator),
    /// Local filesystem storage variant.
    #[cfg(feature = "opendal-fs")]
    LocalFs,
    /// S3 storage variant.
    ///
    /// Accepts any S3-family URL (`s3://`, `s3a://`, `s3n://`); the scheme is
    /// derived from the path at call time.
    #[cfg(feature = "opendal-s3")]
    S3 {
        /// S3 configuration.
        config: Arc<S3Config>,
        /// Custom AWS credential loader.
        #[serde(skip)]
        customized_credential_load: Option<CustomAwsCredentialLoader>,
    },
    /// GCS storage variant.
    #[cfg(feature = "opendal-gcs")]
    Gcs {
        /// GCS configuration.
        config: Arc<GcsConfig>,
    },
    /// OSS storage variant.
    #[cfg(feature = "opendal-oss")]
    Oss {
        /// OSS configuration.
        config: Arc<OssConfig>,
    },
    /// Azure Data Lake Storage variant.
    ///
    /// Accepts paths of the form
    /// `abfs[s]://<filesystem>@<account>.dfs.<endpoint-suffix>/<path>` or
    /// `wasb[s]://<container>@<account>.blob.<endpoint-suffix>/<path>`.
    /// The scheme is derived from the path at call time.
    #[cfg(feature = "opendal-azdls")]
    Azdls {
        /// Azure DLS configuration.
        config: Arc<AzdlsConfig>,
    },
    /// Azure Blob Storage variant.
    #[cfg(feature = "opendal-azblob")]
    Azblob {
        /// Azure Blob Storage configuration.
        config: Arc<AzblobConfig>,
    },
    /// HuggingFace Hub storage variant.
    ///
    /// Accepts paths of the form
    /// `hf://<repo_type>/<owner>/<repo>[@<revision>]/<path_in_repo>`,
    /// where `<repo_type>` must be one of `models`, `datasets`, `spaces`, or `buckets`.
    #[cfg(feature = "opendal-hf")]
    Hf {
        /// HuggingFace Hub configuration (token + endpoint).
        config: Arc<HfConfig>,
    },
}

impl OpenDalStorage {
    /// Creates operator from path.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`](iceberg::io::FileIO).
    ///
    /// # Returns
    ///
    /// The return value consists of two parts:
    ///
    /// * An [`opendal::Operator`] instance used to operate on file.
    /// * Relative path to the root uri of [`opendal::Operator`].
    #[allow(unreachable_code, unused_variables)]
    fn create_operator_with_options<'a>(
        &self,
        path: &'a impl AsRef<str>,
        options: &OpenDalStorageOptions,
    ) -> Result<(Operator, &'a str)> {
        let path = path.as_ref();
        let (operator, relative_path): (Operator, &str) = match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorage::Memory(op) => {
                if let Some(stripped) = path.strip_prefix("memory:/") {
                    (op.clone(), stripped)
                } else {
                    (op.clone(), &path[1..])
                }
            }
            #[cfg(feature = "opendal-fs")]
            OpenDalStorage::LocalFs => {
                let op = fs_config_build()?;
                if let Some(stripped) = path.strip_prefix("file:/") {
                    (op, stripped)
                } else {
                    (op, &path[1..])
                }
            }
            #[cfg(feature = "opendal-s3")]
            OpenDalStorage::S3 {
                config,
                customized_credential_load,
            } => {
                let op = s3_config_build(config, customized_credential_load, path)?;
                let op_info = op.info();

                // Use the URL scheme in the path for prefix matching. This enables
                // use of S3-compatible storage backends using custom schemes (e.g., `minio://`, `r2://`).
                let url = url::Url::parse(path).map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}: {e}"),
                    )
                })?;
                let prefix = format!("{}://{}/", url.scheme(), op_info.name());
                if path.starts_with(&prefix) {
                    (op, &path[prefix.len()..])
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, should start with {prefix}"),
                    ));
                }
            }
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorage::Gcs { config } => {
                let operator = gcs_config_build(config, path)?;
                let prefix = format!("gs://{}/", operator.info().name());
                if path.starts_with(&prefix) {
                    (operator, &path[prefix.len()..])
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, should start with {prefix}"),
                    ));
                }
            }
            #[cfg(feature = "opendal-oss")]
            OpenDalStorage::Oss { config } => {
                let op = oss_config_build(config, path)?;
                let prefix = format!("oss://{}/", op.info().name());
                if path.starts_with(&prefix) {
                    (op, &path[prefix.len()..])
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {path}, should start with {prefix}"),
                    ));
                }
            }
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorage::Azdls { config } => azdls_create_operator(path, config)?,
            #[cfg(feature = "opendal-azblob")]
            OpenDalStorage::Azblob { config } => {
                let operator = azblob_config_build(config, path)?;
                let prefix = format!("azblob://{}/", operator.info().name());
                if path.starts_with(&prefix) {
                    (operator, &path[prefix.len()..])
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid azblob url: {path}, should start with {prefix}"),
                    ));
                }
            }
            #[cfg(feature = "opendal-hf")]
            OpenDalStorage::Hf { config } => hf_config_build(config, path)?,
            #[cfg(all(
                not(feature = "opendal-s3"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
                not(feature = "opendal-azblob"),
                not(feature = "opendal-hf"),
            ))]
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "No storage service has been enabled",
                ));
            }
        };

        // Apply observability/resilience layers. TimeoutLayer must be
        // inside RetryLayer so each retry attempt is independently
        // bounded — without a per-attempt timeout, a future parked on a
        // silently dropped TCP connection never produces an `Err` and
        // RetryLayer cannot retry, leaving the caller hung indefinitely.
        // See: https://opendal.apache.org/docs/rust/opendal/layers/struct.TimeoutLayer.html
        //
        // OpenDAL's TimeoutLayer depends on Tokio's real timer and panics under
        // madsim, so simulated builds retain retries but omit only the timeout.
        #[cfg(not(madsim))]
        let operator = {
            let mut timeout = TimeoutLayer::new();
            if let Some(timeout_seconds) = options.timeout_seconds {
                timeout = timeout.with_io_timeout(Duration::from_secs(timeout_seconds));
            }
            operator.layer(timeout)
        };

        // Transient errors are common for object stores; we retry temporary
        // failures with exponential backoff. The retry behavior also
        // benefits non-object-store backends.
        let mut retry = RetryLayer::new();
        if let Some(max_retries) = options.max_retries {
            retry = retry.with_max_times(max_retries);
        }
        if let Some(min_delay_ms) = options.retry_min_delay_ms {
            retry = retry.with_min_delay(Duration::from_millis(min_delay_ms));
        }
        if let Some(max_delay_ms) = options.retry_max_delay_ms {
            retry = retry.with_max_delay(Duration::from_millis(max_delay_ms));
        }
        let operator = operator.layer(retry);
        Ok((operator, relative_path))
    }

    fn uses_append_mode(&self) -> bool {
        #[cfg(feature = "opendal-azdls")]
        {
            matches!(self, OpenDalStorage::Azdls { .. })
        }
        #[cfg(not(feature = "opendal-azdls"))]
        {
            false
        }
    }

    /// Returns a cache key used by `delete_stream` to group paths by storage operator.
    ///
    /// For most backends the URL host (bucket name) is sufficient. For HF the host
    /// encodes the repo type, not the repo identity, so a more specific key is used.
    fn batch_key_for_path(&self, path: &str) -> String {
        match self {
            #[cfg(feature = "opendal-hf")]
            OpenDalStorage::Hf { .. } => hf_batch_key(path),
            _ => url::Url::parse(path)
                .ok()
                .and_then(|u| u.host_str().map(|s| s.to_string()))
                .unwrap_or_default(),
        }
    }

    /// Extracts the relative path from an absolute path without building an operator.
    ///
    /// This is a lightweight alternative to constructing a full operator when
    /// only the relative path is needed, such as for grouped bulk deletes.
    #[allow(unreachable_code, unused_variables)]
    pub(crate) fn relativize_path<'a>(&self, path: &'a str) -> Result<&'a str> {
        match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorage::Memory(_) => Ok(path.strip_prefix("memory:/").unwrap_or(&path[1..])),
            #[cfg(feature = "opendal-fs")]
            OpenDalStorage::LocalFs => Ok(path.strip_prefix("file:/").unwrap_or(&path[1..])),
            #[cfg(feature = "opendal-s3")]
            OpenDalStorage::S3 { .. } => {
                let url = url::Url::parse(path)?;
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, missing bucket"),
                    )
                })?;
                let prefix = format!("{}://{}/", url.scheme(), bucket);
                if path.starts_with(&prefix) {
                    Ok(&path[prefix.len()..])
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorage::Gcs { .. } => {
                let url = url::Url::parse(path)?;
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, missing bucket"),
                    )
                })?;
                let prefix = format!("gs://{}/", bucket);
                if path.starts_with(&prefix) {
                    Ok(&path[prefix.len()..])
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "opendal-oss")]
            OpenDalStorage::Oss { .. } => {
                let url = url::Url::parse(path)?;
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {path}, missing bucket"),
                    )
                })?;
                let prefix = format!("oss://{}/", bucket);
                if path.starts_with(&prefix) {
                    Ok(&path[prefix.len()..])
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "opendal-azblob")]
            OpenDalStorage::Azblob { .. } => {
                let url = url::Url::parse(path)?;
                let container = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid azblob url: {path}, missing container"),
                    )
                })?;
                let prefix = format!("azblob://{container}/");
                if path.starts_with(&prefix) {
                    Ok(&path[prefix.len()..])
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid azblob url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorage::Azdls { config } => {
                let azure_path = path.parse::<AzureStoragePath>()?;
                match_path_with_config(&azure_path, config)?;
                let relative_path_len = azure_path.path.len();
                Ok(&path[path.len() - relative_path_len..])
            }
            #[cfg(feature = "opendal-hf")]
            OpenDalStorage::Hf { .. } => {
                let parsed = HfUri::parse(path).ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, format!("Invalid hf url: {path}"))
                })?;
                Ok(&path[path.len() - parsed.path.len()..])
            }
            #[cfg(all(
                not(feature = "opendal-s3"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
                not(feature = "opendal-azblob"),
                not(feature = "opendal-hf"),
            ))]
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "No storage service has been enabled",
            )),
        }
    }

    async fn exists_with_options(
        &self,
        path: &str,
        options: &OpenDalStorageOptions,
    ) -> Result<bool> {
        let (operator, relative_path) = self.create_operator_with_options(&path, options)?;
        operator
            .exists(relative_path)
            .await
            .map_err(from_opendal_error)
    }

    async fn metadata_with_options(
        &self,
        path: &str,
        options: &OpenDalStorageOptions,
    ) -> Result<FileMetadata> {
        let (operator, relative_path) = self.create_operator_with_options(&path, options)?;
        let metadata = operator
            .stat(relative_path)
            .await
            .map_err(from_opendal_error)?;
        Ok(FileMetadata {
            size: metadata.content_length(),
        })
    }

    async fn read_with_options(
        &self,
        path: &str,
        options: &OpenDalStorageOptions,
    ) -> Result<Bytes> {
        let (operator, relative_path) = self.create_operator_with_options(&path, options)?;
        Ok(operator
            .read(relative_path)
            .await
            .map_err(from_opendal_error)?
            .to_bytes())
    }

    async fn reader_with_options(
        &self,
        path: &str,
        options: &OpenDalStorageOptions,
    ) -> Result<Box<dyn FileRead>> {
        let (operator, relative_path) = self.create_operator_with_options(&path, options)?;
        Ok(Box::new(OpenDalReader(
            operator
                .reader(relative_path)
                .await
                .map_err(from_opendal_error)?,
        )))
    }

    async fn write_with_options(
        &self,
        path: &str,
        bytes: Bytes,
        options: &OpenDalStorageOptions,
    ) -> Result<()> {
        let mut writer = self.writer_with_options(path, options).await?;
        writer.write(bytes).await?;
        writer.close().await
    }

    async fn writer_with_options(
        &self,
        path: &str,
        options: &OpenDalStorageOptions,
    ) -> Result<Box<dyn FileWrite>> {
        let (operator, relative_path) = self.create_operator_with_options(&path, options)?;
        let mut writer = operator.writer_with(relative_path);
        if self.uses_append_mode() {
            writer = writer.append(true);
        }
        if let Some(chunk_size) = options.write_chunk_size {
            writer = writer.chunk(chunk_size);
        }
        Ok(Box::new(OpenDalWriter(
            writer.await.map_err(from_opendal_error)?,
        )))
    }

    async fn delete_with_options(&self, path: &str, options: &OpenDalStorageOptions) -> Result<()> {
        let (operator, relative_path) = self.create_operator_with_options(&path, options)?;
        operator
            .delete(relative_path)
            .await
            .map_err(from_opendal_error)
    }

    async fn delete_prefix_with_options(
        &self,
        path: &str,
        options: &OpenDalStorageOptions,
    ) -> Result<()> {
        let (operator, relative_path) = self.create_operator_with_options(&path, options)?;
        let path = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        operator
            .delete_with(&path)
            .recursive(true)
            .await
            .map_err(from_opendal_error)
    }

    async fn delete_stream_with_options(
        &self,
        mut paths: BoxStream<'static, String>,
        options: &OpenDalStorageOptions,
    ) -> Result<()> {
        let mut deleters: HashMap<String, opendal::Deleter> = HashMap::new();

        while let Some(path) = paths.next().await {
            let bucket = self.batch_key_for_path(&path);
            let (relative_path, deleter) = match deleters.entry(bucket) {
                Entry::Occupied(entry) => {
                    (self.relativize_path(&path)?.to_string(), entry.into_mut())
                }
                Entry::Vacant(entry) => {
                    let (operator, relative_path) =
                        self.create_operator_with_options(&path, options)?;
                    let relative_path = relative_path.to_string();
                    let deleter = operator.deleter().await.map_err(from_opendal_error)?;
                    (relative_path, entry.insert(deleter))
                }
            };
            deleter
                .delete(relative_path)
                .await
                .map_err(from_opendal_error)?;
        }

        for (_, mut deleter) in deleters {
            deleter.close().await.map_err(from_opendal_error)?;
        }
        Ok(())
    }

    async fn list_with_options(
        &self,
        path: &str,
        recursive: bool,
        options: &OpenDalStorageOptions,
    ) -> Result<BoxStream<'static, Result<ListEntry>>> {
        let path: Arc<str> = Arc::from(path);
        let (operator, relative_path) = self.create_operator_with_options(&path, options)?;
        let absolute_prefix: Arc<str> =
            Arc::from(&path[..path.len().saturating_sub(relative_path.len())]);
        let list_path = if relative_path.is_empty() || relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        let lister = operator
            .lister_with(&list_path)
            .recursive(recursive)
            .await
            .map_err(from_opendal_error)?;

        Ok(lister
            .map(move |entry| {
                entry.map_err(from_opendal_error).map(|entry| {
                    let metadata = entry.metadata();
                    let last_modified_ms = metadata
                        .last_modified()
                        .and_then(|timestamp| {
                            let modified: SystemTime = timestamp.into();
                            modified.duration_since(UNIX_EPOCH).ok()
                        })
                        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX));
                    ListEntry {
                        path: format!("{absolute_prefix}{}", entry.path()),
                        size: metadata.content_length(),
                        last_modified_ms,
                        is_dir: metadata.is_dir(),
                    }
                })
            })
            .boxed())
    }
}

#[typetag::serde(name = "OpenDalStorage")]
#[async_trait]
impl Storage for OpenDalStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.exists_with_options(path, &OpenDalStorageOptions::default())
            .await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.metadata_with_options(path, &OpenDalStorageOptions::default())
            .await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.read_with_options(path, &OpenDalStorageOptions::default())
            .await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.reader_with_options(path, &OpenDalStorageOptions::default())
            .await
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        self.write_with_options(path, bs, &OpenDalStorageOptions::default())
            .await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.writer_with_options(path, &OpenDalStorageOptions::default())
            .await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.delete_with_options(path, &OpenDalStorageOptions::default())
            .await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.delete_prefix_with_options(path, &OpenDalStorageOptions::default())
            .await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        self.delete_stream_with_options(paths, &OpenDalStorageOptions::default())
            .await
    }

    async fn list(
        &self,
        path: &str,
        recursive: bool,
    ) -> Result<BoxStream<'static, Result<ListEntry>>> {
        self.list_with_options(path, recursive, &OpenDalStorageOptions::default())
            .await
    }

    #[allow(unreachable_code, unused_variables)]
    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    #[allow(unreachable_code, unused_variables)]
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

#[typetag::serde(name = "ConfiguredOpenDalStorage")]
#[async_trait]
impl Storage for ConfiguredOpenDalStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.storage.exists_with_options(path, &self.options).await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.storage
            .metadata_with_options(path, &self.options)
            .await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.storage.read_with_options(path, &self.options).await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.storage.reader_with_options(path, &self.options).await
    }

    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        self.storage
            .write_with_options(path, bytes, &self.options)
            .await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.storage.writer_with_options(path, &self.options).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.storage.delete_with_options(path, &self.options).await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.storage
            .delete_prefix_with_options(path, &self.options)
            .await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        self.storage
            .delete_stream_with_options(paths, &self.options)
            .await
    }

    async fn list(
        &self,
        path: &str,
        recursive: bool,
    ) -> Result<BoxStream<'static, Result<ListEntry>>> {
        self.storage
            .list_with_options(path, recursive, &self.options)
            .await
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

// Newtype wrappers for opendal types to satisfy orphan rules.
// We can't implement iceberg's FileRead/FileWrite traits directly on opendal's
// Reader/Writer since neither trait nor type is defined in this crate.

/// Wrapper around `opendal::Reader` that implements `FileRead`.
pub(crate) struct OpenDalReader(pub(crate) opendal::Reader);

#[async_trait]
impl FileRead for OpenDalReader {
    async fn read(&self, range: std::ops::Range<u64>) -> Result<Bytes> {
        Ok(opendal::Reader::read(&self.0, range)
            .await
            .map_err(from_opendal_error)?
            .to_bytes())
    }
}

/// Wrapper around `opendal::Writer` that implements `FileWrite`.
pub(crate) struct OpenDalWriter(pub(crate) opendal::Writer);

#[async_trait]
impl FileWrite for OpenDalWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        Ok(opendal::Writer::write(&mut self.0, bs)
            .await
            .map_err(from_opendal_error)?)
    }

    async fn close(&mut self) -> Result<()> {
        let _ = opendal::Writer::close(&mut self.0)
            .await
            .map_err(from_opendal_error)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "opendal-memory")]
    #[test]
    fn test_default_memory_operator() {
        let op = default_memory_operator();
        assert_eq!(op.info().scheme().to_string(), "memory");
    }

    #[cfg(feature = "opendal-memory")]
    #[test]
    fn test_relativize_path_memory() {
        let storage = OpenDalStorage::Memory(default_memory_operator());

        assert_eq!(
            storage.relativize_path("memory:/path/to/file").unwrap(),
            "path/to/file"
        );
        // Without the scheme prefix, falls back to stripping the leading slash
        assert_eq!(
            storage.relativize_path("/path/to/file").unwrap(),
            "path/to/file"
        );
    }

    #[cfg(feature = "opendal-fs")]
    #[test]
    fn test_relativize_path_fs() {
        let storage = OpenDalStorage::LocalFs;

        assert_eq!(
            storage
                .relativize_path("file:/tmp/data/file.parquet")
                .unwrap(),
            "tmp/data/file.parquet"
        );
        assert_eq!(
            storage.relativize_path("/tmp/data/file.parquet").unwrap(),
            "tmp/data/file.parquet"
        );
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_relativize_path_s3() {
        let storage = OpenDalStorage::S3 {
            config: Arc::new(S3Config::default()),
            customized_credential_load: None,
        };

        // All S3-family schemes are accepted by the same storage instance.
        // Custom schemes for S3-compatible stores (e.g., `minio://`) are also
        // accepted because the path's scheme is used as-is for prefix matching.
        for scheme in ["s3", "s3a", "s3n", "minio"] {
            assert_eq!(
                storage
                    .relativize_path(&format!("{scheme}://my-bucket/path/to/file.parquet"))
                    .unwrap(),
                "path/to/file.parquet"
            );
        }
    }

    #[cfg(feature = "opendal-gcs")]
    #[test]
    fn test_relativize_path_gcs() {
        let storage = OpenDalStorage::Gcs {
            config: Arc::new(GcsConfig::default()),
        };

        assert_eq!(
            storage
                .relativize_path("gs://my-bucket/path/to/file.parquet")
                .unwrap(),
            "path/to/file.parquet"
        );
    }

    #[cfg(feature = "opendal-gcs")]
    #[test]
    fn test_relativize_path_gcs_invalid_scheme() {
        let storage = OpenDalStorage::Gcs {
            config: Arc::new(GcsConfig::default()),
        };

        assert!(
            storage
                .relativize_path("s3://my-bucket/path/to/file.parquet")
                .is_err()
        );
    }

    #[cfg(feature = "opendal-oss")]
    #[test]
    fn test_relativize_path_oss() {
        let storage = OpenDalStorage::Oss {
            config: Arc::new(OssConfig::default()),
        };

        assert_eq!(
            storage
                .relativize_path("oss://my-bucket/path/to/file.parquet")
                .unwrap(),
            "path/to/file.parquet"
        );
    }

    #[cfg(feature = "opendal-oss")]
    #[test]
    fn test_relativize_path_oss_invalid_scheme() {
        let storage = OpenDalStorage::Oss {
            config: Arc::new(OssConfig::default()),
        };

        assert!(
            storage
                .relativize_path("s3://my-bucket/path/to/file.parquet")
                .is_err()
        );
    }

    #[cfg(feature = "opendal-azdls")]
    #[test]
    fn test_relativize_path_azdls() {
        let storage = OpenDalStorage::Azdls {
            config: Arc::new(AzdlsConfig {
                account_name: Some("myaccount".to_string()),
                endpoint: Some("https://myaccount.dfs.core.windows.net".to_string()),
                ..Default::default()
            }),
        };

        assert_eq!(
            storage
                .relativize_path("abfss://myfs@myaccount.dfs.core.windows.net/path/to/file.parquet")
                .unwrap(),
            "/path/to/file.parquet"
        );
        assert!(storage.uses_append_mode());
    }

    #[cfg(feature = "opendal-azblob")]
    #[test]
    fn test_relativize_path_azblob() {
        let storage = OpenDalStorage::Azblob {
            config: Arc::new(AzblobConfig::default()),
        };
        assert_eq!(
            storage
                .relativize_path("azblob://container/path/to/file.parquet")
                .unwrap(),
            "path/to/file.parquet"
        );
    }

    #[cfg(feature = "opendal-memory")]
    #[tokio::test]
    async fn test_list_memory_storage() {
        use futures::TryStreamExt;

        let storage = OpenDalStorage::Memory(default_memory_operator());
        storage
            .write("memory:/root/direct.txt", Bytes::from_static(b"a"))
            .await
            .unwrap();
        storage
            .write(
                "memory:/root/nested/child.txt",
                Bytes::from_static(b"child"),
            )
            .await
            .unwrap();

        let entries: Vec<_> = storage
            .list("memory:/root", true)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(
            entries
                .iter()
                .any(|entry| entry.path == "memory:/root/direct.txt" && entry.size == 1)
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.path == "memory:/root/nested/child.txt" && entry.size == 5)
        );
    }

    #[cfg(feature = "opendal-memory")]
    #[tokio::test]
    async fn test_configured_storage_options_and_write() {
        let config = StorageConfig::new()
            .with_prop(IO_CHUNK_SIZE, "1024")
            .with_prop(IO_TIMEOUT_SECONDS, "30")
            .with_prop(IO_MAX_RETRIES, "5")
            .with_prop(IO_RETRY_MIN_DELAY_MS, "10")
            .with_prop(IO_RETRY_MAX_DELAY_MS, "100");
        let storage = ConfiguredOpenDalStorage::new(
            OpenDalStorage::Memory(default_memory_operator()),
            &config,
        )
        .unwrap();

        assert_eq!(storage.options.write_chunk_size, Some(1024));
        assert_eq!(storage.options.timeout_seconds, Some(30));
        assert_eq!(storage.options.max_retries, Some(5));
        assert_eq!(storage.options.retry_min_delay_ms, Some(10));
        assert_eq!(storage.options.retry_max_delay_ms, Some(100));

        storage
            .write("memory:/configured.txt", Bytes::from_static(b"configured"))
            .await
            .unwrap();
        assert_eq!(
            storage.read("memory:/configured.txt").await.unwrap(),
            Bytes::from_static(b"configured")
        );
    }

    #[test]
    fn test_invalid_storage_options() {
        let invalid_integer = StorageConfig::new().with_prop(IO_TIMEOUT_SECONDS, "not-a-number");
        assert!(OpenDalStorageOptions::try_from(&invalid_integer).is_err());

        let invalid_delays = StorageConfig::new()
            .with_prop(IO_RETRY_MIN_DELAY_MS, "101")
            .with_prop(IO_RETRY_MAX_DELAY_MS, "100");
        assert!(OpenDalStorageOptions::try_from(&invalid_delays).is_err());

        let zero_chunk = StorageConfig::new().with_prop(IO_CHUNK_SIZE, "0");
        assert!(OpenDalStorageOptions::try_from(&zero_chunk).is_err());
    }
}
