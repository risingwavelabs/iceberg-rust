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

//! Table-scoped credential vending. Metadata snapshots are never refreshed here.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use iceberg::io::{
    ADLS_SAS_TOKEN, AzdlsCredential, S3Credential, StorageCredential as IoStorageCredential,
    StorageCredentialKind, StorageCredentialProvider, storage_prefix_covers,
};
use iceberg::{Error, ErrorKind, Result};
use reqwest::{Method, StatusCode, Url};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::client::HttpClient;
use crate::types::{LoadTableResult, StorageCredential};

const LEASE: Duration = Duration::from_secs(300);
const RETRY_DELAY: Duration = Duration::from_secs(1);

fn invalid(message: &'static str) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

/// Reject URL aliasing and compare directory boundaries, not host suffixes.
fn matches_prefix(prefix: &Url, location: &Url) -> bool {
    storage_prefix_covers(prefix.as_str(), location.as_str())
}

// These types deliberately have no Debug implementation: they contain secrets.
struct ParsedCredential {
    kind: StorageCredentialKind,
    expiry: Option<SystemTime>,
}

type ParsedResult = std::result::Result<ParsedCredential, &'static str>;

struct ParsedProperties {
    adls: HashMap<String, ParsedResult>,
    adls_default: ParsedResult,
    s3: ParsedResult,
}

fn parse_expiry(
    value: &str,
    message: &'static str,
) -> std::result::Result<SystemTime, &'static str> {
    let millis = value.parse::<u64>().map_err(|_| message)?;
    UNIX_EPOCH
        .checked_add(Duration::from_millis(millis))
        .ok_or(message)
}

impl ParsedProperties {
    fn new(properties: HashMap<String, String>) -> Self {
        let mut adls = HashMap::new();
        for host in properties.keys().filter_map(|key| {
            key.strip_prefix("adls.sas-token.")
                .or_else(|| key.strip_prefix("adls.sas-token-expires-at-ms."))
        }) {
            adls.entry(host.to_string())
                .or_insert_with(|| Self::adls(&properties, Some(host)));
        }
        Self {
            adls,
            adls_default: Self::adls(&properties, None),
            s3: Self::s3(&properties),
        }
    }

    fn adls(properties: &HashMap<String, String>, host: Option<&str>) -> ParsedResult {
        let token = host
            .and_then(|host| properties.get(&format!("{ADLS_SAS_TOKEN}.{host}")))
            .or_else(|| properties.get(ADLS_SAS_TOKEN))
            .filter(|value| !value.trim_start_matches('?').is_empty())
            .ok_or("No ADLS SAS credential matches the file account and prefix")?
            .trim_start_matches('?')
            .to_string();
        let parameters: HashMap<_, _> = url::form_urlencoded::parse(token.as_bytes()).collect();
        let mut expiry = parameters
            .get("se")
            .map(|se| {
                chrono::DateTime::parse_from_rfc3339(se)
                    .map(SystemTime::from)
                    .map_err(|_| "Invalid ADLS SAS expiry")
            })
            .transpose()?;
        let explicit = host
            .and_then(|host| properties.get(&format!("adls.sas-token-expires-at-ms.{host}")))
            .or_else(|| properties.get("adls.sas-token-expires-at-ms"));
        if let Some(value) = explicit {
            let time = parse_expiry(value, "Invalid ADLS credential expiry")?;
            expiry = Some(expiry.map_or(time, |old| old.min(time)));
        }
        Ok(ParsedCredential {
            kind: StorageCredentialKind::Azdls(AzdlsCredential::new(token)),
            expiry,
        })
    }

    fn s3(properties: &HashMap<String, String>) -> ParsedResult {
        for key in ["s3.access-key-id", "s3.secret-access-key"] {
            if properties.get(key).is_none_or(|value| value.is_empty()) {
                return Err("Incomplete vended S3 credential");
            }
        }
        let expiry = properties
            .get("s3.session-token-expires-at-ms")
            .map(|value| parse_expiry(value, "Invalid S3 credential expiry"))
            .transpose()?;
        Ok(ParsedCredential {
            kind: StorageCredentialKind::S3(S3Credential::new(
                &properties["s3.access-key-id"],
                &properties["s3.secret-access-key"],
                properties.get("s3.session-token").cloned(),
            )),
            expiry,
        })
    }

    fn select(&self, location: &Url) -> Result<&ParsedCredential> {
        let credential = match location.scheme() {
            "abfs" | "abfss" | "wasb" | "wasbs" => {
                let host = location
                    .host_str()
                    .ok_or_else(|| invalid("ADLS location has no account"))?;
                self.adls.get(host).unwrap_or(&self.adls_default)
            }
            "s3" | "s3a" | "s3n" => &self.s3,
            _ => return Err(invalid("Unsupported vended credential backend")),
        };
        credential.as_ref().map_err(|message| invalid(message))
    }
}

struct ScopedCredential {
    prefix: Url,
    prefix_len: usize,
    properties: ParsedProperties,
}

pub(crate) struct CredentialSet {
    config: ParsedProperties,
    entries: Vec<ScopedCredential>,
    issued_at: SystemTime,
}

impl CredentialSet {
    pub(crate) fn new(
        config: HashMap<String, String>,
        entries: Option<Vec<StorageCredential>>,
    ) -> Self {
        Self {
            issued_at: SystemTime::now(),
            config: ParsedProperties::new(config),
            entries: entries
                .unwrap_or_default()
                .into_iter()
                .filter_map(|entry| {
                    Some(ScopedCredential {
                        prefix: Url::parse(&entry.prefix).ok()?,
                        prefix_len: entry.prefix.len(),
                        properties: ParsedProperties::new(entry.config),
                    })
                })
                .collect(),
        }
    }

    fn matched_entry(&self, location: &Url) -> Option<&ScopedCredential> {
        self.entries
            .iter()
            .filter(|entry| matches_prefix(&entry.prefix, location))
            .max_by_key(|entry| entry.prefix_len)
    }

    fn select(&self, location: &Url) -> Result<&ParsedCredential> {
        self.matched_entry(location)
            .map(|entry| &entry.properties)
            .unwrap_or(&self.config)
            .select(location)
    }

    fn load_credential(
        &self,
        location: &Url,
        now: SystemTime,
        fresh: bool,
    ) -> Result<IoStorageCredential> {
        let credential = self.select(location)?;
        let prefix = self
            .matched_entry(location)
            .map(|entry| entry.prefix.to_string())
            .unwrap_or_else(|| {
                let mut root = location.clone();
                root.set_path("/");
                root.to_string()
            });
        let deadline = credential.expiry.unwrap_or(self.issued_at + LEASE);
        let lifetime = deadline.duration_since(self.issued_at).unwrap_or_default();
        let margin = (lifetime / 5).min(Duration::from_secs(30));
        if now >= deadline || (fresh && now + margin >= deadline) {
            return Err(invalid("Vended storage credential requires refresh"));
        }
        // Keep leases below reqsign's cache freshness windows (ADLS: 20s,
        // AWS: 120s), but above AWS's 10s signing-operation headroom.
        // Every request consults this manager, even on an already-open handle.
        let lease = if matches!(location.scheme(), "s3" | "s3a" | "s3n") {
            Duration::from_secs(30)
        } else {
            Duration::from_secs(5)
        };
        Ok(IoStorageCredential::new(credential.kind.clone())
            .with_prefix(prefix)
            .with_expiration(deadline.min(now + lease)))
    }
}

struct State {
    set: CredentialSet,
    retry_after: SystemTime,
    endpoint: Option<bool>,
    revoked: bool,
    last_refresh_status: Option<u16>,
    prefix_only: bool,
    config_retry_after: SystemTime,
}

pub(crate) struct RestVendedCredentialProvider {
    client: Arc<HttpClient>,
    table_url: String,
    state: Mutex<State>,
}

impl std::fmt::Debug for RestVendedCredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestVendedCredentialProvider")
            .finish_non_exhaustive()
    }
}

impl RestVendedCredentialProvider {
    /// Feed newer load/commit credentials to all existing handles.
    pub(crate) async fn update(&self, set: CredentialSet) {
        let mut state = self.state.lock().await;
        state.set = set;
        state.retry_after = UNIX_EPOCH;
        state.revoked = false;
        state.last_refresh_status = None;
        state.prefix_only = false;
        state.config_retry_after = UNIX_EPOCH;
    }

    pub(crate) fn new(
        client: Arc<HttpClient>,
        table_url: String,
        set: CredentialSet,
        endpoint: Option<bool>,
    ) -> Self {
        Self {
            client,
            table_url,
            state: Mutex::new(State {
                set,
                endpoint,
                retry_after: UNIX_EPOCH,
                revoked: false,
                last_refresh_status: None,
                prefix_only: false,
                config_retry_after: UNIX_EPOCH,
            }),
        }
    }

    async fn fetch(&self, state: &mut State, location: &Url) -> Result<CredentialSet> {
        // A recent /credentials response already refreshed the prefix entries.
        // An uncovered location needs only loadTable's missing flat config.
        let needs_config =
            !state.revoked && state.prefix_only && state.set.matched_entry(location).is_none();
        if state.endpoint != Some(false) && !needs_config {
            let response = self
                .request(format!("{}/credentials", self.table_url), state)
                .await?;
            match response.status() {
                StatusCode::OK => {
                    #[derive(Deserialize)]
                    struct Response {
                        #[serde(rename = "storage-credentials")]
                        credentials: Vec<StorageCredential>,
                    }
                    let response: Response = response
                        .json()
                        .await
                        .map_err(|_| invalid("Invalid REST storage credential response"))?;
                    state.endpoint = Some(true);
                    let set = CredentialSet::new(HashMap::new(), Some(response.credentials));
                    state.prefix_only = true;
                    state.config_retry_after = UNIX_EPOCH;
                    if set.matched_entry(location).is_some() {
                        // A matched but malformed/expired entry must not fall
                        // back to a broader loadTable config credential.
                        return Ok(set);
                    }
                    // /credentials contains no flat config. Publish the new
                    // scopes before awaiting loadTable so a failure or cancelled
                    // fallback cannot resurrect credentials the catalog removed.
                    state.set = set;
                    state.revoked = false;
                }
                StatusCode::NOT_FOUND
                | StatusCode::METHOD_NOT_ALLOWED
                | StatusCode::NOT_IMPLEMENTED
                    if state.endpoint.is_none() =>
                {
                    // Legacy server: confirm access with loadTable. Never fall
                    // back to local credentials based on a 404 alone.
                    // Persist the pending confirmation before awaiting loadTable,
                    // including if that request fails, times out, or is cancelled.
                    state.revoked |= response.status() == StatusCode::NOT_FOUND;
                    state.endpoint = Some(false);
                }
                status => return Err(Self::response_error(state, status)),
            }
        }
        state.config_retry_after = SystemTime::now() + RETRY_DELAY;
        let response = self.request(self.table_url.clone(), state).await?;
        if response.status() != StatusCode::OK {
            return Err(Self::response_error(state, response.status()));
        }
        let response: LoadTableResult = response
            .json()
            .await
            .map_err(|_| invalid("Invalid REST table credential response"))?;
        state.prefix_only = false;
        state.config_retry_after = UNIX_EPOCH;
        Ok(CredentialSet::new(
            response.config,
            response.storage_credentials,
        ))
    }

    async fn request(&self, url: String, state: &mut State) -> Result<reqwest::Response> {
        let request = self
            .client
            .request(Method::GET, url)
            .header("X-Iceberg-Access-Delegation", "vended-credentials")
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| invalid("Unable to build credential refresh request"))?;
        tokio::time::timeout(
            Duration::from_secs(10),
            self.client.query_credentials(request, &mut state.revoked),
        )
        .await
        .map_err(|_| {
            Error::new(ErrorKind::Unexpected, "REST credential refresh timed out")
                .with_context("credential_error", "refresh_timeout")
                .with_retryable(true)
        })?
        .map_err(|_| {
            Error::new(
                ErrorKind::Unexpected,
                "REST storage credential refresh failed",
            )
            .with_context("credential_error", "refresh_failed")
            .with_retryable(!state.revoked)
        })
    }

    fn response_error(state: &mut State, status: StatusCode) -> Error {
        state.last_refresh_status = Some(status.as_u16());
        // Don't expose response bodies, which may contain credential material.
        if matches!(
            status,
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::NOT_FOUND
        ) {
            state.revoked = true;
        }
        Error::new(
            ErrorKind::Unexpected,
            "REST storage credential refresh rejected",
        )
        .with_context("status", status.as_u16().to_string())
        .with_context("credential_error", "refresh_rejected")
        .with_retryable(status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS)
    }
}

#[async_trait]
impl StorageCredentialProvider for RestVendedCredentialProvider {
    fn supports_path(&self, path: &str) -> bool {
        Url::parse(path).is_ok_and(|location| {
            matches!(
                location.scheme(),
                "s3" | "s3a" | "s3n" | "abfs" | "abfss" | "wasb" | "wasbs"
            )
        })
    }

    async fn load_credential(&self, location: &str) -> Result<IoStorageCredential> {
        let location = Url::parse(location).map_err(|_| invalid("Invalid credential location"))?;
        if location.query().is_some()
            || location.fragment().is_some()
            || location.password().is_some()
        {
            return Err(invalid(
                "Credential location must not contain a query, fragment or password",
            ));
        }

        // Holding this lock across refresh provides single-flight and cancellation
        // safety; a dropped future releases the lock without publishing partial data.
        let mut state = self.state.lock().await;
        let now = SystemTime::now();
        if !state.revoked
            && let Ok(credential) = state.set.load_credential(&location, now, true)
        {
            return Ok(credential);
        }
        let config_refresh_ready = !state.revoked
            && state.prefix_only
            && state.set.matched_entry(&location).is_none()
            && now >= state.config_retry_after;
        if now < state.retry_after && !config_refresh_ready {
            let result = if state.revoked {
                Err(invalid("Vended storage access was revoked")
                    .with_context("credential_error", "revoked"))
            } else {
                state.set.load_credential(&location, now, false)
            };
            return result.map_err(|error| {
                if let Some(status) = state.last_refresh_status {
                    error
                        .with_context("status", status.to_string())
                        .with_context("credential_error", "refresh_backoff")
                        .with_retryable(status >= 500 || status == 429)
                } else {
                    error
                }
            });
        }
        state.retry_after = now + RETRY_DELAY;
        state.last_refresh_status = None;
        let refreshed = self.fetch(&mut state, &location).await;
        state.retry_after = SystemTime::now() + RETRY_DELAY;
        if state.config_retry_after != UNIX_EPOCH {
            state.config_retry_after = state.retry_after;
        }
        match refreshed {
            Ok(set) => {
                // Even an empty/narrower successful response replaces the old
                // authorization. Never resurrect an old prefix during backoff.
                state.set = set;
                state.revoked = false;
                state
                    .set
                    .load_credential(&location, SystemTime::now(), false)
            }
            Err(error) => {
                if !state.revoked
                    && let Ok(credential) =
                        state
                            .set
                            .load_credential(&location, SystemTime::now(), false)
                {
                    return Ok(credential);
                }
                Err(error)
            }
        }
    }
}

#[cfg(test)]
pub(crate) trait TestCredentialExt {
    fn test_sas_token(&self) -> &str;
}

#[cfg(test)]
impl TestCredentialExt for IoStorageCredential {
    fn test_sas_token(&self) -> &str {
        let StorageCredentialKind::Azdls(credential) = self.kind() else {
            panic!("expected ADLS credential");
        };
        credential.sas_token()
    }
}

#[cfg(test)]
impl TestCredentialExt for ParsedCredential {
    fn test_sas_token(&self) -> &str {
        let StorageCredentialKind::Azdls(credential) = &self.kind else {
            panic!("expected ADLS credential");
        };
        credential.sas_token()
    }
}

#[cfg(test)]
mod tests {
    use iceberg::io::{ADLS_ACCOUNT_KEY, ADLS_ENDPOINT, FileIO, FileIOBuilder, StorageFactory};
    use iceberg_storage_opendal::{OpenDalResolvingStorageFactory, OpenDalStorageFactory};
    use mockito::{Matcher, Server};
    use serde_json::json;

    use super::*;
    use crate::catalog::RestCatalogConfig;

    const LOCATION: &str = "abfss://fs@acct.dfs.core.windows.net/table/data/file";
    const ROOT: &str = "abfss://fs@acct.dfs.core.windows.net/table/";
    const KEY: &str = "adls.sas-token.acct.dfs.core.windows.net";

    fn config(token: &str) -> HashMap<String, String> {
        HashMap::from([(KEY.to_string(), token.to_string())])
    }

    fn provider(
        server: &mockito::ServerGuard,
        set: CredentialSet,
        endpoint: Option<bool>,
    ) -> Arc<RestVendedCredentialProvider> {
        let cfg = RestCatalogConfig::builder().uri(server.url()).build();
        Arc::new(RestVendedCredentialProvider::new(
            Arc::new(HttpClient::new(&cfg).unwrap()),
            format!("{}/table", server.url()),
            set,
            endpoint,
        ))
    }

    fn adls_factories() -> [Arc<dyn StorageFactory>; 2] {
        [
            Arc::new(OpenDalStorageFactory::azdls()),
            Arc::new(OpenDalResolvingStorageFactory::new()),
        ]
    }

    fn adls_file_io(
        factory: Arc<dyn StorageFactory>,
        server: &mockito::ServerGuard,
        provider: Arc<RestVendedCredentialProvider>,
    ) -> FileIO {
        FileIOBuilder::new(factory)
            // Keep endpoint-suffix validation while routing directly to loopback.
            .with_prop(ADLS_ENDPOINT, format!("{}/core.windows.net", server.url()))
            // Neither static authentication mode may override the REST provider.
            .with_prop(ADLS_SAS_TOKEN, "sig=static")
            .with_prop(ADLS_ACCOUNT_KEY, "ZHVtbXktc3RhdGljLWtleQ==")
            .with_prop("io.max-retries", "0")
            .with_prop("io.timeout", "3")
            .with_prop("io.write.chunk-size", "4")
            .with_credential_provider(provider)
            .build()
    }

    #[tokio::test]
    async fn adls_open_handles_use_refreshed_rest_credentials_on_http_requests() {
        for factory in adls_factories() {
            let mut catalog = Server::new_async().await;
            let mut storage = Server::new_async().await;
            let root = ROOT.replace("abfss:", "abfs:");
            let provider = provider(
                &catalog,
                CredentialSet::new(
                    HashMap::from([(ADLS_SAS_TOKEN.into(), "sig=0".into())]),
                    None,
                ),
                Some(true),
            );
            let io = adls_file_io(factory, &storage, provider.clone());
            let reader = io
                .new_input(format!("{root}data/file"))
                .unwrap()
                .reader()
                .await
                .unwrap();
            let mut writer = io
                .new_output(format!("{root}data/output"))
                .unwrap()
                .writer()
                .await
                .unwrap();
            let head = storage
                .mock("HEAD", "/core.windows.net/fs/table/data/output")
                .match_query(Matcher::UrlEncoded("sig".into(), "0".into()))
                .match_header("authorization", Matcher::Missing)
                .with_header("content-length", "0")
                .expect(1)
                .create_async()
                .await;
            let create = storage
                .mock("PUT", "/core.windows.net/fs/table/data/output")
                .match_query(Matcher::AllOf(vec![
                    Matcher::UrlEncoded("resource".into(), "file".into()),
                    Matcher::UrlEncoded("sig".into(), "0".into()),
                ]))
                .match_header("authorization", Matcher::Missing)
                .with_status(201)
                .expect(1)
                .create_async()
                .await;

            for generation in 0..3 {
                let refresh = if generation > 0 {
                    // Expire the manager's lease deterministically, without sleeps.
                    let mut state = provider.state.lock().await;
                    state.set.issued_at = SystemTime::now() - LEASE - Duration::from_secs(1);
                    state.retry_after = UNIX_EPOCH;
                    drop(state);
                    Some(catalog.mock("GET", "/table/credentials")
                        .match_header("X-Iceberg-Access-Delegation", "vended-credentials")
                        .with_body(json!({"storage-credentials": [
                            {"prefix": format!("{root}metadata/"), "config": config("sig=metadata")},
                            {"prefix": format!("{root}data/"), "config": config(&format!("?sig={generation}"))}
                        ]}).to_string())
                        .expect(1).create_async().await)
                } else {
                    None
                };
                let read = storage
                    .mock("GET", "/core.windows.net/fs/table/data/file")
                    .match_query(Matcher::UrlEncoded("sig".into(), generation.to_string()))
                    .match_header("authorization", Matcher::Missing)
                    .match_header("range", "bytes=0-3")
                    .with_status(206)
                    .with_header("content-range", "bytes 0-3/4")
                    .with_body("data")
                    .expect(1)
                    .create_async()
                    .await;
                let append = storage
                    .mock("PATCH", "/core.windows.net/fs/table/data/output")
                    .match_query(Matcher::AllOf(vec![
                        Matcher::UrlEncoded("action".into(), "append".into()),
                        Matcher::UrlEncoded("position".into(), (generation * 4).to_string()),
                        Matcher::UrlEncoded("flush".into(), "true".into()),
                        Matcher::UrlEncoded("sig".into(), generation.to_string()),
                    ]))
                    .match_header("authorization", Matcher::Missing)
                    .match_body("data")
                    .with_status(202)
                    .expect(1)
                    .create_async()
                    .await;
                // Let the writer trigger one refresh, and the reader the next.
                // Keep one byte buffered so each call sends a complete chunk.
                let bytes = if generation == 0 {
                    b"datad".as_slice()
                } else {
                    b"atad".as_slice()
                };
                if generation == 1 {
                    writer.write(bytes.into()).await.unwrap();
                }
                assert_eq!(reader.read(0..4).await.unwrap().as_ref(), b"data");
                if generation != 1 {
                    writer.write(bytes.into()).await.unwrap();
                }
                read.assert_async().await;
                append.assert_async().await;
                if let Some(refresh) = refresh {
                    refresh.assert_async().await;
                    refresh.remove_async().await;
                }
            }
            let tail = storage
                .mock("PATCH", "/core.windows.net/fs/table/data/output")
                .match_query(Matcher::AllOf(vec![
                    Matcher::UrlEncoded("action".into(), "append".into()),
                    Matcher::UrlEncoded("position".into(), "12".into()),
                    Matcher::UrlEncoded("flush".into(), "true".into()),
                    Matcher::UrlEncoded("sig".into(), "2".into()),
                ]))
                .match_header("authorization", Matcher::Missing)
                .match_body("d")
                .with_status(202)
                .expect(1)
                .create_async()
                .await;
            writer.close().await.unwrap();
            tail.assert_async().await;
            head.assert_async().await;
            create.assert_async().await;
        }
    }

    #[tokio::test]
    async fn adls_rest_refresh_failures_never_send_static_or_unsigned_requests() {
        for factory in adls_factories() {
            for status in [200, 403, 503] {
                let mut catalog = Server::new_async().await;
                let mut storage = Server::new_async().await;
                let refresh = catalog
                    .mock("GET", "/table/credentials")
                    .with_status(status)
                    .with_body(r#"{"storage-credentials":[]}"#)
                    .expect(1)
                    .create_async()
                    .await;
                let provider = provider(
                    &catalog,
                    CredentialSet::new(config("sig=expired&se=2000-01-01T00:00:00Z"), None),
                    Some(true),
                );
                let io = adls_file_io(factory.clone(), &storage, provider);
                let mut requests = Vec::new();
                for method in ["GET", "HEAD", "PUT", "PATCH"] {
                    requests.push(
                        storage
                            .mock(method, Matcher::Any)
                            .with_body("must not be reached")
                            .expect(0)
                            .create_async()
                            .await,
                    );
                }
                let location = LOCATION.replace("abfss:", "abfs:");
                let reader = io.new_input(&location).unwrap().reader().await.unwrap();
                let mut writer = io.new_output(&location).unwrap().writer().await.unwrap();
                let error = reader.read(0..4).await.unwrap_err();
                let diagnostic = format!("{error:?}");
                assert!(
                    diagnostic.contains("failure_stage: credential"),
                    "{diagnostic}"
                );
                if status != 200 {
                    assert!(
                        diagnostic.contains(&format!("status: {status}")),
                        "{diagnostic}"
                    );
                    assert!(diagnostic.contains("refresh_rejected"), "{diagnostic}");
                }
                assert!(!diagnostic.contains("sig=expired"));
                let error = writer.write(b"datad".as_slice().into()).await.unwrap_err();
                let diagnostic = format!("{error:?}");
                assert!(
                    diagnostic.contains("failure_stage: credential"),
                    "{diagnostic}"
                );
                if status != 200 {
                    assert!(
                        diagnostic.contains(&format!("status: {status}")),
                        "{diagnostic}"
                    );
                }
                refresh.assert_async().await;
                for request in requests {
                    request.assert_async().await;
                }
            }
        }
    }

    #[test]
    fn account_and_prefix_selection_is_exact() {
        let child = format!("{ROOT}data/");
        let entries = vec![
            StorageCredential {
                prefix: ROOT.to_string(),
                config: config("?sig=root"),
            },
            StorageCredential {
                prefix: child.clone(),
                config: config("?sig=data"),
            },
        ];
        let set = CredentialSet::new(config("sig=fallback"), Some(entries));
        let selected = set
            .load_credential(&Url::parse(LOCATION).unwrap(), SystemTime::now(), true)
            .unwrap();
        assert_eq!(selected.test_sas_token(), "sig=data");
        assert_eq!(selected.prefix(), Some(child.as_str()));
        assert!(selected.covers(LOCATION));
        let outside = "abfss://other@acct.dfs.core.windows.net/outside";
        let credential = set
            .load_credential(&Url::parse(outside).unwrap(), SystemTime::now(), true)
            .unwrap();
        assert_eq!(credential.test_sas_token(), "sig=fallback");
        assert_eq!(
            credential.prefix(),
            Some("abfss://other@acct.dfs.core.windows.net/")
        );
        assert!(credential.covers(outside));
        assert!(!credential.covers(LOCATION));
        assert!(!matches_prefix(
            &Url::parse(ROOT).unwrap(),
            &Url::parse(&LOCATION.replace("/table/", "/table2/")).unwrap()
        ));
        assert!(!matches_prefix(
            &Url::parse(ROOT).unwrap(),
            &Url::parse(&LOCATION.replace("acct.", "otheracct.")).unwrap()
        ));
        assert!(!matches_prefix(
            &Url::parse(ROOT).unwrap(),
            &Url::parse(&LOCATION.replace("fs@", "otherfs@")).unwrap()
        ));
        assert!(!matches_prefix(
            &Url::parse(ROOT).unwrap(),
            &Url::parse(&LOCATION.replace("abfss:", "abfs:")).unwrap()
        ));
        assert!(
            set.select(&Url::parse(&LOCATION.replace("acct.", "otheracct.")).unwrap())
                .is_err()
        );
    }

    #[test]
    fn expiry_and_debug_do_not_expose_tokens() {
        let set = CredentialSet::new(config("sig=secret&se=2000-01-01T00%3A00%3A00Z"), None);
        assert!(
            set.load_credential(&Url::parse(LOCATION).unwrap(), SystemTime::now(), false)
                .is_err()
        );
        let set = CredentialSet::new(config("sig=secret&se=not-a-date"), None);
        let error = set.select(&Url::parse(LOCATION).unwrap()).err().unwrap();
        assert!(!format!("{error:?}").contains("secret"));
        assert!(!format!("{error:?}").contains("not-a-date"));
    }

    #[test]
    fn s3_credentials_are_not_merged_field_by_field() {
        let set = CredentialSet::new(
            HashMap::from([
                ("s3.access-key-id".into(), "static".into()),
                ("s3.secret-access-key".into(), "static-secret".into()),
            ]),
            Some(vec![StorageCredential {
                prefix: "s3://bucket/table/".into(),
                config: HashMap::from([("s3.access-key-id".into(), "vended".into())]),
            }]),
        );
        assert!(
            set.select(&Url::parse("s3://bucket/table/data").unwrap())
                .is_err()
        );
    }

    #[test]
    fn preparsing_preserves_account_precedence_and_both_expiry_limits() {
        let now = SystemTime::now();
        let sas_expiry = now + Duration::from_secs(120);
        let explicit_expiry = now + Duration::from_secs(60);
        let expiry = chrono::DateTime::<chrono::Utc>::from(sas_expiry)
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let mut properties = config(&format!("?sig=account&se={expiry}"));
        properties.insert(ADLS_SAS_TOKEN.into(), "sig=flat".into());
        properties.insert("adls.sas-token-expires-at-ms".into(), "invalid-flat".into());
        properties.insert(
            "adls.sas-token-expires-at-ms.acct.dfs.core.windows.net".into(),
            explicit_expiry
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string(),
        );
        let set = CredentialSet::new(properties, None);
        let selected = set.select(&Url::parse(LOCATION).unwrap()).ok().unwrap();
        assert!(selected.test_sas_token().starts_with("sig=account&"));
        assert_eq!(
            selected
                .expiry
                .unwrap()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            explicit_expiry
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        assert!(
            set.select(&Url::parse(&LOCATION.replace("acct.", "other.")).unwrap())
                .is_err()
        );

        // An account-specific expiry also applies when the token itself is flat.
        let set = CredentialSet::new(
            HashMap::from([
                (ADLS_SAS_TOKEN.into(), format!("sig=flat&se={expiry}")),
                (
                    "adls.sas-token-expires-at-ms.acct.dfs.core.windows.net".into(),
                    "0".into(),
                ),
            ]),
            None,
        );
        assert!(
            set.load_credential(&Url::parse(LOCATION).unwrap(), now, false)
                .is_err()
        );
        assert!(
            set.load_credential(
                &Url::parse(&LOCATION.replace("acct.", "other.")).unwrap(),
                now,
                false
            )
            .is_ok()
        );

        // A longer explicit lifetime must not extend the signed SAS expiry.
        let set = CredentialSet::new(
            HashMap::from([
                (
                    ADLS_SAS_TOKEN.into(),
                    "sig=expired&se=2000-01-01T00:00:00Z".into(),
                ),
                (
                    "adls.sas-token-expires-at-ms".into(),
                    explicit_expiry
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis()
                        .to_string(),
                ),
            ]),
            None,
        );
        assert!(
            set.load_credential(&Url::parse(LOCATION).unwrap(), now, false)
                .is_err()
        );
    }

    #[test]
    fn malformed_unselected_credentials_do_not_poison_other_scopes() {
        let set = CredentialSet::new(
            config("sig=fallback"),
            Some(vec![
                StorageCredential {
                    prefix: "not-a-url".into(),
                    config: config("sig=invalid&se=bad"),
                },
                StorageCredential {
                    prefix: ROOT.into(),
                    config: config("sig=root"),
                },
                StorageCredential {
                    prefix: format!("{ROOT}data/"),
                    config: config("sig=bad&se=bad"),
                },
            ]),
        );
        let metadata = Url::parse(&format!("{ROOT}metadata/file")).unwrap();
        assert_eq!(
            set.load_credential(&metadata, SystemTime::now(), true)
                .unwrap()
                .test_sas_token(),
            "sig=root"
        );
        // A selected invalid child must not fall back to a valid parent grant.
        assert!(
            set.load_credential(&Url::parse(LOCATION).unwrap(), SystemTime::now(), false)
                .is_err()
        );

        let set = CredentialSet::new(
            HashMap::from([
                (ADLS_SAS_TOKEN.into(), "sig=flat".into()),
                (KEY.into(), "?".into()),
            ]),
            None,
        );
        assert!(set.select(&Url::parse(LOCATION).unwrap()).is_err());
        assert!(
            set.select(&Url::parse(&LOCATION.replace("acct.", "other.")).unwrap())
                .is_ok()
        );
    }

    fn expiring_set() -> CredentialSet {
        let now = SystemTime::now();
        let expiry = chrono::DateTime::<chrono::Utc>::from(now + Duration::from_secs(25))
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let mut set = CredentialSet::new(config(&format!("sig=old&se={expiry}")), None);
        set.issued_at = now - Duration::from_secs(300);
        set
    }

    #[tokio::test]
    async fn unknown_endpoint_404_requires_successful_legacy_confirmation() {
        let mut server = Server::new_async().await;
        let missing = server
            .mock("GET", "/table/credentials")
            .with_status(404)
            .expect(1)
            .create_async()
            .await;
        let failure = server
            .mock("GET", "/table")
            .with_status(503)
            .expect(1)
            .create_async()
            .await;
        let provider = provider(&server, expiring_set(), None);
        for _ in 0..3 {
            assert!(provider.load_credential(LOCATION).await.is_err());
        }
        failure.assert_async().await;
        failure.remove_async().await;

        let mut response: serde_json::Value =
            serde_json::from_str(include_str!("../testdata/load_table_response.json")).unwrap();
        response["config"] = json!(config("sig=confirmed"));
        let confirmed = server
            .mock("GET", "/table")
            .with_body(response.to_string())
            .expect(1)
            .create_async()
            .await;
        provider.state.lock().await.retry_after = UNIX_EPOCH;
        assert_eq!(
            provider
                .load_credential(LOCATION)
                .await
                .unwrap()
                .test_sas_token(),
            "sig=confirmed"
        );
        missing.assert_async().await;
        confirmed.assert_async().await;
    }

    async fn gated_table_load(
        server: &mut mockito::ServerGuard,
        max_wait: Duration,
    ) -> (
        mockito::Mock,
        Arc<tokio::sync::Notify>,
        std::sync::mpsc::Sender<()>,
    ) {
        let started = Arc::new(tokio::sync::Notify::new());
        let notify = started.clone();
        let (release, receiver) = std::sync::mpsc::channel();
        let receiver = std::sync::Mutex::new(receiver);
        let load = server
            .mock("GET", "/table")
            .with_chunked_body(move |writer| {
                notify.notify_one();
                let _ = receiver.lock().unwrap().recv_timeout(max_wait);
                writer.write_all(b"{}")
            })
            .expect(1)
            .create_async()
            .await;
        (load, started, release)
    }

    async fn interrupted_legacy_confirmation(cancel: bool) {
        let mut server = Server::new_async().await;
        let missing = server
            .mock("GET", "/table/credentials")
            .with_status(404)
            .expect(1)
            .create_async()
            .await;
        let (load, started, release) = gated_table_load(&mut server, Duration::from_secs(15)).await;
        let provider = provider(&server, expiring_set(), None);
        let task = {
            let provider = provider.clone();
            tokio::spawn(async move { provider.load_credential(LOCATION).await })
        };
        tokio::time::timeout(Duration::from_secs(5), started.notified())
            .await
            .unwrap();
        if cancel {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            assert!(task.await.unwrap().is_err());
        }
        let _ = release.send(());
        // Old credentials are still valid; only the pending 404 confirmation
        // should prevent their use on this and subsequent calls.
        let state = provider.state.lock().await;
        assert!(
            state
                .set
                .load_credential(&Url::parse(LOCATION).unwrap(), SystemTime::now(), false)
                .is_ok()
        );
        drop(state);
        assert!(provider.load_credential(LOCATION).await.is_err());
        missing.assert_async().await;
        load.assert_async().await;
    }

    #[tokio::test]
    async fn unknown_endpoint_404_survives_legacy_cancellation() {
        interrupted_legacy_confirmation(true).await;
    }

    #[tokio::test]
    async fn unknown_endpoint_404_survives_legacy_timeout() {
        interrupted_legacy_confirmation(false).await;
    }

    #[tokio::test]
    async fn refresh_is_single_flight_and_replaces_all_prefixes() {
        let mut server = Server::new_async().await;
        let refresh = server
            .mock("GET", "/table/credentials")
            .match_header("X-Iceberg-Access-Delegation", "vended-credentials")
            .with_body(
                json!({"storage-credentials": [
                    {"prefix": ROOT, "config": config("sig=new-root")},
                    {"prefix": format!("{ROOT}data/"), "config": config("sig=new-data")}
                ]})
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;
        let provider = provider(
            &server,
            CredentialSet::new(HashMap::new(), None),
            Some(true),
        );
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..32 {
            let provider = provider.clone();
            tasks.spawn(async move { provider.load_credential(LOCATION).await.unwrap() });
        }
        while let Some(result) = tasks.join_next().await {
            assert_eq!(result.unwrap().test_sas_token(), "sig=new-data");
        }
        let metadata = provider
            .load_credential(&format!("{ROOT}metadata/file"))
            .await
            .unwrap();
        assert_eq!(metadata.test_sas_token(), "sig=new-root");
        refresh.assert_async().await;
    }

    #[tokio::test]
    async fn advertised_endpoint_404_does_not_fall_back_and_errors_are_redacted() {
        let mut server = Server::new_async().await;
        let missing = server
            .mock("GET", "/table/credentials")
            .with_status(404)
            .with_body("secret-response")
            .expect(1)
            .create_async()
            .await;
        let load = server.mock("GET", "/table").expect(0).create_async().await;
        let provider = provider(
            &server,
            CredentialSet::new(HashMap::new(), None),
            Some(true),
        );
        for _ in 0..3 {
            let error = provider.load_credential(LOCATION).await.unwrap_err();
            assert!(!format!("{error:?}").contains("secret-response"));
        }
        missing.assert_async().await;
        load.assert_async().await;
    }

    #[tokio::test]
    async fn transient_failure_can_use_unexpired_but_not_expired_credentials() {
        let mut server = Server::new_async().await;
        let failure = server
            .mock("GET", "/table/credentials")
            .with_status(503)
            .expect(1)
            .create_async()
            .await;
        let provider = provider(&server, expiring_set(), Some(true));
        assert!(provider.load_credential(LOCATION).await.is_ok());
        provider.state.lock().await.set =
            CredentialSet::new(config("sig=expired&se=2000-01-01T00:00:00Z"), None);
        assert!(provider.load_credential(LOCATION).await.is_err());
        failure.assert_async().await;
    }

    #[tokio::test]
    async fn revoked_credentials_never_fall_back_to_unexpired_tokens() {
        let mut server = Server::new_async().await;
        let failure = server
            .mock("GET", "/table/credentials")
            .with_status(403)
            .expect(1)
            .create_async()
            .await;
        let provider = provider(&server, expiring_set(), Some(true));
        for _ in 0..3 {
            assert!(provider.load_credential(LOCATION).await.is_err());
        }
        failure.assert_async().await;
    }

    #[tokio::test]
    async fn outside_prefix_reads_renew_flat_credentials_through_load_table() {
        for factory in adls_factories() {
            let mut catalog = Server::new_async().await;
            let mut storage = Server::new_async().await;
            let root = ROOT.replace("abfss:", "abfs:");
            let outside = "abfs://fs@acct.dfs.core.windows.net/outside/file";
            let provider = provider(
                &catalog,
                CredentialSet::new(config("sig=old"), None),
                Some(true),
            );
            let io = adls_file_io(factory, &storage, provider.clone());
            let old = storage
                .mock("GET", "/core.windows.net/fs/outside/file")
                .match_query(Matcher::UrlEncoded("sig".into(), "old".into()))
                .with_body("old")
                .expect(1)
                .create_async()
                .await;
            let file = io.new_input(outside).unwrap();
            assert_eq!(file.read().await.unwrap().as_ref(), b"old");
            provider.state.lock().await.set.issued_at = SystemTime::now() - LEASE;
            let entries = json!([{"prefix": root, "config": config("sig=scoped")}]);
            let refresh = catalog
                .mock("GET", "/table/credentials")
                .with_body(json!({"storage-credentials": entries}).to_string())
                .expect(1)
                .create_async()
                .await;
            let mut response: serde_json::Value =
                serde_json::from_str(include_str!("../testdata/load_table_response.json")).unwrap();
            response["config"] = json!(config("sig=renewed"));
            response["storage-credentials"] = entries;
            let load = catalog
                .mock("GET", "/table")
                .match_header("X-Iceberg-Access-Delegation", "vended-credentials")
                .with_body(response.to_string())
                .expect(1)
                .create_async()
                .await;
            let renewed = storage
                .mock("GET", "/core.windows.net/fs/outside/file")
                .match_query(Matcher::UrlEncoded("sig".into(), "renewed".into()))
                .with_body("renewed")
                .expect(1)
                .create_async()
                .await;
            // Refresh a covered file first, then immediately access an uncovered
            // one. The successful prefix refresh must not throttle fetching the
            // missing config or trigger another /credentials request.
            assert_eq!(
                provider
                    .load_credential(&format!("{root}data/file"))
                    .await
                    .unwrap()
                    .test_sas_token(),
                "sig=scoped"
            );
            assert_eq!(file.read().await.unwrap().as_ref(), b"renewed");
            // Both fresh config and prefixed credentials remain available.
            assert_eq!(
                provider
                    .load_credential(&format!("{root}data/file"))
                    .await
                    .unwrap()
                    .test_sas_token(),
                "sig=scoped"
            );
            old.assert_async().await;
            renewed.assert_async().await;
            refresh.assert_async().await;
            load.assert_async().await;
        }
    }

    #[tokio::test]
    async fn cancelled_scope_fallback_does_not_resurrect_old_config() {
        let mut server = Server::new_async().await;
        let refresh = server
            .mock("GET", "/table/credentials")
            .with_body(
                json!({"storage-credentials": [{
                    "prefix": format!("{ROOT}new/"), "config": config("sig=new")
                }]})
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;
        let (load, started, release) = gated_table_load(&mut server, Duration::from_secs(5)).await;
        let provider = provider(&server, expiring_set(), Some(true));
        let task = {
            let provider = provider.clone();
            tokio::spawn(async move { provider.load_credential(LOCATION).await })
        };
        tokio::time::timeout(Duration::from_secs(5), started.notified())
            .await
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let _ = release.send(());
        assert!(provider.load_credential(LOCATION).await.is_err());
        assert_eq!(
            provider
                .load_credential(&format!("{ROOT}new/file"))
                .await
                .unwrap()
                .test_sas_token(),
            "sig=new"
        );
        refresh.assert_async().await;
        load.assert_async().await;
    }

    #[tokio::test]
    async fn malformed_matched_scope_does_not_use_load_table_fallback() {
        let mut server = Server::new_async().await;
        let refresh = server
            .mock("GET", "/table/credentials")
            .with_body(
                json!({"storage-credentials": [{
                    "prefix": ROOT, "config": config("sig=bad&se=invalid")
                }]})
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;
        let load = server.mock("GET", "/table").expect(0).create_async().await;
        let provider = provider(&server, expiring_set(), Some(true));
        assert!(provider.load_credential(LOCATION).await.is_err());
        refresh.assert_async().await;
        load.assert_async().await;
    }

    #[tokio::test]
    async fn successful_refresh_removing_a_prefix_does_not_restore_old_credentials() {
        let mut server = Server::new_async().await;
        let refresh = server
            .mock("GET", "/table/credentials")
            .with_body(r#"{"storage-credentials":[]}"#)
            .expect(1)
            .create_async()
            .await;
        let load = server
            .mock("GET", "/table")
            .with_status(503)
            .expect(1)
            .create_async()
            .await;
        let provider = provider(&server, expiring_set(), Some(true));
        for _ in 0..3 {
            assert!(provider.load_credential(LOCATION).await.is_err());
        }
        refresh.assert_async().await;
        load.assert_async().await;
    }

    #[tokio::test]
    async fn failed_oauth_exchange_does_not_hide_revocation() {
        let mut server = Server::new_async().await;
        let refresh = server
            .mock("GET", "/table/credentials")
            .with_status(401)
            .expect(1)
            .create_async()
            .await;
        let oauth = server
            .mock("POST", "/v1/oauth/tokens")
            .with_status(400)
            .with_body(
                r#"{"error":{"message":"rejected","type":"UnauthorizedException","code":400}}"#,
            )
            .expect(1)
            .create_async()
            .await;
        let cfg = RestCatalogConfig::builder()
            .uri(server.url())
            .props(HashMap::from([
                ("token".to_string(), "old-token".to_string()),
                ("credential".to_string(), "client:dummy-secret".to_string()),
            ]))
            .build();
        let provider = RestVendedCredentialProvider::new(
            Arc::new(HttpClient::new(&cfg).unwrap()),
            format!("{}/table", server.url()),
            expiring_set(),
            Some(true),
        );
        for _ in 0..3 {
            assert!(provider.load_credential(LOCATION).await.is_err());
        }
        refresh.assert_async().await;
        oauth.assert_async().await;
    }
}
