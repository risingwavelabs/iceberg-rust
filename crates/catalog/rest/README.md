<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Apache Iceberg Rest Catalog Official Native Rust Implementation

[![crates.io](https://img.shields.io/crates/v/iceberg.svg)](https://crates.io/crates/iceberg-catalog-rest)
[![docs.rs](https://img.shields.io/docsrs/iceberg.svg)](https://docs.rs/iceberg/latest/iceberg-catalog-rest/)

This crate contains the official Native Rust implementation of Apache Iceberg Rest Catalog.

See the [API documentation](https://docs.rs/iceberg-catalog-rest/latest) for examples and the full API.

## Vended storage credentials

With `iceberg-storage-opendal` and its `opendal-azdls` or `opendal-s3` feature,
set `header.X-Iceberg-Access-Delegation=vended-credentials` on the REST catalog.
Use `OpenDalResolvingStorageFactory` for tables spanning storage locations.

Credentials are selected for each file URI, not just the metadata URI. The
longest matching `storage-credentials` prefix wins, followed by the response's
`config`. Azure supports `adls.sas-token.<account-host>` and `adls.sas-token`;
account-specific keys take precedence. A leading `?` is accepted.

Already-open readers and writers share automatic renewal through the table's
`/credentials` endpoint. Legacy catalogs without that endpoint fall back to
loadTable without replacing the query's metadata snapshot. SAS `se`, optional
`adls.sas-token-expires-at-ms[.<account-host>]`, and
`s3.session-token-expires-at-ms` bound credential lifetime. Missing expiry gets
a five-minute renewal lease. Refresh is single-flight, with a ten-second timeout
and a one-second retry gate.

Delegated mode fails closed: it never falls back to static or ambient storage
credentials after a missing, expired or revoked vended credential. To use only
static credentials, do not request delegation. Non-authentication options such
as `adls.endpoint`, retries and write chunk size remain in FileIO configuration.
Delegated S3 requests always require signing, even if anonymous access was
configured. An unadvertised credentials endpoint returning 404 blocks reuse of
old credentials until the catalog successfully confirms access through loadTable.
