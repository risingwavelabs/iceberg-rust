<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Iceberg Rust — Agent Instructions

This file provides repository-specific guidance for automated agents working
in this repository.

## Security Model

When assessing potential vulnerabilities or calibrating automated security
findings, use [`SECURITY-THREAT-MODEL.md`](SECURITY-THREAT-MODEL.md) as the
authoritative detailed description of this repository's security boundaries,
trust assumptions, and non-boundaries.

## Public API Snapshots

Each published crate commits its public API to `<crate>/public-api.txt`, and CI's `check-public-api` job fails on any difference, including additions such as a new `pub fn`. After changing a crate's public items, run `make generate-public-api` and commit the updated snapshots; `make check-public-api` reproduces the CI check locally. Clippy and tests don't catch this. The make output ends in a bare `make: *** [check-public-api] Error 1`, so look further up for the `ERROR: Public API for <crate> has changed` line and its diff.
