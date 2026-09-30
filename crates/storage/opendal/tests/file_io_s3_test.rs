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

//! Integration tests for FileIO S3.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.
//! Each test uses unique file paths based on module path to avoid conflicts.
#[cfg(feature = "opendal-s3")]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use async_trait::async_trait;
    use futures::StreamExt;
    use iceberg::io::{
        FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
        S3_SECRET_ACCESS_KEY, StorageCredential, StorageCredentialProvider, StorageFactory,
    };
    use iceberg_storage_opendal::{OpenDalResolvingStorageFactory, OpenDalStorageFactory};
    use iceberg_test_utils::{get_object_store_endpoint, normalize_test_name_with_parts, set_up};

    async fn get_file_io() -> FileIO {
        set_up();

        let object_store_endpoint = get_object_store_endpoint();

        FileIOBuilder::new(Arc::new(OpenDalStorageFactory::s3()))
            .with_props(vec![
                (S3_ENDPOINT, object_store_endpoint),
                (S3_ACCESS_KEY_ID, "admin".to_string()),
                (S3_SECRET_ACCESS_KEY, "password".to_string()),
                (S3_REGION, "us-east-1".to_string()),
                (S3_PATH_STYLE_ACCESS, "true".to_string()),
            ])
            .build()
    }

    #[tokio::test]
    async fn test_file_io_s3_exists() {
        let file_io = get_file_io().await;
        assert!(!file_io.exists("s3://bucket2/any").await.unwrap());
        assert!(file_io.exists("s3://bucket1/").await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_s3_output() {
        let file_io = get_file_io().await;
        // Use unique file path based on module path to avoid conflicts
        let output_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_file_io_s3_output")
        );
        // Clean up from any previous test runs
        let _ = file_io.delete(&output_path).await;
        assert!(!file_io.exists(&output_path).await.unwrap());
        let output_file = file_io.new_output(&output_path).unwrap();
        {
            output_file.write("123".into()).await.unwrap();
        }
        assert!(file_io.exists(&output_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_s3_input() {
        let file_io = get_file_io().await;
        // Use unique file path based on module path to avoid conflicts
        let file_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_file_io_s3_input")
        );
        let output_file = file_io.new_output(&file_path).unwrap();
        {
            output_file.write("test_input".into()).await.unwrap();
        }

        let input_file = file_io.new_input(&file_path).unwrap();

        {
            let buffer = input_file.read().await.unwrap();
            assert_eq!(buffer, "test_input".as_bytes());
        }
    }

    #[derive(Debug)]
    struct TestCredentialProvider;

    #[async_trait]
    impl StorageCredentialProvider for TestCredentialProvider {
        async fn load_credential(&self, location: &str) -> iceberg::Result<StorageCredential> {
            assert!(location.starts_with("s3://bucket1/"));
            Ok(
                StorageCredential::new(iceberg::io::StorageCredentialKind::S3(
                    iceberg::io::S3Credential::new("admin", "password", None),
                ))
                .with_expiration(SystemTime::now() + Duration::from_secs(30)),
            )
        }
    }

    #[tokio::test]
    async fn test_s3_with_credential_provider() {
        set_up();
        let factories: [Arc<dyn StorageFactory>; 2] = [
            Arc::new(OpenDalStorageFactory::s3()),
            Arc::new(OpenDalResolvingStorageFactory::new()),
        ];
        for (index, factory) in factories.into_iter().enumerate() {
            let io = FileIOBuilder::new(factory)
                .with_props([
                    (S3_ENDPOINT, get_object_store_endpoint()),
                    (S3_REGION, "us-east-1".to_string()),
                    (S3_PATH_STYLE_ACCESS, "true".to_string()),
                ])
                .with_credential_provider(Arc::new(TestCredentialProvider))
                .build();
            let path = format!(
                "s3://bucket1/{}/{index}",
                normalize_test_name_with_parts!("test_s3_with_credential_provider")
            );
            io.new_output(&path)
                .unwrap()
                .write("custom credentials".into())
                .await
                .unwrap();
            assert_eq!(
                io.new_input(&path).unwrap().read().await.unwrap().as_ref(),
                b"custom credentials"
            );
            io.delete(&path).await.unwrap();
            assert!(!io.exists(&path).await.unwrap());
        }
    }

    #[tokio::test]
    async fn test_file_io_s3_delete_stream() {
        let file_io = get_file_io().await;

        // Write multiple files
        let paths: Vec<String> = (0..5)
            .map(|i| {
                format!(
                    "s3://bucket1/{}/file-{i}",
                    normalize_test_name_with_parts!("test_file_io_s3_delete_stream")
                )
            })
            .collect();
        for path in &paths {
            let _ = file_io.delete(path).await;
            file_io
                .new_output(path)
                .unwrap()
                .write("delete-me".into())
                .await
                .unwrap();
            assert!(file_io.exists(path).await.unwrap());
        }

        // Delete via delete_stream
        let stream = futures::stream::iter(paths.clone()).boxed();
        file_io.delete_stream(stream).await.unwrap();

        // Verify all files are gone
        for path in &paths {
            assert!(!file_io.exists(path).await.unwrap());
        }
    }

    #[tokio::test]
    async fn test_file_io_s3_delete_stream_empty() {
        let file_io = get_file_io().await;
        let stream = futures::stream::empty().boxed();
        // Should succeed with no-op
        file_io.delete_stream(stream).await.unwrap();
    }
}
