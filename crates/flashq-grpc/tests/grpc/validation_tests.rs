use super::test_utilities::TestServer;
use flashq_grpc::flashq::v1 as proto;
use tonic::Code;

#[tokio::test]
async fn test_record_key_size_validation() {
    let srv = TestServer::start().await.expect("start server");
    let addr = format!("http://127.0.0.1:{}", srv.port);
    let mut client = proto::producer_client::ProducerClient::connect(addr)
        .await
        .expect("connect producer");

    // Test oversized key (> 1024 chars)
    let oversized_key = "x".repeat(1025);
    let request = proto::ProduceRequest {
        topic: "validation_test_topic".to_string(),
        records: vec![proto::Record {
            key: oversized_key,
            value: "test_value".to_string(),
            headers: std::collections::HashMap::new(),
        }],
    };

    let result = client.produce(request).await;
    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("key exceeds maximum length of 1024 characters (got 1025)"));
}

#[tokio::test]
async fn test_record_value_size_validation() {
    let srv = TestServer::start().await.expect("start server");
    let addr = format!("http://127.0.0.1:{}", srv.port);
    let mut client = proto::producer_client::ProducerClient::connect(addr)
        .await
        .expect("connect producer");

    // Test oversized value (> 1MB)
    let oversized_value = "x".repeat(1_048_577); // 1MB + 1 byte
    let request = proto::ProduceRequest {
        topic: "validation_test_topic".to_string(),
        records: vec![proto::Record {
            key: "test_key".to_string(),
            value: oversized_value,
            headers: std::collections::HashMap::new(),
        }],
    };

    let result = client.produce(request).await;
    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("value exceeds maximum length of 1048576 bytes (got 1048577)"));
}

#[tokio::test]
async fn test_record_header_size_validation() {
    let srv = TestServer::start().await.expect("start server");
    let addr = format!("http://127.0.0.1:{}", srv.port);
    let mut client = proto::producer_client::ProducerClient::connect(addr)
        .await
        .expect("connect producer");

    // Test oversized header value (> 1024 chars)
    let oversized_header_value = "y".repeat(1025);
    let mut headers = std::collections::HashMap::new();
    headers.insert("test_header".to_string(), oversized_header_value);

    let request = proto::ProduceRequest {
        topic: "validation_test_topic".to_string(),
        records: vec![proto::Record {
            key: "test_key".to_string(),
            value: "test_value".to_string(),
            headers,
        }],
    };

    let result = client.produce(request).await;
    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("header 'test_header' value exceeds maximum length of 1024 characters (got 1025)"));
}

#[tokio::test]
async fn test_valid_record_sizes_pass_validation() {
    let srv = TestServer::start().await.expect("start server");
    let addr = format!("http://127.0.0.1:{}", srv.port);
    let mut client = proto::producer_client::ProducerClient::connect(addr)
        .await
        .expect("connect producer");

    // Test maximum allowed sizes that should pass
    let max_key = "x".repeat(1024);         // Exactly 1024 chars
    let max_value = "y".repeat(1_048_576);  // Exactly 1MB
    let mut headers = std::collections::HashMap::new();
    headers.insert("test_header".to_string(), "z".repeat(1024)); // Exactly 1024 chars

    let request = proto::ProduceRequest {
        topic: "validation_test_topic".to_string(),
        records: vec![proto::Record {
            key: max_key,
            value: max_value,
            headers,
        }],
    };

    let result = client.produce(request).await;
    assert!(result.is_ok(), "Valid record sizes should pass validation");
}

