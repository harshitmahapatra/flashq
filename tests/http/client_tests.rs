// Tests for expanded client functionality
// These tests verify that the client binary supports all operations needed by integration tests

use super::http_test_helpers::{TestServer, ensure_client_binary};
use std::process::Command;

#[tokio::test]
async fn test_client_consumer_group_operations() {
    let server = TestServer::start().await.unwrap();
    let client_binary = ensure_client_binary().unwrap();
    let port = server.port.to_string();

    // Test creating consumer group
    let output = Command::new(&client_binary)
        .args(["--port", &port, "consumer", "create", "test_group"])
        .output()
        .expect("Failed to execute client");
    assert!(
        output.status.success(),
        "Failed to create consumer group: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Test getting consumer group offset
    let output = Command::new(&client_binary)
        .args([
            "--port",
            &port,
            "consumer",
            "offset",
            "get",
            "test_group",
            "test_topic",
        ])
        .output()
        .expect("Failed to execute client");
    assert!(
        output.status.success(),
        "Failed to get consumer group offset: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Test setting consumer group offset
    let output = Command::new(&client_binary)
        .args([
            "--port",
            &port,
            "consumer",
            "offset",
            "commit",
            "test_group",
            "test_topic",
            "5",
        ])
        .output()
        .expect("Failed to execute client");
    assert!(
        output.status.success(),
        "Failed to set consumer group offset: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Test consumer group polling
    let output = Command::new(&client_binary)
        .args([
            "--port",
            &port,
            "consumer",
            "fetch",
            "test_group",
            "test_topic",
        ])
        .output()
        .expect("Failed to execute client");
    assert!(
        output.status.success(),
        "Failed to poll with consumer group: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Test leaving consumer group
    let output = Command::new(&client_binary)
        .args(["--port", &port, "consumer", "leave", "test_group"])
        .output()
        .expect("Failed to execute client");
    assert!(
        output.status.success(),
        "Failed to leave consumer group: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn test_client_batch_posting() {
    let server = TestServer::start().await.unwrap();
    let client_binary = ensure_client_binary().unwrap();
    let port = server.port.to_string();

    // Test batch posting with JSON file
    let batch_data = r#"[
        {"value": "message1"},
        {"key": "key1", "value": "message2", "headers": {"type": "test"}},
        {"value": "message3"}
    ]"#;

    std::fs::write("/tmp/test_batch.json", batch_data).unwrap();

    let output = Command::new(&client_binary)
        .args([
            "--port",
            &port,
            "producer",
            "records",
            "test_topic",
            "--batch",
            "/tmp/test_batch.json",
        ])
        .output()
        .expect("Failed to execute client");
    assert!(
        output.status.success(),
        "Failed to post batch: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn test_client_advanced_post() {
    let server = TestServer::start().await.unwrap();
    let client_binary = ensure_client_binary().unwrap();
    let port = server.port.to_string();

    // Test posting with key and headers
    let output = Command::new(&client_binary)
        .args([
            "--port",
            &port,
            "producer",
            "records",
            "test_topic",
            "test_message",
            "--key",
            "test_key",
            "--header",
            "type=test",
            "--header",
            "priority=high",
        ])
        .output()
        .expect("Failed to execute client");
    assert!(
        output.status.success(),
        "Failed to post with key/headers: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn test_client_advanced_poll() {
    let server = TestServer::start().await.unwrap();
    let client_binary = ensure_client_binary().unwrap();
    let port = server.port.to_string();

    // First create consumer group
    let output = Command::new(&client_binary)
        .args(["--port", &port, "consumer", "create", "test_group"])
        .output()
        .expect("Failed to execute client");
    assert!(
        output.status.success(),
        "Failed to create consumer group: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Test polling with from_offset
    let output = Command::new(&client_binary)
        .args([
            "--port",
            &port,
            "consumer",
            "fetch",
            "test_group",
            "test_topic",
            "--from-offset",
            "5",
            "--max-records",
            "10",
        ])
        .output()
        .expect("Failed to execute client");
    assert!(
        output.status.success(),
        "Failed to poll with from_offset: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn test_client_health_check() {
    let server = TestServer::start().await.unwrap();
    let client_binary = ensure_client_binary().unwrap();
    let port = server.port.to_string();

    // Test health check
    let output = Command::new(&client_binary)
        .args(["--port", &port, "health"])
        .output()
        .expect("Failed to execute client");
    assert!(
        output.status.success(),
        "Failed to get health status: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
