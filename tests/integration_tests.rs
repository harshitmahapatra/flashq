use message_queue_rs::api::*;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;

struct TestServer {
    process: Child,
    port: u16,
}

impl TestServer {
    async fn start(port: u16) -> Result<Self, Box<dyn std::error::Error>> {
        let mut process = Command::new("cargo")
            .args(&["run", "--bin", "server", &port.to_string()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        // Wait for server to start and verify it's responding
        let client = reqwest::Client::new();
        let health_url = format!("http://127.0.0.1:{}/health", port);

        for _ in 0..30 {
            // Try for up to 15 seconds
            sleep(Duration::from_millis(500)).await;

            // Check if process is still running
            if let Ok(Some(_)) = process.try_wait() {
                return Err("Server process exited".into());
            }

            // Try to connect to health endpoint
            if let Ok(response) = client.get(&health_url).send().await {
                if response.status().is_success() {
                    return Ok(TestServer { process, port });
                }
            }
        }

        let _ = process.kill();
        Err("Server failed to start within timeout".into())
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

#[tokio::test]
async fn test_post_message_integration() {
    let server = TestServer::start(8081)
        .await
        .expect("Failed to start test server");

    let client = reqwest::Client::new();
    let url = format!("{}/api/topics/test/messages", server.base_url());

    let request_body = PostMessageRequest {
        content: "Integration test message".to_string(),
    };

    let response = client
        .post(&url)
        .json(&request_body)
        .send()
        .await
        .expect("Failed to send POST request");

    assert!(response.status().is_success());

    let response_data: PostMessageResponse = response
        .json()
        .await
        .expect("Failed to parse response JSON");

    assert_eq!(response_data.id, 0); // First message should have ID 0
    assert!(response_data.timestamp > 0);
}

#[tokio::test]
async fn test_poll_messages_integration() {
    let server = TestServer::start(8082)
        .await
        .expect("Failed to start test server");

    let client = reqwest::Client::new();
    let base_url = server.base_url();

    // First post a message
    let post_url = format!("{}/api/topics/test/messages", base_url);
    let request_body = PostMessageRequest {
        content: "Message for polling test".to_string(),
    };

    client
        .post(&post_url)
        .json(&request_body)
        .send()
        .await
        .expect("Failed to post message");

    // Then poll for messages
    let poll_url = format!("{}/api/topics/test/messages", base_url);
    let response = client
        .get(&poll_url)
        .send()
        .await
        .expect("Failed to send GET request");

    assert!(response.status().is_success());

    let response_data: PollMessagesResponse = response
        .json()
        .await
        .expect("Failed to parse response JSON");

    assert_eq!(response_data.count, 1);
    assert_eq!(response_data.messages.len(), 1);
    assert_eq!(
        response_data.messages[0].content,
        "Message for polling test"
    );
    assert_eq!(response_data.messages[0].id, 0);
}

#[tokio::test]
async fn test_end_to_end_workflow() {
    let server = TestServer::start(8083)
        .await
        .expect("Failed to start test server");

    let client = reqwest::Client::new();
    let base_url = server.base_url();

    let post_message_requests = vec![
        (
            "topic1".to_string(),
            PostMessageRequest {
                content: "topic1 message1".to_string(),
            },
        ),
        (
            "topic2".to_string(),
            PostMessageRequest {
                content: "topic2 message1".to_string(),
            },
        ),
        (
            "topic1".to_string(),
            PostMessageRequest {
                content: "topic1 message2".to_string(),
            },
        ),
    ];

    // Post all messages sequentially
    for (topic, post_message_request) in &post_message_requests {
        client
            .post(format!("{}/api/topics/{}/messages", base_url, topic))
            .json(&post_message_request)
            .send()
            .await
            .expect("Failed to post message");
    }

    // Verify that messages are present and have the correct ordering and content

    // Test topic1: should have 2 messages in FIFO order
    let topic1_url = format!("{}/api/topics/topic1/messages", base_url);
    let topic1_response = client
        .get(&topic1_url)
        .send()
        .await
        .expect("Failed to poll topic1 messages");

    assert!(topic1_response.status().is_success());

    let topic1_data: PollMessagesResponse = topic1_response
        .json()
        .await
        .expect("Failed to parse topic1 response");

    assert_eq!(topic1_data.count, 2);
    assert_eq!(topic1_data.messages.len(), 2);
    assert_eq!(topic1_data.messages[0].content, "topic1 message1");
    assert_eq!(topic1_data.messages[1].content, "topic1 message2");
    assert_eq!(topic1_data.messages[0].id, 0); // First message in topic1 (offset 0)
    assert_eq!(topic1_data.messages[1].id, 1); // Second message in topic1 (offset 1)

    // Test topic2: should have 1 message
    let topic2_url = format!("{}/api/topics/topic2/messages", base_url);
    let topic2_response = client
        .get(&topic2_url)
        .send()
        .await
        .expect("Failed to poll topic2 messages");

    assert!(topic2_response.status().is_success());

    let topic2_data: PollMessagesResponse = topic2_response
        .json()
        .await
        .expect("Failed to parse topic2 response");

    assert_eq!(topic2_data.count, 1);
    assert_eq!(topic2_data.messages.len(), 1);
    assert_eq!(topic2_data.messages[0].content, "topic2 message1");
    assert_eq!(topic2_data.messages[0].id, 0); // First message in topic2 (offset 0)

    // Test count parameter: limit topic1 to 1 message
    let topic1_limited_url = format!("{}/api/topics/topic1/messages?count=1", base_url);
    let limited_response = client
        .get(&topic1_limited_url)
        .send()
        .await
        .expect("Failed to poll topic1 with count limit");

    assert!(limited_response.status().is_success());

    let limited_data: PollMessagesResponse = limited_response
        .json()
        .await
        .expect("Failed to parse limited response");

    assert_eq!(limited_data.count, 1);
    assert_eq!(limited_data.messages.len(), 1);
    assert_eq!(limited_data.messages[0].content, "topic1 message1"); // Should get first message

    // Verify timestamps are in ascending order (FIFO)
    assert!(topic1_data.messages[0].timestamp <= topic1_data.messages[1].timestamp);
}

#[tokio::test]
async fn test_health_check() {
    let server = TestServer::start(8084)
        .await
        .expect("Failed to start test server");

    let client = reqwest::Client::new();
    let url = format!("{}/health", server.base_url());

    let response = client
        .get(&url)
        .send()
        .await
        .expect("Failed to send health check request");

    assert!(response.status().is_success());

    let response_json: serde_json::Value = response
        .json()
        .await
        .expect("Failed to parse health check response");

    assert_eq!(response_json["status"], "healthy");
    assert_eq!(response_json["service"], "message-queue-rs");
}

#[tokio::test]
async fn test_error_handling() {
    let server = TestServer::start(8085)
        .await
        .expect("Failed to start test server");

    let client = reqwest::Client::new();
    let url = format!("{}/api/topics/test/messages", server.base_url());

    // Test invalid JSON
    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .body("invalid json")
        .send()
        .await
        .expect("Failed to send invalid request");

    assert!(!response.status().is_success());
}
