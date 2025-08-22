use message_queue_rs::api::*;
use std::env;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, Once};
use std::time::Duration;
use tokio::time::sleep;

use std::net::TcpListener;

static INIT: Once = Once::new();
static SERVER_BINARY_PATH: Mutex<Option<PathBuf>> = Mutex::new(None);

fn find_available_port() -> Result<u16, Box<dyn std::error::Error>> {
    // Bind to port 0 to let the OS choose an available port
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    Ok(addr.port())
}

fn ensure_server_binary() -> Result<PathBuf, Box<dyn std::error::Error>> {
    INIT.call_once(|| {
        eprintln!("Building server binary for integration tests...");
        let output = Command::new("cargo")
            .args(&["build", "--bin", "server"])
            .output()
            .expect("Failed to build server binary");

        if !output.status.success() {
            panic!(
                "Failed to build server binary: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let binary_path = PathBuf::from("target/debug/server");
        *SERVER_BINARY_PATH.lock().unwrap() = Some(binary_path);
        eprintln!("Server binary built successfully");
    });

    let binary_path = SERVER_BINARY_PATH
        .lock()
        .unwrap()
        .as_ref()
        .expect("Server binary path should be set")
        .clone();

    Ok(binary_path)
}

fn get_timeout_config() -> (u32, u64) {
    // Returns (max_attempts, sleep_ms)
    if env::var("CI").is_ok() {
        eprintln!("Running in CI environment - using extended timeouts");
        (60, 500) // 30 seconds total in CI
    } else {
        (30, 500) // 15 seconds locally
    }
}

struct TestServer {
    process: Child,
    port: u16,
}

impl TestServer {
    async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let port = find_available_port()?;
        let server_binary = ensure_server_binary()?;
        let (max_attempts, sleep_ms) = get_timeout_config();

        eprintln!(
            "Starting server on port {} using binary: {:?}",
            port, server_binary
        );

        let mut process = Command::new(&server_binary)
            .args(&[&port.to_string()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        // Wait for server to start and verify it's responding
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        let health_url = format!("http://127.0.0.1:{}/health", port);

        for attempt in 0..max_attempts {
            sleep(Duration::from_millis(sleep_ms)).await;

            // Check if process is still running
            if let Ok(Some(exit_status)) = process.try_wait() {
                eprintln!("Server process exited with status: {}", exit_status);
                return Err("Server process exited".into());
            }

            // Try to connect to health endpoint with retry logic
            match Self::try_health_check(&client, &health_url, attempt + 1).await {
                Ok(true) => {
                    eprintln!(
                        "Server started successfully on port {} after {} attempts",
                        port,
                        attempt + 1
                    );
                    return Ok(TestServer { process, port });
                }
                Ok(false) => continue, // Retry
                Err(e) => {
                    eprintln!("Health check attempt {} failed: {}", attempt + 1, e);
                    continue;
                }
            }
        }

        eprintln!("Server failed to start within {} attempts", max_attempts);
        let _ = process.kill();
        Err("Server failed to start within timeout".into())
    }

    async fn try_health_check(
        client: &reqwest::Client,
        health_url: &str,
        attempt: u32,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        // Exponential backoff for network retries within each attempt
        for retry in 0..3 {
            let backoff_ms = 100 * (2_u64.pow(retry));
            if retry > 0 {
                sleep(Duration::from_millis(backoff_ms)).await;
            }

            match client.get(health_url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        return Ok(true);
                    } else {
                        eprintln!(
                            "Health check attempt {}.{}: HTTP {}",
                            attempt,
                            retry + 1,
                            response.status()
                        );
                    }
                }
                Err(e) if retry < 2 => {
                    eprintln!(
                        "Health check attempt {}.{} failed, retrying: {}",
                        attempt,
                        retry + 1,
                        e
                    );
                    continue;
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        Ok(false) // All retries failed
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
    let server = TestServer::start()
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
    let server = TestServer::start()
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
    let server = TestServer::start()
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
    let server = TestServer::start()
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
    let server = TestServer::start()
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

#[tokio::test]
#[ignore] // TODO: Implement consumer group isolation testing
async fn test_consumer_group_isolation() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let client = reqwest::Client::new();
    let base_url = server.base_url();
    let topic = "isolation_test_topic";

    // Setup: Create a topic with messages
    for i in 0..5 {
        let response = client
            .post(&format!("{}/api/topics/{}/messages", base_url, topic))
            .json(&PostMessageRequest {
                content: format!("Message {}", i),
            })
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }

    // Create consumer groups
    let group1 = "isolation_group_1";
    let group2 = "isolation_group_2";

    for group in [group1, group2] {
        let response = client
            .post(&format!("{}/api/consumer-groups", base_url))
            .json(&CreateConsumerGroupRequest {
                group_id: group.to_string(),
            })
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }

    // TODO(human): Test that consumer groups maintain separate offsets
    // - Have both groups poll the same topic
    // - Verify their offsets are independent
    // - Ensure one group's consumption doesn't affect the other
}

#[tokio::test]
async fn test_consumer_group_offset_boundaries() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let client = reqwest::Client::new();
    let base_url = server.base_url();
    let topic = "boundary_test_topic";
    let group = "boundary_test_group";

    // Setup: Create topic with 3 messages (offsets 0, 1, 2)
    for i in 0..3 {
        let response = client
            .post(&format!("{}/api/topics/{}/messages", base_url, topic))
            .json(&PostMessageRequest {
                content: format!("Message {}", i),
            })
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }

    // Create consumer group
    client
        .post(&format!("{}/api/consumer-groups", base_url))
        .json(&CreateConsumerGroupRequest {
            group_id: group.to_string(),
        })
        .send()
        .await
        .unwrap();

    let update_url = format!("{}/api/consumer-groups/{}/topics/{}/offset", base_url, group, topic);

    // Test boundary conditions
    let test_cases = [
        (10, 404),        // Beyond available messages
        (u64::MAX, 404),  // Extreme value
        (3, 200),         // Valid boundary (end position)
        (1, 200),         // Valid within bounds
    ];

    for (offset, expected_status) in test_cases {
        let response = client
            .post(&update_url)
            .json(&UpdateConsumerGroupOffsetRequest { offset })
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected_status, "Failed for offset {}", offset);
    }
}

#[tokio::test]
#[ignore] // TODO: Implement error handling testing
async fn test_consumer_group_error_handling() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let client = reqwest::Client::new();
    let base_url = server.base_url();

    // TODO(human): Test error conditions
    // - Operations on nonexistent consumer groups (expect 404)
    // - Operations on nonexistent topics
    // - Duplicate consumer group creation (expect 400)
    // - Invalid JSON payloads (expect 400)
    // - Verify proper HTTP status codes and error messages
}

#[tokio::test]
#[ignore] // TODO: Implement empty topic testing
async fn test_consumer_group_empty_topics() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let client = reqwest::Client::new();
    let base_url = server.base_url();

    // Create consumer group
    let group = "empty_topic_group";
    let response = client
        .post(&format!("{}/api/consumer-groups", base_url))
        .json(&CreateConsumerGroupRequest {
            group_id: group.to_string(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    // TODO(human): Test empty topic scenarios
    // - Poll from topics that don't exist yet
    // - Poll from topics with no messages
    // - Verify offset behavior for empty topics (should stay at 0)
    // - Test offset updates on empty topics
}

#[tokio::test]
#[ignore] // TODO: Implement offset advancement testing
async fn test_consumer_group_offset_advancement() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let client = reqwest::Client::new();
    let base_url = server.base_url();
    let topic = "advancement_test_topic";

    // Setup: Create topic with messages
    for i in 0..5 {
        let response = client
            .post(&format!("{}/api/topics/{}/messages", base_url, topic))
            .json(&PostMessageRequest {
                content: format!("Message {}", i),
            })
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }

    // Create consumer group
    let group = "advancement_test_group";
    let response = client
        .post(&format!("{}/api/consumer-groups", base_url))
        .json(&CreateConsumerGroupRequest {
            group_id: group.to_string(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    // TODO(human): Test offset advancement behavior
    // - Poll with different count limits (1, 2, all)
    // - Verify offset advances correctly after each poll
    // - Test polling when at end of log (no new messages)
    // - Verify new_offset in response matches expected values
}

#[tokio::test]
#[ignore] // TODO: Implement concurrent operations testing
async fn test_consumer_group_concurrent_operations() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let client = reqwest::Client::new();
    let base_url = server.base_url();
    let topic = "concurrent_test_topic";

    // Setup: Create topic with messages
    for i in 0..10 {
        let response = client
            .post(&format!("{}/api/topics/{}/messages", base_url, topic))
            .json(&PostMessageRequest {
                content: format!("Message {}", i),
            })
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }

    // Create multiple consumer groups
    let groups = [
        "concurrent_group_1",
        "concurrent_group_2",
        "concurrent_group_3",
    ];
    for group in groups {
        let response = client
            .post(&format!("{}/api/consumer-groups", base_url))
            .json(&CreateConsumerGroupRequest {
                group_id: group.to_string(),
            })
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }

    // TODO(human): Test concurrent operations
    // - Multiple groups polling same topic simultaneously
    // - Concurrent offset updates and polling
    // - Verify thread safety and data consistency
    // - Test with high concurrency load
}
