mod test_helpers;

use message_queue_rs::api::*;
use std::time::Duration;
use test_helpers::{TestHelper, TestServer};

#[tokio::test]
async fn test_post_message_integration() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);

    // Test basic message posting with new structure
    let response = helper
        .post_message_with_record("test", None, "Integration test message", None)
        .await
        .unwrap();
    assert!(response.status().is_success());

    let response_data: PostMessageResponse = response
        .json()
        .await
        .expect("Failed to parse response JSON");
    assert_eq!(response_data.offset, 0); // First message should have offset 0
    assert!(response_data.timestamp.contains("T")); // ISO 8601 format check

    // Test message with key and headers
    let mut headers = std::collections::HashMap::new();
    headers.insert("source".to_string(), "integration-test".to_string());
    headers.insert("priority".to_string(), "high".to_string());

    let response = helper
        .post_message_with_record(
            "test",
            Some("user123".to_string()),
            "Message with metadata",
            Some(headers),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    let response_data: PostMessageResponse = response
        .json()
        .await
        .expect("Failed to parse response JSON");
    assert_eq!(response_data.offset, 1); // Second message should have offset 1
}

#[tokio::test]
async fn test_poll_messages_integration() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);

    // Post a message
    helper
        .post_message("test", "Message for polling test")
        .await
        .expect("Failed to post message");

    // Poll for messages
    let response = helper
        .poll_messages("test", None)
        .await
        .expect("Failed to send GET request");
    let poll_data = helper
        .assert_poll_response(response, 1, Some(&["Message for polling test"]))
        .await;

    // Verify MessageWithOffset structure
    assert_eq!(poll_data.messages[0].offset, 0); // Changed from id to offset
    assert_eq!(poll_data.messages[0].value, "Message for polling test"); // Changed from content to value
    assert!(poll_data.messages[0].key.is_none()); // Key should be None for basic message
    assert!(poll_data.messages[0].headers.is_none()); // Headers should be None for basic message
    assert!(poll_data.messages[0].timestamp.contains("T")); // ISO 8601 timestamp format
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
                key: None,
                value: "topic1 message1".to_string(),
                headers: None,
            },
        ),
        (
            "topic2".to_string(),
            PostMessageRequest {
                key: Some("user456".to_string()),
                value: "topic2 message1".to_string(),
                headers: {
                    let mut headers = std::collections::HashMap::new();
                    headers.insert("source".to_string(), "workflow-test".to_string());
                    Some(headers)
                },
            },
        ),
        (
            "topic1".to_string(),
            PostMessageRequest {
                key: Some("user123".to_string()),
                value: "topic1 message2".to_string(),
                headers: None,
            },
        ),
    ];

    // Post all messages sequentially
    for (topic, post_message_request) in &post_message_requests {
        client
            .post(format!("{base_url}/topics/{topic}/records"))
            .json(&post_message_request)
            .send()
            .await
            .expect("Failed to post message");
    }

    // Verify that messages are present and have the correct ordering and content

    // Test topic1: should have 2 messages in FIFO order
    let topic1_url = format!("{base_url}/topics/topic1/messages");
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

    // Verify MessageWithOffset structure for topic1
    assert_eq!(topic1_data.messages[0].value, "topic1 message1");
    assert_eq!(topic1_data.messages[1].value, "topic1 message2");
    assert_eq!(topic1_data.messages[0].offset, 0); // First message in topic1 (offset 0)
    assert_eq!(topic1_data.messages[1].offset, 1); // Second message in topic1 (offset 1)
    assert!(topic1_data.messages[0].key.is_none()); // First message has no key
    assert_eq!(topic1_data.messages[1].key.as_ref().unwrap(), "user123"); // Second message has key

    // Test topic2: should have 1 message with metadata
    let topic2_url = format!("{base_url}/topics/topic2/messages");
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
    assert_eq!(topic2_data.messages[0].value, "topic2 message1");
    assert_eq!(topic2_data.messages[0].offset, 0); // First message in topic2 (offset 0)
    assert_eq!(topic2_data.messages[0].key.as_ref().unwrap(), "user456");

    // Verify headers
    let headers = topic2_data.messages[0].headers.as_ref().unwrap();
    assert_eq!(headers.get("source").unwrap(), "workflow-test");

    // Test count parameter: limit topic1 to 1 message
    let topic1_limited_url = format!("{base_url}/topics/topic1/messages?count=1");
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
    assert_eq!(limited_data.messages[0].value, "topic1 message1"); // Should get first message

    // Verify timestamps are in ascending order (FIFO) and in ISO 8601 format
    assert!(topic1_data.messages[0].timestamp <= topic1_data.messages[1].timestamp);
    assert!(topic1_data.messages[0].timestamp.contains("T")); // ISO 8601 format
    assert!(topic1_data.messages[1].timestamp.contains("T")); // ISO 8601 format
}

#[tokio::test]
async fn test_health_check() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);

    let response = helper
        .health_check()
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
    let url = format!("{}/topics/test/records", server.base_url());

    // Test invalid JSON
    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .body("invalid json")
        .send()
        .await
        .expect("Failed to send invalid request");

    assert!(!response.status().is_success());

    // Test missing required 'value' field
    let response = client
        .post(&url)
        .json(&serde_json::json!({
            "key": "test-key"
        }))
        .send()
        .await
        .expect("Failed to send request without value");

    assert!(!response.status().is_success());

    // Test valid minimal request (only value field)
    let response = client
        .post(&url)
        .json(&serde_json::json!({
            "value": "test message"
        }))
        .send()
        .await
        .expect("Failed to send minimal valid request");

    assert!(response.status().is_success());
}

#[tokio::test]
async fn test_consumer_group_isolation() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);
    let topic = "isolation_test_topic";

    // Setup: Create a topic with messages
    helper.setup_topic_with_messages(topic, 5).await;

    // Create consumer groups
    let group1 = "isolation_group_1";
    let group2 = "isolation_group_2";
    helper.setup_consumer_group(group1).await;
    helper.setup_consumer_group(group2).await;

    // Test that consumer groups maintain separate offsets
    // Group 1 polls all messages (no count limit)
    let response = helper
        .poll_consumer_group_messages(group1, topic, None)
        .await
        .unwrap();
    let _group1_data = helper
        .assert_consumer_group_poll_response(
            response,
            5,
            Some(&[
                "Message 0",
                "Message 1",
                "Message 2",
                "Message 3",
                "Message 4",
            ]),
        )
        .await;

    // Group 2 polls only 1 message
    let response = helper
        .poll_consumer_group_messages(group2, topic, Some(1))
        .await
        .unwrap();
    let _group2_data_first = helper
        .assert_consumer_group_poll_response(response, 1, Some(&["Message 0"]))
        .await;

    // Verify offsets are independent: Group 2 should still be able to get remaining messages
    let response = helper
        .poll_consumer_group_messages(group2, topic, Some(4))
        .await
        .unwrap();
    let _group2_data_remaining = helper
        .assert_consumer_group_poll_response(
            response,
            4,
            Some(&["Message 1", "Message 2", "Message 3", "Message 4"]),
        )
        .await;

    // Group 1 should have no new messages since it already consumed everything
    let response = helper
        .poll_consumer_group_messages(group1, topic, None)
        .await
        .unwrap();
    helper
        .assert_consumer_group_poll_response(response, 0, None)
        .await;
}

#[tokio::test]
async fn test_consumer_group_offset_boundaries() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);
    let topic = "boundary_test_topic";
    let group = "boundary_test_group";

    // Setup: Create topic with 3 messages (offsets 0, 1, 2)
    helper.setup_topic_with_messages(topic, 3).await;
    helper.setup_consumer_group(group).await;

    // Test boundary conditions
    let test_cases = [
        (10, 404),       // Beyond available messages
        (u64::MAX, 404), // Extreme value
        (3, 200),        // Valid boundary (end position)
        (1, 200),        // Valid within bounds
    ];

    for (offset, expected_status) in test_cases {
        let response = helper
            .update_consumer_group_offset(group, topic, offset)
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            expected_status,
            "Failed for offset {offset}"
        );
    }
}

#[tokio::test]
async fn test_consumer_group_error_handling() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);

    // Test 1: Operations on nonexistent consumer groups (expect 404)
    let nonexistent_group = "nonexistent_group";
    let test_topic = "test_topic";

    // Try to poll messages from nonexistent consumer group
    let response = helper
        .poll_consumer_group_messages(nonexistent_group, test_topic, None)
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        404,
        "Polling from nonexistent consumer group should return 404"
    );

    // Try to update offset for nonexistent consumer group
    let response = helper
        .update_consumer_group_offset(nonexistent_group, test_topic, 0)
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        404,
        "Updating offset for nonexistent consumer group should return 404"
    );

    // Test 2: Duplicate consumer group creation (expect 400)
    let duplicate_group = "duplicate_test_group";

    // Create the group first time (should succeed)
    helper.setup_consumer_group(duplicate_group).await;

    // Try to create the same group again (should fail with 400)
    let response = helper.create_consumer_group(duplicate_group).await.unwrap();
    assert_eq!(
        response.status(),
        400,
        "Duplicate consumer group creation should return 400"
    );

    // Test 3: Invalid JSON payloads (expect 400 or 422)

    // Invalid JSON for consumer group creation (expect 400)
    let response = helper
        .client
        .post(format!("{}/consumer/invalid-group", helper.base_url))
        .header("Content-Type", "application/json")
        .body("invalid json")
        .send()
        .await
        .unwrap();
    assert!(
        response.status() == 400 || response.status() == 422,
        "Invalid JSON for consumer group creation should return 400 or 422"
    );

    // Invalid JSON for offset update (expect 422 based on axum's deserialization behavior)
    let response = helper
        .client
        .post(format!(
            "{}/consumer/{}/topics/{}/offset",
            helper.base_url, duplicate_group, test_topic
        ))
        .header("Content-Type", "application/json")
        .body("{\"invalid\": \"json\"}")
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        422,
        "Invalid JSON for offset update should return 422"
    );

    // Missing required fields in consumer group creation (expect 422)
    let response = helper
        .client
        .post(format!("{}/consumer/missing-fields-group", helper.base_url))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        422,
        "Missing group_id field should return 422"
    );

    // Test 4: Invalid offset values
    let valid_group = "valid_error_test_group";

    // Create a valid group for offset testing
    helper.setup_consumer_group(valid_group).await;

    // Test negative offset (serde deserializes i64 as u64, may cause 422)
    let response = helper
        .client
        .post(format!(
            "{}/consumer/{}/topics/{}/offset",
            helper.base_url, valid_group, test_topic
        ))
        .json(&serde_json::json!({"offset": -1}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 422, "Negative offset should return 422");

    // Test 5: Valid operations that should succeed for comparison

    // Test getting offset for valid group (should return 404 for nonexistent topic or 200 with offset 0)
    let response = helper
        .get_consumer_group_offset(valid_group, test_topic)
        .await
        .unwrap();
    // Could be 404 if topic doesn't exist yet, or 200 if it does
    assert!(
        response.status() == 404 || response.status() == 200,
        "Getting offset should return 404 or 200"
    );
}

#[tokio::test]
async fn test_consumer_group_empty_topics() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);

    // Create consumer group
    let group = "empty_topic_group";
    helper.setup_consumer_group(group).await;

    // Test 1: Poll from topic that doesn't exist yet
    let nonexistent_topic = "nonexistent_topic";
    let response = helper
        .poll_consumer_group_messages(group, nonexistent_topic, None)
        .await
        .unwrap();
    // Server returns 404 for nonexistent consumer group + topic combination initially
    // But since we created the consumer group, it should succeed and return empty results
    if response.status() == 200 {
        let poll_data: ConsumerGroupPollResponse = response.json().await.unwrap();
        assert_eq!(poll_data.count, 0);
        assert_eq!(poll_data.messages.len(), 0);
        assert_eq!(
            poll_data.new_offset, 0,
            "Offset should remain 0 for nonexistent topic"
        );
    } else {
        // If server returns 404, that's also acceptable behavior for nonexistent topic
        assert_eq!(response.status(), 404);
    }

    // Test 2: Poll from topic with no messages (create topic by posting then ensure it exists)
    let empty_topic = "empty_topic";
    // First, create the topic by posting and then immediately create a new consumer group
    // to test empty topic behavior
    let response = helper
        .post_message(empty_topic, "temp message")
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    // Now create a new consumer group for testing empty behavior
    let empty_group = "empty_test_group";
    helper.setup_consumer_group(empty_group).await;

    // Test polling from the topic - should get the message and advance offset
    let response = helper
        .poll_consumer_group_messages(empty_group, empty_topic, None)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let poll_data: ConsumerGroupPollResponse = response.json().await.unwrap();
    assert_eq!(poll_data.count, 1);
    assert_eq!(poll_data.messages.len(), 1);
    assert_eq!(
        poll_data.new_offset, 1,
        "Offset should advance to 1 after consuming message"
    );

    // Test 3: Now poll again - should get no messages (empty state)
    let response = helper
        .poll_consumer_group_messages(empty_group, empty_topic, None)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let poll_data: ConsumerGroupPollResponse = response.json().await.unwrap();
    assert_eq!(poll_data.count, 0);
    assert_eq!(poll_data.messages.len(), 0);
    assert_eq!(
        poll_data.new_offset, 1,
        "Offset should remain at 1 when no new messages"
    );

    // Test 4: Poll multiple times to ensure offset doesn't change when empty
    for _ in 0..3 {
        let response = helper
            .poll_consumer_group_messages(empty_group, empty_topic, Some(5))
            .await
            .unwrap();
        assert_eq!(response.status(), 200);

        let poll_data: ConsumerGroupPollResponse = response.json().await.unwrap();
        assert_eq!(poll_data.count, 0);
        assert_eq!(poll_data.messages.len(), 0);
        assert_eq!(
            poll_data.new_offset, 1,
            "Offset should consistently remain at 1"
        );
    }

    // Test 5: Test offset updates on empty topics

    // Try to update offset to 0 (should succeed)
    let response = helper
        .update_consumer_group_offset(empty_group, empty_topic, 0)
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "Setting offset to 0 should succeed");

    // Try to update offset to valid position (should succeed)
    let response = helper
        .update_consumer_group_offset(empty_group, empty_topic, 1)
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "Setting offset to valid position should succeed"
    );

    // Try to update offset beyond available messages (should fail)
    let response = helper
        .update_consumer_group_offset(empty_group, empty_topic, 10)
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        404,
        "Setting offset beyond available messages should fail with 404"
    );
}

#[tokio::test]
async fn test_consumer_group_offset_advancement() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);
    let topic = "advancement_test_topic";

    // Setup: Create topic with 5 messages
    helper.setup_topic_with_messages(topic, 5).await;

    // Create consumer group
    let group = "advancement_test_group";
    helper.setup_consumer_group(group).await;

    // Test 1: Poll 1 message, verify offset advances to 1
    let response = helper
        .poll_consumer_group_messages(group, topic, Some(1))
        .await
        .unwrap();
    let poll_data: ConsumerGroupPollResponse = response.json().await.unwrap();

    assert_eq!(poll_data.count, 1);
    assert_eq!(poll_data.messages.len(), 1);
    assert_eq!(poll_data.messages[0].value, "Message 0");
    assert_eq!(
        poll_data.new_offset, 1,
        "Offset should advance to 1 after consuming 1 message"
    );

    // Test 2: Poll 2 more messages, verify offset advances to 3
    let response = helper
        .poll_consumer_group_messages(group, topic, Some(2))
        .await
        .unwrap();
    let poll_data: ConsumerGroupPollResponse = response.json().await.unwrap();

    assert_eq!(poll_data.count, 2);
    assert_eq!(poll_data.messages.len(), 2);
    assert_eq!(poll_data.messages[0].value, "Message 1");
    assert_eq!(poll_data.messages[1].value, "Message 2");
    assert_eq!(
        poll_data.new_offset, 3,
        "Offset should advance to 3 after consuming 2 more messages"
    );

    // Test 3: Poll remaining messages (no count limit), verify offset advances to 5
    let response = helper
        .poll_consumer_group_messages(group, topic, None)
        .await
        .unwrap();
    let poll_data: ConsumerGroupPollResponse = response.json().await.unwrap();

    assert_eq!(poll_data.count, 2);
    assert_eq!(poll_data.messages.len(), 2);
    assert_eq!(poll_data.messages[0].value, "Message 3");
    assert_eq!(poll_data.messages[1].value, "Message 4");
    assert_eq!(
        poll_data.new_offset, 5,
        "Offset should advance to 5 after consuming all remaining messages"
    );

    // Test 4: Poll when at end of log (no new messages)
    let response = helper
        .poll_consumer_group_messages(group, topic, None)
        .await
        .unwrap();
    let poll_data: ConsumerGroupPollResponse = response.json().await.unwrap();

    assert_eq!(poll_data.count, 0);
    assert_eq!(poll_data.messages.len(), 0);
    assert_eq!(
        poll_data.new_offset, 5,
        "Offset should remain at 5 when no new messages"
    );

    // Test 5: Add new messages and verify offset continues from where it left off
    for i in 5..7 {
        helper
            .post_message(topic, &format!("Message {i}"))
            .await
            .unwrap();
    }

    let response = helper
        .poll_consumer_group_messages(group, topic, None)
        .await
        .unwrap();
    let poll_data: ConsumerGroupPollResponse = response.json().await.unwrap();

    assert_eq!(poll_data.count, 2);
    assert_eq!(poll_data.messages.len(), 2);
    assert_eq!(poll_data.messages[0].value, "Message 5");
    assert_eq!(poll_data.messages[1].value, "Message 6");
    assert_eq!(
        poll_data.new_offset, 7,
        "Offset should advance to 7 after consuming new messages"
    );
}

#[tokio::test]
async fn test_consumer_group_concurrent_operations() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);
    let topic = "concurrent_test_topic";

    // Setup: Create topic with 10 messages
    helper.setup_topic_with_messages(topic, 10).await;

    // Create 3 consumer groups
    let groups = [
        "concurrent_group_1",
        "concurrent_group_2",
        "concurrent_group_3",
    ];
    for group in groups {
        helper.setup_consumer_group(group).await;
    }

    // Test concurrent polling: each group polls all messages in 3 rounds
    let handles: Vec<_> = groups
        .iter()
        .map(|&group| {
            let helper = TestHelper::new(&server);
            tokio::spawn(async move {
                let mut all_messages = Vec::new();
                for _ in 0..3 {
                    let response = helper
                        .poll_consumer_group_messages(group, topic, Some(4))
                        .await
                        .unwrap();
                    let poll_data: ConsumerGroupPollResponse = response.json().await.unwrap();
                    let messages = poll_data.messages;

                    // Verify FIFO ordering within each poll
                    for i in 1..messages.len() {
                        assert!(
                            messages[i - 1].offset < messages[i].offset,
                            "Messages should maintain FIFO order"
                        );
                    }
                    all_messages.extend(messages);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                (group, all_messages)
            })
        })
        .collect();

    // Verify each group consumed all messages correctly
    for handle in handles {
        let (group, messages) = handle.await.unwrap();
        assert_eq!(
            messages.len(),
            10,
            "Group {group} should consume all messages"
        );

        // Verify content and uniqueness
        for (i, msg) in messages.iter().enumerate() {
            assert_eq!(msg.value, format!("Message {i}"));
        }
        let ids: std::collections::HashSet<_> = messages.iter().map(|m| m.offset).collect();
        assert_eq!(
            ids.len(),
            10,
            "Group {group} should have unique message IDs"
        );
    }

    // Stress test: 20 concurrent operations across the groups
    let stress_handles: Vec<_> = (0..20)
        .map(|i| {
            let helper = TestHelper::new(&server);
            let group = format!("concurrent_group_{}", (i % 3) + 1);
            tokio::spawn(async move {
                helper
                    .poll_consumer_group_messages(&group, topic, Some(1))
                    .await
                    .unwrap()
                    .status()
                    == 200
            })
        })
        .collect();

    // All stress operations should succeed
    for handle in stress_handles {
        assert!(
            handle.await.unwrap(),
            "High-concurrency operation should succeed"
        );
    }
}

#[tokio::test]
async fn test_message_size_and_validation_limits() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);
    let topic = "validation_test_topic";

    // Test 1: Valid message with maximum key size (1024 chars)
    let max_key = "x".repeat(1024);
    let response = helper
        .post_message_with_record(
            topic,
            Some(max_key.clone()),
            "Valid message with max key size",
            None,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "Max key size should be accepted");

    // Test 2: Key exceeding limit should return 400 Bad Request
    let oversized_key = "x".repeat(1025); // 1025 chars, exceeds 1024 limit
    let response = helper
        .post_message_with_record(
            topic,
            Some(oversized_key),
            "Message with oversized key",
            None,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 400, "Oversized key should be rejected");

    // Test 2b: Value exceeding limit should return 400 Bad Request
    let oversized_value = "x".repeat(1_048_577); // Exceeds 1MB limit
    let response = helper
        .post_message_with_record(topic, None, &oversized_value, None)
        .await
        .unwrap();
    assert_eq!(response.status(), 400, "Oversized value should be rejected");

    // Test 2c: Header value exceeding limit should return 400 Bad Request
    let mut oversized_header = std::collections::HashMap::new();
    oversized_header.insert("large_header".to_string(), "z".repeat(1025)); // Exceeds 1024 limit
    let response = helper
        .post_message_with_record(
            topic,
            None,
            "Message with oversized header",
            Some(oversized_header),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        400,
        "Oversized header should be rejected"
    );

    // Test 3: Valid message with large value (within reasonable limits)
    let large_value = "Message content ".repeat(1000); // ~16KB
    let response = helper
        .post_message_with_record(topic, None, &large_value, None)
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "Large message should be accepted");

    // Test 4: Valid headers with multiple entries
    let mut headers = std::collections::HashMap::new();
    for i in 0..10 {
        headers.insert(format!("header_{i}"), format!("value_{i}"));
    }
    let response = helper
        .post_message_with_record(
            topic,
            Some("test_key".to_string()),
            "Message with multiple headers",
            Some(headers),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "Multiple headers should be accepted"
    );

    // Test 5: Header with maximum value size (1024 chars)
    let mut max_header = std::collections::HashMap::new();
    max_header.insert("large_header".to_string(), "y".repeat(1024));
    let response = helper
        .post_message_with_record(
            topic,
            None,
            "Message with max header size",
            Some(max_header),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "Max header value size should be accepted"
    );

    // Verify all messages were stored correctly
    let response = helper.poll_messages(topic, None).await.unwrap();
    assert_eq!(response.status(), 200);
    let poll_data: PollMessagesResponse = response.json().await.unwrap();
    assert_eq!(poll_data.count, 4, "All valid messages should be stored");

    // Verify the max key message
    let max_key_message = &poll_data.messages[0];
    assert_eq!(max_key_message.key.as_ref().unwrap(), &max_key);
    assert_eq!(max_key_message.value, "Valid message with max key size");
}

#[tokio::test]
async fn test_concurrent_message_posting() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);
    let topic = "concurrent_posting_topic";

    // Test concurrent posting to same topic
    let post_handles: Vec<_> = (0..20)
        .map(|i| {
            let helper = TestHelper::new(&server);
            tokio::spawn(async move {
                let key = if i % 2 == 0 {
                    Some(format!("key_{i}"))
                } else {
                    None
                };
                let mut headers = std::collections::HashMap::new();
                headers.insert("thread".to_string(), format!("thread_{i}"));
                headers.insert("index".to_string(), i.to_string());

                let response = helper
                    .post_message_with_record(
                        topic,
                        key,
                        &format!("Concurrent message {i}"),
                        Some(headers),
                    )
                    .await
                    .unwrap();
                (i, response.status() == 200)
            })
        })
        .collect();

    // Wait for all posts to complete
    let mut results = Vec::new();
    for handle in post_handles {
        let (index, success) = handle.await.unwrap();
        results.push((index, success));
    }

    // Verify all posts succeeded
    for (index, success) in &results {
        assert!(success, "Concurrent post {index} should succeed");
    }

    // Verify message ordering and content
    let response = helper.poll_messages(topic, None).await.unwrap();
    assert_eq!(response.status(), 200);
    let poll_data: PollMessagesResponse = response.json().await.unwrap();
    assert_eq!(
        poll_data.count, 20,
        "All concurrent messages should be stored"
    );

    // Verify offset sequence (should be 0-19)
    for (i, message) in poll_data.messages.iter().enumerate() {
        assert_eq!(message.offset, i as u64, "Offsets should be sequential");
        assert!(
            message.value.starts_with("Concurrent message"),
            "Message content should be preserved"
        );

        // Verify headers were preserved
        if let Some(ref headers) = message.headers {
            assert!(
                headers.contains_key("thread"),
                "Thread header should be present"
            );
            assert!(
                headers.contains_key("index"),
                "Index header should be present"
            );
        }
    }

    // Test concurrent posting to different topics
    let multi_topic_handles: Vec<_> = (0..15)
        .map(|i| {
            let helper = TestHelper::new(&server);
            let topic_name = format!("concurrent_topic_{}", i % 3);
            tokio::spawn(async move {
                let response = helper
                    .post_message(&topic_name, &format!("Multi-topic message {i}"))
                    .await
                    .unwrap();
                (topic_name, response.status() == 200)
            })
        })
        .collect();

    // Verify multi-topic concurrent posting
    for handle in multi_topic_handles {
        let (topic_name, success) = handle.await.unwrap();
        assert!(success, "Multi-topic post to {topic_name} should succeed");
    }

    // Verify each topic received the expected number of messages
    for topic_index in 0..3 {
        let topic_name = format!("concurrent_topic_{topic_index}");
        let response = helper.poll_messages(&topic_name, None).await.unwrap();
        assert_eq!(response.status(), 200);
        let poll_data: PollMessagesResponse = response.json().await.unwrap();
        assert_eq!(poll_data.count, 5, "Each topic should have 5 messages");
    }
}

#[tokio::test]
async fn test_message_structure_edge_cases() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);
    let topic = "edge_case_topic";

    // Test 1: Empty key (should be treated as None)
    let response = helper
        .post_message_with_record(topic, Some("".to_string()), "Message with empty key", None)
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "Empty key should be accepted");

    // Test 2: Key with special characters
    let special_key = "key-with_special.chars@domain.com:123";
    let response = helper
        .post_message_with_record(
            topic,
            Some(special_key.to_string()),
            "Message with special chars in key",
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "Special characters in key should be accepted"
    );

    // Test 3: Headers with special characters and empty values
    let mut special_headers = std::collections::HashMap::new();
    special_headers.insert("content-type".to_string(), "application/json".to_string());
    special_headers.insert("x-custom-header".to_string(), "".to_string()); // Empty header value
    special_headers.insert("unicode-header".to_string(), "测试数据".to_string()); // Unicode

    let response = helper
        .post_message_with_record(
            topic,
            None,
            "Message with special headers",
            Some(special_headers.clone()),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "Special headers should be accepted");

    // Test 4: Message with only value (minimal valid request)
    let response = helper
        .client
        .post(format!("{}/topics/{}/records", helper.base_url, topic))
        .json(&serde_json::json!({
            "value": "Minimal message"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "Minimal message should be accepted");

    // Test 5: Message with null key and headers (explicit nulls)
    let response = helper
        .client
        .post(format!("{}/topics/{}/records", helper.base_url, topic))
        .json(&serde_json::json!({
            "key": null,
            "value": "Message with explicit nulls",
            "headers": null
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "Explicit nulls should be accepted");

    // Verify all messages were stored with correct structure
    let response = helper.poll_messages(topic, None).await.unwrap();
    assert_eq!(response.status(), 200);
    let poll_data: PollMessagesResponse = response.json().await.unwrap();
    assert_eq!(
        poll_data.count, 5,
        "All edge case messages should be stored"
    );

    // Verify specific message structures
    let messages = &poll_data.messages;

    // First message: empty key
    assert_eq!(messages[0].key.as_deref(), Some(""));
    assert_eq!(messages[0].value, "Message with empty key");

    // Second message: special key
    assert_eq!(messages[1].key.as_ref().unwrap(), &special_key);
    assert_eq!(messages[1].value, "Message with special chars in key");

    // Third message: special headers
    assert_eq!(messages[2].value, "Message with special headers");
    let headers = messages[2].headers.as_ref().unwrap();
    assert_eq!(headers.get("content-type").unwrap(), "application/json");
    assert_eq!(headers.get("x-custom-header").unwrap(), "");
    assert_eq!(headers.get("unicode-header").unwrap(), "测试数据");

    // Fourth and fifth messages: should have None keys and headers
    assert!(messages[3].key.is_none());
    assert!(messages[3].headers.is_none());
    assert_eq!(messages[3].value, "Minimal message");

    assert!(messages[4].key.is_none());
    assert!(messages[4].headers.is_none());
    assert_eq!(messages[4].value, "Message with explicit nulls");
}

#[tokio::test]
async fn test_replay_functionality_basic_polling() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);

    // Post several messages
    for i in 0..5 {
        let response = helper
            .post_message_with_record("replay-test", None, &format!("message {i}"), None)
            .await
            .unwrap();
        assert!(response.status().is_success());
    }

    // Test polling from offset 0 (beginning)
    let response = helper
        .client
        .get(format!(
            "{}/topics/replay-test/messages?from_offset=0",
            helper.base_url
        ))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());

    let poll_response: PollMessagesResponse = response.json().await.unwrap();
    assert_eq!(poll_response.count, 5);
    assert_eq!(poll_response.messages[0].value, "message 0");
    assert_eq!(poll_response.messages[0].offset, 0);
    assert_eq!(poll_response.messages[4].value, "message 4");
    assert_eq!(poll_response.messages[4].offset, 4);

    // Test polling from offset 2 (middle)
    let response = helper
        .client
        .get(format!(
            "{}/topics/replay-test/messages?from_offset=2",
            helper.base_url
        ))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());

    let poll_response: PollMessagesResponse = response.json().await.unwrap();
    assert_eq!(poll_response.count, 3);
    assert_eq!(poll_response.messages[0].value, "message 2");
    assert_eq!(poll_response.messages[0].offset, 2);
    assert_eq!(poll_response.messages[2].value, "message 4");
    assert_eq!(poll_response.messages[2].offset, 4);

    // Test polling with count limit from offset
    let response = helper
        .client
        .get(format!(
            "{}/topics/replay-test/messages?from_offset=1&count=2",
            helper.base_url
        ))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());

    let poll_response: PollMessagesResponse = response.json().await.unwrap();
    assert_eq!(poll_response.count, 2);
    assert_eq!(poll_response.messages[0].value, "message 1");
    assert_eq!(poll_response.messages[0].offset, 1);
    assert_eq!(poll_response.messages[1].value, "message 2");
    assert_eq!(poll_response.messages[1].offset, 2);
}

#[tokio::test]
async fn test_consumer_group_replay_functionality() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);

    // Create consumer group
    let response = helper.create_consumer_group("replay-group").await.unwrap();
    assert_eq!(response.status(), 200);

    // Post several messages
    for i in 0..4 {
        let response = helper
            .post_message_with_record("consumer-replay-test", None, &format!("message {i}"), None)
            .await
            .unwrap();
        assert!(response.status().is_success());
    }

    // Normal consumer group polling (should advance offset)
    let response = helper
        .poll_consumer_group_messages("replay-group", "consumer-replay-test", Some(2))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let poll_response: ConsumerGroupPollResponse = response.json().await.unwrap();
    assert_eq!(poll_response.count, 2);
    assert_eq!(poll_response.new_offset, 2); // Offset advanced
    assert_eq!(poll_response.messages[0].value, "message 0");
    assert_eq!(poll_response.messages[1].value, "message 1");

    // Replay from offset 0 (should NOT advance the consumer group offset)
    let response = helper
        .client
        .get(format!("{}/consumer/replay-group/topics/consumer-replay-test?from_offset=0&count=3", helper.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let poll_response: ConsumerGroupPollResponse = response.json().await.unwrap();
    assert_eq!(poll_response.count, 3);
    assert_eq!(poll_response.new_offset, 2); // Offset should remain unchanged
    assert_eq!(poll_response.messages[0].value, "message 0");
    assert_eq!(poll_response.messages[1].value, "message 1");
    assert_eq!(poll_response.messages[2].value, "message 2");

    // Verify consumer group offset is still at 2 by doing normal polling
    let response = helper
        .poll_consumer_group_messages("replay-group", "consumer-replay-test", None)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let poll_response: ConsumerGroupPollResponse = response.json().await.unwrap();
    assert_eq!(poll_response.count, 2);
    assert_eq!(poll_response.new_offset, 4); // Now advanced to end
    assert_eq!(poll_response.messages[0].value, "message 2");
    assert_eq!(poll_response.messages[1].value, "message 3");
}

#[tokio::test]
async fn test_replay_from_invalid_offset() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);

    // Post one message
    let response = helper
        .post_message_with_record("invalid-offset-test", None, "single message", None)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    // Try to replay from offset beyond available messages
    let response = helper
        .client
        .get(format!(
            "{}/topics/invalid-offset-test/messages?from_offset=5",
            helper.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let poll_response: PollMessagesResponse = response.json().await.unwrap();
    assert_eq!(poll_response.count, 0); // Should return empty result, not error
    assert!(poll_response.messages.is_empty());
}

#[tokio::test]
async fn test_replay_nonexistent_topic() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);

    // Try to replay from nonexistent topic
    let response = helper
        .client
        .get(format!(
            "{}/topics/nonexistent/messages?from_offset=0",
            helper.base_url
        ))
        .send()
        .await
        .unwrap();

    // The important test: TopicNotFound should map to 404 status code
    assert_eq!(response.status(), 404);
}
