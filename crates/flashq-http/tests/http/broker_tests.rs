use super::test_utilities::{TestBroker, TestClient};
use flashq_http::TopicsResponse;
// Health test moved to health_tests.rs

#[tokio::test]
async fn test_broker_error_handling() {
    let broker = TestBroker::start()
        .await
        .expect("Failed to start test broker");
    let helper = TestClient::new(&broker);

    let response = helper
        .client
        .get(format!("{}/nonexistent-endpoint", helper.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);

    let response = helper
        .client
        .post(format!("{}/topic/test/record", helper.base_url))
        .json(&serde_json::json!({"invalid": "request"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 422);
}

#[tokio::test]
async fn test_malformed_requests() {
    let broker = TestBroker::start()
        .await
        .expect("Failed to start test broker");
    let helper = TestClient::new(&broker);

    let response = helper
        .client
        .post(format!("{}/topic/test/record", helper.base_url))
        .body("invalid json")
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);

    let response = helper
        .client
        .post(format!("{}/topic/test/record", helper.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 415);
}

#[tokio::test]
async fn test_get_topics_empty_initially() {
    // Setup
    let broker = TestBroker::start()
        .await
        .expect("Failed to start test broker");
    let helper = TestClient::new(&broker);

    // Action
    let response = helper
        .client
        .get(format!("{}/topics", helper.base_url))
        .send()
        .await
        .unwrap();

    // Expectation
    assert_eq!(response.status(), 200);
    let topics_response: TopicsResponse = response.json().await.unwrap();
    assert!(topics_response.topics.is_empty());
}

#[tokio::test]
async fn test_get_topics_after_creating_single_topic() {
    // Setup
    let broker = TestBroker::start()
        .await
        .expect("Failed to start test broker");
    let helper = TestClient::new(&broker);

    let record = serde_json::json!({
        "records": [
            {
                "key": "test-key",
                "value": "test-value",
                "headers": null
            }
        ]
    });

    // Action
    helper
        .client
        .post(format!("{}/topic/test-topic/record", helper.base_url))
        .json(&record)
        .send()
        .await
        .unwrap();

    let response = helper
        .client
        .get(format!("{}/topics", helper.base_url))
        .send()
        .await
        .unwrap();

    // Expectation
    assert_eq!(response.status(), 200);
    let topics_response: TopicsResponse = response.json().await.unwrap();
    assert_eq!(topics_response.topics.len(), 1);
    assert_eq!(topics_response.topics[0], "test-topic");
}

#[tokio::test]
async fn test_get_topics_tracks_multiple_topics() {
    // Setup
    let broker = TestBroker::start()
        .await
        .expect("Failed to start test broker");
    let helper = TestClient::new(&broker);

    let record = serde_json::json!({
        "records": [
            {
                "key": "test-key",
                "value": "test-value",
                "headers": null
            }
        ]
    });

    // Action
    helper
        .client
        .post(format!("{}/topic/first-topic/record", helper.base_url))
        .json(&record)
        .send()
        .await
        .unwrap();

    helper
        .client
        .post(format!("{}/topic/second-topic/record", helper.base_url))
        .json(&record)
        .send()
        .await
        .unwrap();

    let response = helper
        .client
        .get(format!("{}/topics", helper.base_url))
        .send()
        .await
        .unwrap();

    // Expectation
    assert_eq!(response.status(), 200);
    let topics_response: TopicsResponse = response.json().await.unwrap();
    assert_eq!(topics_response.topics.len(), 2);
    assert!(topics_response.topics.contains(&"first-topic".to_string()));
    assert!(topics_response.topics.contains(&"second-topic".to_string()));
}
