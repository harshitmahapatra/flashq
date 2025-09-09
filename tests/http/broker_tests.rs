use super::test_utilities::{TestBroker, TestClient};

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
