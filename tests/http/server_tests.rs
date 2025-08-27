use super::test_utilities::{TestClient, TestServer};

// Health test moved to health_tests.rs

#[tokio::test]
async fn test_server_error_handling() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);

    let response = helper
        .client
        .get(format!("{}/nonexistent-endpoint", helper.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);

    let response = helper
        .client
        .post(format!("{}/topics/test/records", helper.base_url))
        .json(&serde_json::json!({"invalid": "request"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 422);
}

#[tokio::test]
async fn test_malformed_requests() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);

    let response = helper
        .client
        .post(format!("{}/topics/test/records", helper.base_url))
        .body("invalid json")
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);

    let response = helper
        .client
        .post(format!("{}/topics/test/records", helper.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 415);
}
