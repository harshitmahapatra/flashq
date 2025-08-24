mod test_helpers;

use test_helpers::{TestHelper, TestServer};

#[tokio::test]
async fn test_health_check() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);

    let response = helper.health_check().await.unwrap();
    assert_eq!(response.status(), 200);

    let health_data: serde_json::Value = response.json().await.unwrap();
    assert_eq!(health_data["status"], "healthy");
    assert_eq!(health_data["service"], "flashq");
    assert!(health_data["timestamp"].as_u64().is_some());
}

#[tokio::test]
async fn test_server_error_handling() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);

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
    let helper = TestHelper::new(&server);

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
