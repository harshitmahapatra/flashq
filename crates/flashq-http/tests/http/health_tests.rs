use super::test_utilities::{TestBroker, TestClient};

#[tokio::test]
async fn test_health_check() {
    let broker = TestBroker::start()
        .await
        .expect("Failed to start test broker");
    let helper = TestClient::new(&broker);

    let response = helper.health_check().await.unwrap();
    assert_eq!(response.status(), 200);

    let health_data: serde_json::Value = response.json().await.unwrap();
    assert_eq!(health_data["status"], "healthy");
    assert_eq!(health_data["service"], "flashq");
    assert!(health_data["timestamp"].as_u64().is_some());
}
