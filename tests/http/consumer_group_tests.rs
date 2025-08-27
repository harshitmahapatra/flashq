use super::test_utilities::{TestClient, TestServer};
use flashq::http::*;

#[tokio::test]
async fn test_consumer_group_operations() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    let group_id = "test_group";

    let response = helper.create_consumer_group(group_id).await.unwrap();
    assert_eq!(response.status(), 200);
    let group_response: ConsumerGroupResponse = response.json().await.unwrap();
    assert_eq!(group_response.group_id, group_id);

    let response = helper.leave_consumer_group(group_id).await.unwrap();
    assert_eq!(response.status(), 204);

    let response = helper.get_consumer_group_offset(group_id, "test").await;
    assert!(response.is_err() || !response.unwrap().status().is_success());
}

#[tokio::test]
async fn test_multiple_consumer_groups() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    
    // Create multiple consumer groups
    let group1 = "group_1";
    let group2 = "group_2";
    
    let response1 = helper.create_consumer_group(group1).await.unwrap();
    assert_eq!(response1.status(), 200);
    
    let response2 = helper.create_consumer_group(group2).await.unwrap();
    assert_eq!(response2.status(), 200);
    
    // Verify both groups exist independently
    let group1_response: ConsumerGroupResponse = response1.json().await.unwrap();
    let group2_response: ConsumerGroupResponse = response2.json().await.unwrap();
    
    assert_eq!(group1_response.group_id, group1);
    assert_eq!(group2_response.group_id, group2);
    
    // Clean up
    let _ = helper.leave_consumer_group(group1).await;
    let _ = helper.leave_consumer_group(group2).await;
}

#[tokio::test]
async fn test_consumer_group_error_cases() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    
    // Test invalid group operations
    let invalid_group = "non_existent_group";
    
    // Try to fetch from non-existent group
    let response = helper.fetch_records_for_consumer_group(invalid_group, "test", None).await.unwrap();
    assert!(!response.status().is_success());
    
    // Try to get offset from non-existent group
    let response = helper.get_consumer_group_offset(invalid_group, "test").await;
    assert!(response.is_err() || !response.unwrap().status().is_success());
    
    // Try to leave non-existent group
    let response = helper.leave_consumer_group(invalid_group).await.unwrap();
    assert!(!response.status().is_success());
}