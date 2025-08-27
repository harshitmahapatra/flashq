use super::test_utilities::{TestClient, TestServer};
use flashq::http::*;

#[tokio::test]
async fn test_consumer_group_offset_management() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    let group_id = "offset_test_group";
    let topic = "offset_test_topic";

    helper.create_consumer_group(group_id).await.unwrap();

    for i in 0..5 {
        helper
            .post_record(topic, &format!("Record {i}"))
            .await
            .unwrap();
    }

    let response = helper
        .get_consumer_group_offset(group_id, topic)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let offset_response: OffsetResponse = response.json().await.unwrap();
    assert_eq!(offset_response.committed_offset, 0);
    assert_eq!(offset_response.high_water_mark, 5);
    assert_eq!(offset_response.lag, 5);

    let response = helper
        .update_consumer_group_offset(group_id, topic, 3)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let response = helper
        .get_consumer_group_offset(group_id, topic)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let offset_response: OffsetResponse = response.json().await.unwrap();
    assert_eq!(offset_response.committed_offset, 3);
    assert_eq!(offset_response.lag, 2);
}

#[tokio::test]
async fn test_offset_validation_and_edge_cases() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    let group_id = "validation_group";
    let topic = "validation_topic";

    helper.create_consumer_group(group_id).await.unwrap();

    // Post some records
    for i in 0..3 {
        helper
            .post_record(topic, &format!("Validation Record {i}"))
            .await
            .unwrap();
    }

    // Test setting offset beyond high water mark - this should be rejected
    let response = helper
        .update_consumer_group_offset(group_id, topic, 10)
        .await
        .unwrap();
    // Server rejects offsets beyond high water mark
    assert_eq!(response.status(), 500);

    // Test setting offset to 0 (beginning)
    let response = helper
        .update_consumer_group_offset(group_id, topic, 0)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let response = helper
        .get_consumer_group_offset(group_id, topic)
        .await
        .unwrap();
    let offset_response: OffsetResponse = response.json().await.unwrap();
    assert_eq!(offset_response.committed_offset, 0);
    assert_eq!(offset_response.lag, 3);
}

#[tokio::test]
async fn test_multiple_topic_offsets() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    let group_id = "multi_topic_group";
    let topic1 = "topic_one";
    let topic2 = "topic_two";

    helper.create_consumer_group(group_id).await.unwrap();

    // Post records to both topics
    for i in 0..3 {
        helper
            .post_record(topic1, &format!("Topic1 Record {i}"))
            .await
            .unwrap();
        helper
            .post_record(topic2, &format!("Topic2 Record {i}"))
            .await
            .unwrap();
    }

    // Set different offsets for each topic
    helper
        .update_consumer_group_offset(group_id, topic1, 1)
        .await
        .unwrap();
    helper
        .update_consumer_group_offset(group_id, topic2, 2)
        .await
        .unwrap();

    // Verify offsets are independent
    let response1 = helper
        .get_consumer_group_offset(group_id, topic1)
        .await
        .unwrap();
    let offset_response1: OffsetResponse = response1.json().await.unwrap();
    assert_eq!(offset_response1.committed_offset, 1);
    assert_eq!(offset_response1.lag, 2);

    let response2 = helper
        .get_consumer_group_offset(group_id, topic2)
        .await
        .unwrap();
    let offset_response2: OffsetResponse = response2.json().await.unwrap();
    assert_eq!(offset_response2.committed_offset, 2);
    assert_eq!(offset_response2.lag, 1);
}

#[tokio::test]
async fn test_offset_operations_for_nonexistent_groups() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    let invalid_group = "nonexistent_group";
    let topic = "test_topic";

    // Try to get offset for non-existent group
    let response = helper.get_consumer_group_offset(invalid_group, topic).await;
    assert!(response.is_err() || !response.unwrap().status().is_success());

    // Try to update offset for non-existent group
    let response = helper
        .update_consumer_group_offset(invalid_group, topic, 5)
        .await;
    assert!(response.is_err() || !response.unwrap().status().is_success());
}
