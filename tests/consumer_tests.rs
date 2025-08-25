mod test_helpers;

use flashq::api::*;
use test_helpers::{TestClient, TestServer};

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
async fn test_consumer_group_record_fetching() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    let group_id = "fetch_test_group";
    let topic = "fetch_test_topic";

    helper.create_consumer_group(group_id).await.unwrap();

    for i in 0..10 {
        helper
            .post_record(topic, &format!("Group Record {i}"))
            .await
            .unwrap();
    }

    let response = helper
        .fetch_records_for_consumer_group(group_id, topic, None)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let fetch_response: FetchResponse = response.json().await.unwrap();
    assert_eq!(fetch_response.records.len(), 10);
    assert_eq!(fetch_response.next_offset, 0); // Offset not advanced until commit
    assert_eq!(fetch_response.high_water_mark, 10);

    // Commit offset to 5
    helper
        .update_consumer_group_offset(group_id, topic, 5)
        .await
        .unwrap();

    // Verify offset was advanced after commit
    let response = helper
        .fetch_records_for_consumer_group(group_id, topic, Some(3))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let fetch_response: FetchResponse = response.json().await.unwrap();
    assert_eq!(fetch_response.records.len(), 3);
    assert_eq!(fetch_response.next_offset, 5); // Offset reflects committed position

    // Commit to offset 8 and verify it advances
    helper
        .update_consumer_group_offset(group_id, topic, 8)
        .await
        .unwrap();

    let response = helper
        .fetch_records_for_consumer_group(group_id, topic, Some(2))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let fetch_response: FetchResponse = response.json().await.unwrap();
    assert_eq!(fetch_response.records.len(), 2); // Records from offset 8-9
    assert_eq!(fetch_response.next_offset, 8); // Offset advanced to committed position
}

#[tokio::test]
async fn test_consumer_group_from_offset_fetching() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    let group_id = "offset_fetch_group";
    let topic = "offset_fetch_topic";

    helper.create_consumer_group(group_id).await.unwrap();

    for i in 0..8 {
        helper
            .post_record(topic, &format!("Offset Record {i}"))
            .await
            .unwrap();
    }

    let response = helper
        .fetch_records_for_consumer_group_with_options(group_id, topic, Some(3), Some(3))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let fetch_response: FetchResponse = response.json().await.unwrap();
    assert_eq!(fetch_response.records.len(), 3);
    assert_eq!(fetch_response.records[0].offset, 3);
    assert_eq!(fetch_response.records[0].record.value, "Offset Record 3");
    assert_eq!(fetch_response.records[2].offset, 5);

    let response = helper
        .fetch_records_for_consumer_group_with_options(group_id, topic, Some(10), Some(5))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let fetch_response: FetchResponse = response.json().await.unwrap();
    assert_eq!(fetch_response.records.len(), 0);
}

#[tokio::test]
async fn test_multiple_consumer_groups() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    let topic = "multi_group_topic";

    for group_id in ["group_a", "group_b", "group_c"] {
        helper.create_consumer_group(group_id).await.unwrap();
    }

    for i in 0..6 {
        helper
            .post_record(topic, &format!("Multi Group Record {i}"))
            .await
            .unwrap();
    }

    helper
        .update_consumer_group_offset("group_a", topic, 2)
        .await
        .unwrap();
    helper
        .update_consumer_group_offset("group_b", topic, 4)
        .await
        .unwrap();

    let response_a = helper
        .get_consumer_group_offset("group_a", topic)
        .await
        .unwrap();
    let offset_a: OffsetResponse = response_a.json().await.unwrap();
    assert_eq!(offset_a.committed_offset, 2);
    assert_eq!(offset_a.lag, 4);

    let response_b = helper
        .get_consumer_group_offset("group_b", topic)
        .await
        .unwrap();
    let offset_b: OffsetResponse = response_b.json().await.unwrap();
    assert_eq!(offset_b.committed_offset, 4);
    assert_eq!(offset_b.lag, 2);

    let response_c = helper
        .get_consumer_group_offset("group_c", topic)
        .await
        .unwrap();
    let offset_c: OffsetResponse = response_c.json().await.unwrap();
    assert_eq!(offset_c.committed_offset, 0);
    assert_eq!(offset_c.lag, 6);
}

#[tokio::test]
async fn test_consumer_group_error_cases() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);

    let response = helper
        .get_consumer_group_offset("nonexistent_group", "test_topic")
        .await;
    assert!(response.is_err() || !response.unwrap().status().is_success());

    let response = helper
        .update_consumer_group_offset("nonexistent_group", "test_topic", 5)
        .await;
    assert!(response.is_err() || !response.unwrap().status().is_success());

    let response = helper
        .fetch_records_for_consumer_group("nonexistent_group", "test_topic", None)
        .await;
    assert!(response.is_err() || !response.unwrap().status().is_success());

    let response = helper.leave_consumer_group("nonexistent_group").await;
    assert!(response.is_err() || !response.unwrap().status().is_success());
}

#[tokio::test]
async fn test_consumer_group_edge_cases() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    let group_id = "edge_case_group";
    let topic = "edge_case_topic";

    helper.create_consumer_group(group_id).await.unwrap();

    // Test fetching from non-existent topic (should return 404)
    let response = helper
        .fetch_records_for_consumer_group(group_id, topic, None)
        .await
        .unwrap();
    assert_eq!(response.status(), 404);

    helper.post_record(topic, "Single record").await.unwrap();

    let response = helper
        .fetch_records_for_consumer_group(group_id, topic, Some(1))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let fetch_response: FetchResponse = response.json().await.unwrap();
    assert_eq!(fetch_response.records.len(), 1);
    assert_eq!(fetch_response.records[0].record.value, "Single record");

    helper
        .update_consumer_group_offset(group_id, topic, 1)
        .await
        .unwrap();

    let response = helper
        .fetch_records_for_consumer_group(group_id, topic, None)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let fetch_response: FetchResponse = response.json().await.unwrap();
    assert_eq!(fetch_response.records.len(), 0);
    assert_eq!(fetch_response.next_offset, 1);
}
