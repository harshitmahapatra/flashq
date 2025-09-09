use super::test_utilities::{TestBroker, TestClient};
use flashq_http::*;

#[tokio::test]
async fn test_consumer_group_record_fetching() {
    let broker = TestBroker::start()
        .await
        .expect("Failed to start test broker");
    let helper = TestClient::new(&broker);
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
    let broker = TestBroker::start()
        .await
        .expect("Failed to start test broker");
    let helper = TestClient::new(&broker);
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
        .fetch_records_for_consumer_group_by_offset(group_id, topic, Some(3), Some(3))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let fetch_response: FetchResponse = response.json().await.unwrap();
    assert_eq!(fetch_response.records.len(), 3);
    assert_eq!(fetch_response.records[0].offset, 3);
    assert_eq!(fetch_response.records[0].record.value, "Offset Record 3");
    assert_eq!(fetch_response.records[2].offset, 5);

    let response = helper
        .fetch_records_for_consumer_group_by_offset(group_id, topic, Some(10), Some(5))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let fetch_response: FetchResponse = response.json().await.unwrap();
    assert_eq!(fetch_response.records.len(), 0);
}

#[tokio::test]
async fn test_basic_polling_for_testing() {
    let broker = TestBroker::start()
        .await
        .expect("Failed to start test broker");
    let helper = TestClient::new(&broker);
    let topic = "poll_test_topic";

    // Post some records
    for i in 0..5 {
        helper
            .post_record(topic, &format!("Poll Record {i}"))
            .await
            .unwrap();
    }

    // Use the basic polling method that creates temporary consumer groups
    let response = helper
        .poll_records_for_testing(topic, Some(3))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let poll_data = helper
        .assert_poll_response(
            response,
            3,
            Some(&["Poll Record 0", "Poll Record 1", "Poll Record 2"]),
        )
        .await;

    assert_eq!(poll_data.records.len(), 3);
    assert_eq!(poll_data.high_water_mark, 5);
}

#[tokio::test]
async fn test_polling_with_max_records_limit() {
    let broker = TestBroker::start()
        .await
        .expect("Failed to start test broker");
    let helper = TestClient::new(&broker);
    let topic = "limit_test_topic";

    // Post 10 records
    for i in 0..10 {
        helper
            .post_record(topic, &format!("Limit Record {i}"))
            .await
            .unwrap();
    }

    // Test different max_records limits
    let response = helper
        .poll_records_for_testing(topic, Some(2))
        .await
        .unwrap();
    let poll_data = helper.assert_poll_response(response, 2, None).await;
    assert_eq!(poll_data.records.len(), 2);

    let response = helper
        .poll_records_for_testing(topic, Some(5))
        .await
        .unwrap();
    let poll_data = helper.assert_poll_response(response, 5, None).await;
    assert_eq!(poll_data.records.len(), 5);

    // Test no limit (should return all records)
    let response = helper.poll_records_for_testing(topic, None).await.unwrap();
    let poll_data = helper.assert_poll_response(response, 10, None).await;
    assert_eq!(poll_data.records.len(), 10);
}
