use super::test_utilities::{TestBroker, TestClient};
use flashq_http::*;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn time_polling_returns_tail_from_mid_timestamp_http() {
    // Setup
    let broker = TestBroker::start().await.expect("start broker");
    let helper = TestClient::new(&broker);
    let topic = "http_time_poll";
    helper
        .post_record_with_record(topic, None, "m0", None)
        .await
        .unwrap();
    sleep(Duration::from_millis(5)).await;
    helper
        .post_record_with_record(topic, None, "m1", None)
        .await
        .unwrap();
    sleep(Duration::from_millis(5)).await;
    helper
        .post_record_with_record(topic, None, "m2", None)
        .await
        .unwrap();

    // Action: poll all to capture middle timestamp, then fetch from_time
    let response = helper
        .poll_records_for_testing(topic, None)
        .await
        .expect("poll all");
    assert_eq!(response.status(), 200);
    let all: FetchResponse = response.json().await.unwrap();
    let mid_ts = all.records[1].timestamp.clone();

    let group_id = "http_time_group_ok";
    helper.create_consumer_group(group_id).await.unwrap();
    let resp = helper
        .fetch_records_for_consumer_group_by_time(group_id, topic, &mid_ts, None)
        .await
        .unwrap();

    // Expectation
    assert_eq!(resp.status(), 200);
    let fr: FetchResponse = resp.json().await.unwrap();
    assert_eq!(fr.records.len(), 2);
    assert_eq!(fr.records[0].record.value, "m1");
    assert_eq!(fr.records[1].record.value, "m2");
    let _ = helper.leave_consumer_group(group_id).await;
}

#[tokio::test]
async fn time_polling_invalid_timestamp_returns_400_http() {
    // Setup
    let broker = TestBroker::start().await.expect("start broker");
    let helper = TestClient::new(&broker);
    let topic = "http_time_poll_invalid";
    let group_id = "http_time_group_bad";
    helper.create_consumer_group(group_id).await.unwrap();

    // Action
    let bad = helper
        .fetch_records_for_consumer_group_by_time(group_id, topic, "not-a-time", None)
        .await
        .unwrap();

    // Expectation
    assert_eq!(bad.status(), 400);
    let _ = helper.leave_consumer_group(group_id).await;
}

#[tokio::test]
async fn offset_fetch_control_returns_200_http() {
    // Setup
    let broker = TestBroker::start().await.expect("start broker");
    let helper = TestClient::new(&broker);
    let topic = "http_time_poll_offset";
    helper
        .post_record_with_record(topic, None, "m0", None)
        .await
        .unwrap();
    let group_id = "http_time_group_offset";
    helper.create_consumer_group(group_id).await.unwrap();

    // Action
    let resp = helper
        .fetch_records_for_consumer_group_by_offset(group_id, topic, Some(0), Some(10))
        .await
        .unwrap();

    // Expectation
    assert_eq!(resp.status(), 200);
    let _ = helper.leave_consumer_group(group_id).await;
}
