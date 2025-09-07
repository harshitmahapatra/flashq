use super::test_utilities::{TestClient, TestServer};
use flashq::http::*;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_http_time_based_polling_memory() {
    let server = TestServer::start().await.expect("start server");
    let helper = TestClient::new(&server);
    let topic = "http_time_poll";

    // Post three records with small delays to ensure distinct timestamps
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

    // Poll all to capture timestamps
    let response = helper
        .poll_records_for_testing(topic, None)
        .await
        .expect("poll all");
    assert_eq!(response.status(), 200);
    let all: FetchResponse = response.json().await.unwrap();
    assert_eq!(all.records.len(), 3);
    let mid_ts = all.records[1].timestamp.clone();

    // Create a consumer group and fetch from_time
    let group_id = "http_time_group";
    helper.create_consumer_group(group_id).await.unwrap();
    let resp = helper
        .fetch_records_for_consumer_group_with_time(group_id, topic, &mid_ts, None)
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let fr: FetchResponse = resp.json().await.unwrap();
    assert_eq!(fr.records.len(), 2);
    assert_eq!(fr.records[0].record.value, "m1");
    assert_eq!(fr.records[1].record.value, "m2");

    // Invalid from_time → 400
    let bad = helper
        .fetch_records_for_consumer_group_with_time(group_id, topic, "not-a-time", None)
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);

    // Both from_offset and from_time → 400
    let resp = helper
        .fetch_records_for_consumer_group_with_options(group_id, topic, Some(0), Some(10))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200); // control
    let client = reqwest::Client::new();
    let url = format!("{}/consumer/{}/topics/{}", helper.base_url, group_id, topic);
    let resp = client
        .get(&url)
        .query(&[("from_offset", "0"), ("from_time", "2025-01-01T00:00:00Z")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // Cleanup group
    let _ = helper.leave_consumer_group(group_id).await;
}
