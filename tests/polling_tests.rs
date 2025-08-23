mod test_helpers;

use message_queue_rs::api::*;
use test_helpers::{TestHelper, TestServer};

#[tokio::test]
async fn test_basic_message_polling() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);
    let topic = "basic_poll_topic";

    for i in 0..5 {
        helper
            .post_message(topic, &format!("Basic Message {i}"))
            .await
            .unwrap();
    }

    let response = helper.poll_messages(topic, None).await.unwrap();
    let poll_data = helper
        .assert_poll_response(
            response,
            5,
            Some(&[
                "Basic Message 0",
                "Basic Message 1",
                "Basic Message 2",
                "Basic Message 3",
                "Basic Message 4",
            ]),
        )
        .await;

    assert_eq!(poll_data.next_offset, 5);
    assert_eq!(poll_data.high_water_mark, 5);
    assert_eq!(poll_data.lag, Some(0));
}

#[tokio::test]
async fn test_polling_with_count_limit() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);
    let topic = "count_limit_topic";

    for i in 0..8 {
        helper
            .post_message(topic, &format!("Limited Message {i}"))
            .await
            .unwrap();
    }

    let response = helper.poll_messages(topic, Some(3)).await.unwrap();
    let poll_data = helper
        .assert_poll_response(
            response,
            3,
            Some(&[
                "Limited Message 0",
                "Limited Message 1",
                "Limited Message 2",
            ]),
        )
        .await;

    assert_eq!(poll_data.next_offset, 3);
    assert_eq!(poll_data.high_water_mark, 8);
    assert_eq!(poll_data.lag, Some(5));
}

#[tokio::test]
async fn test_polling_empty_topic() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);
    let topic = "empty_topic";

    let response = helper.poll_messages(topic, None).await.unwrap();
    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn test_polling_from_offset() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);
    let topic = "offset_poll_topic";

    for i in 0..7 {
        helper
            .post_message(topic, &format!("Offset Message {i}"))
            .await
            .unwrap();
    }

    let response = helper
        .client
        .get(format!("{}/topics/{}/messages", helper.base_url, topic))
        .query(&[("from_offset", "3"), ("count", "2")])
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let poll_data: FetchResponse = response.json().await.unwrap();
    assert_eq!(poll_data.records.len(), 2);
    assert_eq!(poll_data.records[0].offset, 3);
    assert_eq!(poll_data.records[0].value, "Offset Message 3");
    assert_eq!(poll_data.records[1].offset, 4);
    assert_eq!(poll_data.records[1].value, "Offset Message 4");
    assert_eq!(poll_data.next_offset, 5);
    assert_eq!(poll_data.high_water_mark, 7);
}

#[tokio::test]
async fn test_polling_beyond_available_messages() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);
    let topic = "beyond_limit_topic";

    helper.post_message(topic, "Only message").await.unwrap();

    let response = helper
        .client
        .get(format!("{}/topics/{}/messages", helper.base_url, topic))
        .query(&[("from_offset", "5")])
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let poll_data: FetchResponse = response.json().await.unwrap();
    assert_eq!(poll_data.records.len(), 0);
    assert_eq!(poll_data.next_offset, 5);
    assert_eq!(poll_data.high_water_mark, 1);
}

#[tokio::test]
async fn test_multi_topic_polling() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestHelper::new(&server);

    let topics = ["topic_a", "topic_b", "topic_c"];

    for (topic_idx, topic) in topics.iter().enumerate() {
        for msg_idx in 0..3 {
            helper
                .post_message(topic, &format!("Topic {topic_idx} Message {msg_idx}"))
                .await
                .unwrap();
        }
    }

    for (topic_idx, topic) in topics.iter().enumerate() {
        let response = helper.poll_messages(topic, None).await.unwrap();
        let poll_data = helper.assert_poll_response(response, 3, None).await;

        for (msg_idx, record) in poll_data.records.iter().enumerate() {
            assert_eq!(record.value, format!("Topic {topic_idx} Message {msg_idx}"));
            assert_eq!(record.offset, msg_idx as u64);
        }
    }
}
