use super::test_utilities::{TestClient, TestServer};
use flashq::Record;
use flashq::http::*;

#[tokio::test]
async fn test_post_record_integration() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);

    let response = helper
        .post_record_with_record("test", None, "Integration test record", None)
        .await
        .unwrap();
    assert!(response.status().is_success());

    let response_data: ProduceResponse = response
        .json()
        .await
        .expect("Failed to parse response JSON");
    assert_eq!(response_data.offset, 0);
    assert!(response_data.timestamp.contains("T"));

    let mut headers = std::collections::HashMap::new();
    headers.insert("source".to_string(), "integration-test".to_string());
    headers.insert("priority".to_string(), "high".to_string());

    let response = helper
        .post_record_with_record(
            "test",
            Some("user123".to_string()),
            "Record with metadata",
            Some(headers),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    let response_data: ProduceResponse = response
        .json()
        .await
        .expect("Failed to parse response JSON");
    assert_eq!(response_data.offset, 1);
}

#[tokio::test]
async fn test_record_size_and_validation_limits() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    let topic = "validation_test_topic";

    let max_key = "x".repeat(1024);
    let response = helper
        .post_record_with_record(
            topic,
            Some(max_key.clone()),
            "Valid record with max key size",
            None,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "Max key size should be accepted");

    let oversized_key = "x".repeat(1025);
    let response = helper
        .post_record_with_record(
            topic,
            Some(oversized_key),
            "Record with oversized key",
            None,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 400, "Oversized key should be rejected");

    let oversized_value = "x".repeat(1_048_577);
    let response = helper
        .post_record_with_record(topic, None, &oversized_value, None)
        .await
        .unwrap();
    assert_eq!(response.status(), 400, "Oversized value should be rejected");

    let mut oversized_header = std::collections::HashMap::new();
    oversized_header.insert("large_header".to_string(), "z".repeat(1025));
    let response = helper
        .post_record_with_record(
            topic,
            None,
            "Record with oversized header",
            Some(oversized_header),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        400,
        "Oversized header should be rejected"
    );

    let large_value = "Record content ".repeat(1000);
    let response = helper
        .post_record_with_record(topic, None, &large_value, None)
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "Large record should be accepted");

    let mut headers = std::collections::HashMap::new();
    for i in 0..10 {
        headers.insert(format!("header_{i}"), format!("value_{i}"));
    }
    let response = helper
        .post_record_with_record(
            topic,
            Some("test_key".to_string()),
            "Record with multiple headers",
            Some(headers),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "Multiple headers should be accepted"
    );

    let mut max_header = std::collections::HashMap::new();
    max_header.insert("large_header".to_string(), "y".repeat(1024));
    let response = helper
        .post_record_with_record(topic, None, "Record with max header size", Some(max_header))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "Max header value size should be accepted"
    );

    let response = helper.poll_records_for_testing(topic, None).await.unwrap();
    assert_eq!(response.status(), 200);
    let poll_data: FetchResponse = response.json().await.unwrap();
    assert_eq!(
        poll_data.records.len(),
        4,
        "All valid records should be stored"
    );

    let max_key_record = &poll_data.records[0];
    assert_eq!(max_key_record.record.key.as_ref().unwrap(), &max_key);
    assert_eq!(
        max_key_record.record.value,
        "Valid record with max key size"
    );
}

#[tokio::test]
async fn test_client_binary_integration() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    let topic = "client_test_topic";

    // Test posting with client binary
    helper
        .post_record_with_client(topic, "Hello from client binary!")
        .await
        .expect("Failed to post record with client");

    // Test polling with client binary
    let output = helper
        .poll_records_with_client(topic, Some(10))
        .await
        .expect("Failed to poll records with client");

    // Verify the output contains our record
    assert!(output.contains("Hello from client binary!"));
    assert!(output.contains("Got 1 records for consumer group"));
    // With the "do not advance offset on poll" change, next offset remains 0 after fetching the record at offset 0
    assert!(output.contains("Next offset: 0"));
}

#[tokio::test]
async fn test_concurrent_record_posting() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    let topic = "concurrent_posting_topic";

    let post_handles: Vec<_> = (0..20)
        .map(|i| {
            let helper = TestClient::new(&server);
            tokio::spawn(async move {
                let key = if i % 2 == 0 {
                    Some(format!("key_{i}"))
                } else {
                    None
                };
                let mut headers = std::collections::HashMap::new();
                headers.insert("thread".to_string(), format!("thread_{i}"));
                headers.insert("index".to_string(), i.to_string());

                let response = helper
                    .post_record_with_record(
                        topic,
                        key,
                        &format!("Concurrent record {i}"),
                        Some(headers),
                    )
                    .await
                    .unwrap();
                (i, response.status() == 200)
            })
        })
        .collect();

    let mut results = Vec::new();
    for handle in post_handles {
        let (index, success) = handle.await.unwrap();
        results.push((index, success));
    }

    for (index, success) in &results {
        assert!(success, "Concurrent post {index} should succeed");
    }

    let response = helper.poll_records_for_testing(topic, None).await.unwrap();
    assert_eq!(response.status(), 200);
    let poll_data: FetchResponse = response.json().await.unwrap();
    assert_eq!(
        poll_data.records.len(),
        20,
        "All concurrent records should be stored"
    );

    for (i, record) in poll_data.records.iter().enumerate() {
        assert_eq!(record.offset, i as u64, "Offsets should be sequential");
        assert!(
            record.record.value.starts_with("Concurrent record"),
            "Record content should be preserved"
        );

        if let Some(ref headers) = record.record.headers {
            assert!(
                headers.contains_key("thread"),
                "Thread header should be present"
            );
            assert!(
                headers.contains_key("index"),
                "Index header should be present"
            );
        }
    }

    let multi_topic_handles: Vec<_> = (0..15)
        .map(|i| {
            let helper = TestClient::new(&server);
            let topic_name = format!("concurrent_topic_{}", i % 3);
            tokio::spawn(async move {
                let response = helper
                    .post_record(&topic_name, &format!("Multi-topic record {i}"))
                    .await
                    .unwrap();
                (topic_name, response.status() == 200)
            })
        })
        .collect();

    for handle in multi_topic_handles {
        let (topic_name, success) = handle.await.unwrap();
        assert!(success, "Multi-topic post to {topic_name} should succeed");
    }

    for topic_index in 0..3 {
        let topic_name = format!("concurrent_topic_{topic_index}");
        let response = helper
            .poll_records_for_testing(&topic_name, None)
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let poll_data: FetchResponse = response.json().await.unwrap();
        assert_eq!(
            poll_data.records.len(),
            5,
            "Each topic should have 5 records"
        );
    }
}

#[tokio::test]
async fn test_batch_record_posting() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    let topic = "batch_test_topic";

    // Test single record batch
    let single_record = vec![Record {
        key: Some("user1".to_string()),
        value: "First record".to_string(),
        headers: None,
    }];

    let response = helper
        .post_batch_records(topic, single_record)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let response_data: ProduceResponse = response.json().await.unwrap();
    assert_eq!(response_data.offset, 0);

    // Test multiple records batch
    let mut headers = std::collections::HashMap::new();
    headers.insert("source".to_string(), "batch-test".to_string());

    let batch_records = vec![
        Record {
            key: Some("user2".to_string()),
            value: "Second record".to_string(),
            headers: Some(headers.clone()),
        },
        Record {
            key: None,
            value: "Third record".to_string(),
            headers: None,
        },
        Record {
            key: Some("user3".to_string()),
            value: "Fourth record".to_string(),
            headers: Some(headers),
        },
    ];

    let response = helper
        .post_batch_records(topic, batch_records)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let response_data: ProduceResponse = response.json().await.unwrap();
    assert_eq!(response_data.offset, 3);

    // Verify all records were posted correctly
    let response = helper.poll_records_for_testing(topic, None).await.unwrap();
    assert_eq!(response.status(), 200);
    let poll_data: FetchResponse = response.json().await.unwrap();
    assert_eq!(poll_data.records.len(), 4);

    assert_eq!(poll_data.records[0].record.value, "First record");
    assert_eq!(poll_data.records[1].record.value, "Second record");
    assert_eq!(poll_data.records[2].record.value, "Third record");
    assert_eq!(poll_data.records[3].record.value, "Fourth record");
}

#[tokio::test]
async fn test_record_structure_edge_cases() {
    let server = TestServer::start()
        .await
        .expect("Failed to start test server");
    let helper = TestClient::new(&server);
    let topic = "edge_case_topic";

    let response = helper
        .post_record_with_record(topic, Some("".to_string()), "Record with empty key", None)
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "Empty key should be accepted");

    let special_key = "key-with_special.chars@domain.com:123";
    let response = helper
        .post_record_with_record(
            topic,
            Some(special_key.to_string()),
            "Record with special chars in key",
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "Special characters in key should be accepted"
    );

    let mut special_headers = std::collections::HashMap::new();
    special_headers.insert("content-type".to_string(), "application/json".to_string());
    special_headers.insert("x-custom-header".to_string(), "".to_string());
    special_headers.insert("unicode-header".to_string(), "测试数据".to_string());

    let response = helper
        .post_record_with_record(
            topic,
            None,
            "Record with special headers",
            Some(special_headers.clone()),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "Special headers should be accepted");

    // Test minimal record using new batch format
    let response = helper
        .client
        .post(format!("{}/topics/{}/records", helper.base_url, topic))
        .json(&serde_json::json!({
            "records": [{
                "value": "Minimal record"
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "Minimal record should be accepted");

    // Test record with explicit nulls using new batch format
    let response = helper
        .client
        .post(format!("{}/topics/{}/records", helper.base_url, topic))
        .json(&serde_json::json!({
            "records": [{
                "key": null,
                "value": "Record with explicit nulls",
                "headers": null
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "Explicit nulls should be accepted");

    let response = helper.poll_records_for_testing(topic, None).await.unwrap();
    assert_eq!(response.status(), 200);
    let poll_data: FetchResponse = response.json().await.unwrap();
    assert_eq!(
        poll_data.records.len(),
        5,
        "All edge case records should be stored"
    );

    let records = &poll_data.records;

    assert_eq!(records[0].record.key.as_deref(), Some(""));
    assert_eq!(records[0].record.value, "Record with empty key");

    assert_eq!(records[1].record.key.as_ref().unwrap(), &special_key);
    assert_eq!(records[1].record.value, "Record with special chars in key");

    assert_eq!(records[2].record.value, "Record with special headers");
    let headers = records[2].record.headers.as_ref().unwrap();
    assert_eq!(headers.get("content-type").unwrap(), "application/json");
    assert_eq!(headers.get("x-custom-header").unwrap(), "");
    assert_eq!(headers.get("unicode-header").unwrap(), "测试数据");

    assert!(records[3].record.key.is_none());
    assert!(records[3].record.headers.is_none());
    assert_eq!(records[3].record.value, "Minimal record");

    assert!(records[4].record.key.is_none());
    assert!(records[4].record.headers.is_none());
    assert_eq!(records[4].record.value, "Record with explicit nulls");
}
