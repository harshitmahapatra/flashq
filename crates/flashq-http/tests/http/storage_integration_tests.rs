use super::test_utilities::{TestBroker, TestClient};
use flashq_http::*;
use uuid::Uuid;

// Helper functions for generating unique test identifiers
fn unique_consumer_group() -> String {
    let test_id = Uuid::new_v4().to_string().replace('-', "");
    format!("test_group_{test_id}")
}

fn unique_topic() -> String {
    let test_id = Uuid::new_v4().to_string().replace('-', "");
    format!("test_topic_{test_id}")
}

#[tokio::test]
async fn test_memory_backend_basic_operations() {
    let broker = TestBroker::start()
        .await
        .expect("Failed to start test broker");
    let client = TestClient::new(&broker);
    let topic = unique_topic();

    // Test basic post and poll operations with memory backend (default)
    let response = client
        .post_record(&topic, "Memory test record")
        .await
        .unwrap();
    assert!(response.status().is_success());

    let response = client
        .poll_records_for_testing(&topic, Some(1))
        .await
        .unwrap();
    let poll_data = client
        .assert_poll_response(response, 1, Some(&["Memory test record"]))
        .await;
    assert_eq!(poll_data.records.len(), 1);
}

#[tokio::test]
async fn test_file_backend_basic_operations() {
    let broker = TestBroker::start_with_storage("file")
        .await
        .expect("Failed to start test broker with file backend");
    let client = TestClient::new(&broker);
    let topic = unique_topic();

    // Test basic post and poll operations with file backend
    let response = client
        .post_record(&topic, "File test record")
        .await
        .unwrap();
    assert!(response.status().is_success());

    let response = client
        .poll_records_for_testing(&topic, Some(1))
        .await
        .unwrap();
    let poll_data = client
        .assert_poll_response(response, 1, Some(&["File test record"]))
        .await;
    assert_eq!(poll_data.records.len(), 1);

    // Verify data directory was created
    if let Some(data_dir) = broker.data_dir() {
        assert!(data_dir.exists());
        assert!(data_dir.is_dir());
    }
}

#[tokio::test]
async fn test_file_backend_persistence_across_restarts() {
    let topic = unique_topic();

    // Create a temporary directory for this test
    let temp_dir = tempfile::Builder::new()
        .prefix("flashq_persistence_test_")
        .tempdir()
        .expect("Failed to create temporary directory");

    // Start broker with file backend
    let broker = TestBroker::start_with_data_dir(temp_dir.path())
        .await
        .expect("Failed to start test broker");
    let client = TestClient::new(&broker);

    // Post records
    for i in 0..5 {
        client
            .post_record(&topic, &format!("Persistent record {i}"))
            .await
            .unwrap();
    }

    // Verify records exist
    let response = client.poll_records_for_testing(&topic, None).await.unwrap();
    let poll_data = client.assert_poll_response(response, 5, None).await;
    assert_eq!(poll_data.high_water_mark, 5);

    // Stop the broker (drop it)
    drop(broker);
    drop(client);

    // Start a new broker with the same data directory
    let broker2 = TestBroker::start_with_data_dir(temp_dir.path())
        .await
        .expect("Failed to restart test broker");
    let client2 = TestClient::new(&broker2);

    // Verify records persisted across restart
    let response = client2
        .poll_records_for_testing(&topic, None)
        .await
        .unwrap();
    let poll_data = client2.assert_poll_response(response, 5, None).await;
    assert_eq!(poll_data.high_water_mark, 5);
    assert_eq!(poll_data.records[0].record.value, "Persistent record 0");
    assert_eq!(poll_data.records[4].record.value, "Persistent record 4");

    // Clean up
    drop(broker2);
    // TempDir automatically cleans up when dropped
}

#[tokio::test]
async fn test_consumer_group_persistence_across_restarts() {
    let topic = unique_topic();
    let group_id = unique_consumer_group();

    // Create a temporary directory for this test
    let temp_dir = tempfile::Builder::new()
        .prefix("flashq_consumer_persistence_test_")
        .tempdir()
        .expect("Failed to create temporary directory");

    // Start broker with file backend
    let broker = TestBroker::start_with_data_dir(temp_dir.path())
        .await
        .expect("Failed to start test broker");
    let client = TestClient::new(&broker);

    // Create consumer group and post records
    client.create_consumer_group(&group_id).await.unwrap();

    for i in 0..3 {
        client
            .post_record(&topic, &format!("Consumer record {i}"))
            .await
            .unwrap();
    }

    // Commit offset to 2
    client
        .update_consumer_group_offset(&group_id, &topic, 2)
        .await
        .unwrap();

    // Verify offset
    let response = client
        .get_consumer_group_offset(&group_id, &topic)
        .await
        .unwrap();
    let offset_response: OffsetResponse = response.json().await.unwrap();
    assert_eq!(offset_response.committed_offset, 2);

    // Stop the broker
    drop(broker);
    drop(client);

    // Start a new broker with the same data directory
    let broker2 = TestBroker::start_with_data_dir(temp_dir.path())
        .await
        .expect("Failed to restart test broker");
    let client2 = TestClient::new(&broker2);

    // Verify consumer group and offset persisted
    let response = client2
        .get_consumer_group_offset(&group_id, &topic)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let offset_response: OffsetResponse = response.json().await.unwrap();
    assert_eq!(offset_response.committed_offset, 2);
    assert_eq!(offset_response.high_water_mark, 3);

    // Clean up
    drop(broker2);
    // TempDir automatically cleans up when dropped
}

#[tokio::test]
async fn test_memory_vs_file_backend_consistency() {
    let topic = unique_topic();

    // Test with memory backend
    let memory_broker = TestBroker::start()
        .await
        .expect("Failed to start memory broker");
    let memory_client = TestClient::new(&memory_broker);

    // Test with file backend
    let file_broker = TestBroker::start_with_storage("file")
        .await
        .expect("Failed to start file broker");
    let file_client = TestClient::new(&file_broker);

    // Post same records to both backends
    let test_records = vec!["Record A", "Record B", "Record C"];

    for record in &test_records {
        memory_client.post_record(&topic, record).await.unwrap();
        file_client.post_record(&topic, record).await.unwrap();
    }

    // Poll from both backends
    let memory_response = memory_client
        .poll_records_for_testing(&topic, None)
        .await
        .unwrap();
    let memory_data = memory_client
        .assert_poll_response(memory_response, 3, Some(&test_records))
        .await;

    let file_response = file_client
        .poll_records_for_testing(&topic, None)
        .await
        .unwrap();
    let file_data = file_client
        .assert_poll_response(file_response, 3, Some(&test_records))
        .await;

    // Verify both backends have consistent behavior
    assert_eq!(memory_data.records.len(), file_data.records.len());
    assert_eq!(memory_data.high_water_mark, file_data.high_water_mark);

    for (memory_record, file_record) in memory_data.records.iter().zip(file_data.records.iter()) {
        assert_eq!(memory_record.offset, file_record.offset);
        assert_eq!(memory_record.record.value, file_record.record.value);
        // Note: timestamps may differ slightly, so we don't compare them
    }
}

#[tokio::test]
async fn test_file_backend_creates_data_directory() {
    let broker = TestBroker::start_with_storage("file")
        .await
        .expect("Failed to start test broker with file backend");
    let client = TestClient::new(&broker);
    let topic = unique_topic();

    // Post a record to ensure directory creation
    client
        .post_record(&topic, "Directory test record")
        .await
        .unwrap();

    // Verify data directory exists
    if let Some(data_dir) = broker.data_dir() {
        assert!(data_dir.exists());
        assert!(data_dir.is_dir());

        // Check that topic dir was created
        let topic_dir = data_dir.join(&topic);
        assert!(topic_dir.exists());
        assert!(topic_dir.is_dir());
    } else {
        panic!("Expected data directory to be set for file backend");
    }
}
