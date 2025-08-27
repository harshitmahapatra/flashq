//! HTTP integration tests specifically for file backend storage

use super::http_test_helpers::*;
use flashq::http::*;
use std::fs;
use std::path::Path;
use tokio::time::{Duration, sleep};

fn unique_consumer_group() -> String {
    format!("test_group_{}", uuid::Uuid::new_v4())
}

fn unique_topic() -> String {
    format!("test_topic_{}", uuid::Uuid::new_v4())
}

/// Test that server starts successfully with file storage backend
#[tokio::test]
async fn test_server_starts_with_file_backend() {
    let _server = TestServer::start_with_storage("file")
        .await
        .expect("Server should start with file backend");
}

/// Test basic record posting and polling with file backend
#[tokio::test]
async fn test_file_backend_post_and_poll_records() {
    let server = TestServer::start_with_storage("file")
        .await
        .expect("Server should start with file backend");

    let client = TestClient::new(&server);
    let topic = unique_topic();
    let group_id = unique_consumer_group();

    // Post a record
    let post_response = client
        .post_record(&topic, "Test message for file backend")
        .await
        .expect("Should post record successfully");

    assert_eq!(post_response.status(), 200);
    let post_data: ProduceResponse = post_response.json().await.expect("Should parse response");
    assert_eq!(post_data.offsets[0].offset, 0);

    // Create consumer group and fetch records
    let create_response = client
        .create_consumer_group(&group_id)
        .await
        .expect("Should create consumer group");
    assert_eq!(create_response.status(), 200);

    let fetch_response = client
        .fetch_records_for_consumer_group(&group_id, &topic, None)
        .await
        .expect("Should fetch records");

    assert_eq!(fetch_response.status(), 200);
    let fetch_data: FetchResponse = fetch_response
        .json()
        .await
        .expect("Should parse fetch response");
    assert_eq!(fetch_data.records.len(), 1);
    assert_eq!(
        fetch_data.records[0].record.value,
        "Test message for file backend"
    );
    assert_eq!(fetch_data.records[0].offset, 0);
}

#[tokio::test]
async fn test_file_backend_persistence_across_restarts() {
    // Create a unique temporary directory for this test
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let shared_data_dir = std::env::temp_dir().join(format!("flashq_persistence_test_{test_id}"));

    let topic = unique_topic();
    let group_id = unique_consumer_group();

    // Start first server instance and post records
    {
        let server = TestServer::start_with_data_dir(&shared_data_dir)
            .await
            .expect("Server should start with file backend");

        let client = TestClient::new(&server);

        // Post multiple records
        client
            .post_record(&topic, "Message 1")
            .await
            .expect("Should post record 1");

        client
            .post_record(&topic, "Message 2")
            .await
            .expect("Should post record 2");

        // Verify records were posted
        client
            .create_consumer_group(&group_id)
            .await
            .expect("Should create consumer group");

        let fetch_response = client
            .fetch_records_for_consumer_group(&group_id, &topic, None)
            .await
            .expect("Should fetch records");

        let fetch_data: FetchResponse = fetch_response
            .json()
            .await
            .expect("Should parse fetch response");
        assert_eq!(fetch_data.records.len(), 2);
        assert_eq!(fetch_data.records[0].record.value, "Message 1");
        assert_eq!(fetch_data.records[1].record.value, "Message 2");

        // Debug: Check what files were created
        eprintln!("DEBUG: Files in data directory after first server:");
        if shared_data_dir.exists() {
            for entry in fs::read_dir(&shared_data_dir).unwrap() {
                let entry = entry.unwrap();
                eprintln!("  - {:?}", entry.path());
            }
        } else {
            eprintln!("  - Data directory doesn't exist yet!");
        }
    } // Server drops here, terminating the process

    // Small delay to ensure server shutdown
    sleep(Duration::from_millis(100)).await;

    // Debug: Check files still exist after server shutdown
    eprintln!("DEBUG: Files in data directory after server shutdown:");
    if shared_data_dir.exists() {
        for entry in fs::read_dir(&shared_data_dir).unwrap() {
            let entry = entry.unwrap();
            eprintln!("  - {:?}", entry.path());
        }
    } else {
        eprintln!("  - Data directory was deleted!");
    }

    // Start second server instance with the SAME data directory
    {
        let server = TestServer::start_with_data_dir(&shared_data_dir)
            .await
            .expect("Server should start with file backend after restart");

        let client = TestClient::new(&server);

        // Create a new consumer group and verify records persisted
        let new_group_id = unique_consumer_group();
        client
            .create_consumer_group(&new_group_id)
            .await
            .expect("Should create consumer group after restart");

        let fetch_response = client
            .fetch_records_for_consumer_group(&new_group_id, &topic, None)
            .await
            .expect("Should fetch persisted records");

        if fetch_response.status().is_success() {
            let fetch_data: FetchResponse = fetch_response.json().await.unwrap_or_else(|e| {
                eprintln!("Failed to parse successful fetch response: {e:?}");
                panic!("Should parse fetch response");
            });
            // ... rest of your assertions
            assert_eq!(fetch_data.records.len(), 2);
            assert_eq!(fetch_data.records[0].record.value, "Message 1");
            assert_eq!(fetch_data.records[0].offset, 0);
            assert_eq!(fetch_data.records[1].record.value, "Message 2");
            assert_eq!(fetch_data.records[1].offset, 1);
        } else {
            let error_text = fetch_response.text().await.expect("Should get error text");
            eprintln!("Error response: {error_text}");
            panic!("Fetch request failed");
        }

        // Post a new record - should continue with correct offset
        let post_response = client
            .post_record(&topic, "Message 3 after restart")
            .await
            .expect("Should post record after restart");

        assert_eq!(post_response.status(), 200);
        let post_data: ProduceResponse = post_response.json().await.expect("Should parse response");
        assert_eq!(post_data.offsets[0].offset, 2);
    }

    // Clean up the shared directory
    if shared_data_dir.exists() {
        fs::remove_dir_all(shared_data_dir).ok();
    }
}

#[tokio::test]
async fn test_consumer_group_persistence_across_restarts() {
    // Create a unique temporary directory for this test
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let shared_data_dir =
        std::env::temp_dir().join(format!("flashq_consumer_persistence_test_{test_id}"));

    let topic = unique_topic();
    let group_id1 = unique_consumer_group();
    let group_id2 = unique_consumer_group();

    // First server instance - create consumer groups and set offsets
    {
        let server = TestServer::start_with_data_dir(&shared_data_dir)
            .await
            .expect("Server should start with file backend");

        let client = TestClient::new(&server);

        // Post several records
        for i in 1..=5 {
            client
                .post_record(&topic, &format!("Message {i}"))
                .await
                .unwrap_or_else(|_| panic!("Should post record {i}"));
        }

        // Create consumer groups
        client
            .create_consumer_group(&group_id1)
            .await
            .expect("Should create consumer group 1");

        client
            .create_consumer_group(&group_id2)
            .await
            .expect("Should create consumer group 2");

        // Group 1 consumes first 2 records
        let fetch_response = client
            .fetch_records_for_consumer_group(&group_id1, &topic, Some(2))
            .await
            .expect("Should fetch records for group 1");

        let fetch_data: FetchResponse = fetch_response
            .json()
            .await
            .expect("Should parse fetch response");
        assert_eq!(fetch_data.records.len(), 2);

        // Update group 1 offset to 2
        client
            .update_consumer_group_offset(&group_id1, &topic, 2)
            .await
            .expect("Should update group 1 offset");

        // Group 2 consumes first 3 records
        let fetch_response = client
            .fetch_records_for_consumer_group(&group_id2, &topic, Some(3))
            .await
            .expect("Should fetch records for group 2");

        let fetch_data: FetchResponse = fetch_response
            .json()
            .await
            .expect("Should parse fetch response");
        assert_eq!(fetch_data.records.len(), 3);

        // Update group 2 offset to 3
        client
            .update_consumer_group_offset(&group_id2, &topic, 3)
            .await
            .expect("Should update group 2 offset");

        // Debug: Check what consumer group files were created
        eprintln!("DEBUG: Files in consumer_groups directory after first server:");
        let consumer_groups_dir = shared_data_dir.join("consumer_groups");
        if consumer_groups_dir.exists() {
            for entry in fs::read_dir(&consumer_groups_dir).unwrap() {
                let entry = entry.unwrap();
                eprintln!("  - {:?}", entry.path());
            }
        }
    } // Server drops here

    // Small delay to ensure server shutdown
    sleep(Duration::from_millis(100)).await;

    // Second server instance - should recover consumer group state
    {
        let server = TestServer::start_with_data_dir(&shared_data_dir)
            .await
            .expect("Server should start with file backend after restart");

        let client = TestClient::new(&server);

        // Get consumer group offsets - should be recovered from disk
        let offset_response = client
            .get_consumer_group_offset(&group_id1, &topic)
            .await
            .expect("Should get consumer group 1 offset");

        assert_eq!(offset_response.status(), 200);
        let offset_data: OffsetResponse = offset_response
            .json()
            .await
            .expect("Should parse offset response");
        assert_eq!(offset_data.committed_offset, 2);

        let offset_response = client
            .get_consumer_group_offset(&group_id2, &topic)
            .await
            .expect("Should get consumer group 2 offset");

        assert_eq!(offset_response.status(), 200);
        let offset_data: OffsetResponse = offset_response
            .json()
            .await
            .expect("Should parse offset response");
        assert_eq!(offset_data.committed_offset, 3);

        // Verify consumer groups can fetch from correct offsets
        let fetch_response = client
            .fetch_records_for_consumer_group(&group_id1, &topic, None)
            .await
            .expect("Should fetch remaining records for group 1");

        let fetch_data: FetchResponse = fetch_response
            .json()
            .await
            .expect("Should parse fetch response");
        assert_eq!(fetch_data.records.len(), 3); // Records 2, 3, 4 (offsets 2-4)
        assert_eq!(fetch_data.records[0].record.value, "Message 3");
        assert_eq!(fetch_data.records[0].offset, 2);

        let fetch_response = client
            .fetch_records_for_consumer_group(&group_id2, &topic, None)
            .await
            .expect("Should fetch remaining records for group 2");

        let fetch_data: FetchResponse = fetch_response
            .json()
            .await
            .expect("Should parse fetch response");
        assert_eq!(fetch_data.records.len(), 2); // Records 3, 4 (offsets 3-4)
        assert_eq!(fetch_data.records[0].record.value, "Message 4");
        assert_eq!(fetch_data.records[0].offset, 3);
    }

    // Clean up the shared directory
    if shared_data_dir.exists() {
        fs::remove_dir_all(shared_data_dir).ok();
    }
}

#[tokio::test]
async fn test_consumer_group_cross_restart_updates() {
    // Test that consumer group offset updates persist across server restarts
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_id = format!("{}_{}", std::process::id(), timestamp);
    let shared_data_dir =
        std::env::temp_dir().join(format!("flashq_consumer_updates_test_{test_id}"));

    let topic = unique_topic();
    let group_id = unique_consumer_group();

    // First server - create consumer group and set initial offset
    {
        let server = TestServer::start_with_data_dir(&shared_data_dir)
            .await
            .expect("Server should start");

        let client = TestClient::new(&server);

        // Post records
        for i in 1..=10 {
            client
                .post_record(&topic, &format!("Message {i}"))
                .await
                .unwrap_or_else(|_| panic!("Should post record {i}"));
        }

        // Create consumer group and consume some records
        client
            .create_consumer_group(&group_id)
            .await
            .expect("Should create consumer group");

        client
            .update_consumer_group_offset(&group_id, &topic, 5)
            .await
            .expect("Should set initial offset");
    }

    sleep(Duration::from_millis(100)).await;

    // Second server - update offset further
    {
        let server = TestServer::start_with_data_dir(&shared_data_dir)
            .await
            .expect("Server should restart");

        let client = TestClient::new(&server);

        // Verify initial offset was persisted
        let offset_response = client
            .get_consumer_group_offset(&group_id, &topic)
            .await
            .expect("Should get offset");

        let offset_data: OffsetResponse = offset_response
            .json()
            .await
            .expect("Should parse offset response");
        assert_eq!(offset_data.committed_offset, 5);

        // Update offset
        client
            .update_consumer_group_offset(&group_id, &topic, 8)
            .await
            .expect("Should update offset");
    }

    sleep(Duration::from_millis(100)).await;

    // Third server - verify final offset persisted
    {
        let server = TestServer::start_with_data_dir(&shared_data_dir)
            .await
            .expect("Server should restart again");

        let client = TestClient::new(&server);

        // Verify updated offset was persisted
        let offset_response = client
            .get_consumer_group_offset(&group_id, &topic)
            .await
            .expect("Should get final offset");

        let offset_data: OffsetResponse = offset_response
            .json()
            .await
            .expect("Should parse offset response");
        assert_eq!(offset_data.committed_offset, 8);

        // Fetch from this offset to verify it works
        let fetch_response = client
            .fetch_records_for_consumer_group(&group_id, &topic, None)
            .await
            .expect("Should fetch from persisted offset");

        let fetch_data: FetchResponse = fetch_response
            .json()
            .await
            .expect("Should parse fetch response");
        assert_eq!(fetch_data.records.len(), 2); // Records 8, 9 (offsets 8-9)
        assert_eq!(fetch_data.records[0].record.value, "Message 9");
        assert_eq!(fetch_data.records[0].offset, 8);
    }

    // Clean up
    if shared_data_dir.exists() {
        fs::remove_dir_all(shared_data_dir).ok();
    }
}

#[tokio::test]
async fn test_file_backend_creates_data_directory() {
    let server = TestServer::start_with_storage("file")
        .await
        .expect("Server should start with file backend");

    let client = TestClient::new(&server);

    // Post a record to trigger data directory creation
    let topic = unique_topic();
    client
        .post_record(&topic, "Test message")
        .await
        .expect("Should post record successfully");

    // Get the temp data directory from the server
    let data_dir = server.data_dir().expect("Should have temp data directory");

    // Verify data directory was created
    assert!(data_dir.exists(), "Data directory should be created");
    assert!(data_dir.is_dir(), "Data path should be a directory");

    // Verify topic log file was created
    let topic_file = data_dir.join(format!("{topic}.log"));
    assert!(topic_file.exists(), "Topic log file should be created");

    // Temp directory will be cleaned up automatically when server drops
}

/// Test file backend with consumer groups
#[tokio::test]
async fn test_file_backend_consumer_groups() {
    let server = TestServer::start_with_storage("file")
        .await
        .expect("Server should start with file backend");

    let client = TestClient::new(&server);
    let topic = unique_topic();
    let group_id = unique_consumer_group();

    // Post some records
    for i in 0..3 {
        client
            .post_record(&topic, &format!("Message {i}"))
            .await
            .expect("Should post record successfully");
    }

    // Create consumer group
    let create_response = client
        .create_consumer_group(&group_id)
        .await
        .expect("Should create consumer group");
    assert_eq!(create_response.status(), 200);

    // Fetch records for consumer group
    let fetch_response = client
        .fetch_records_for_consumer_group(&group_id, &topic, None)
        .await
        .expect("Should fetch records for consumer group");

    assert_eq!(fetch_response.status(), 200);
    let fetch_data: FetchResponse = fetch_response
        .json()
        .await
        .expect("Should parse fetch response");
    assert_eq!(fetch_data.records.len(), 3);

    // Commit offset
    let commit_response = client
        .update_consumer_group_offset(&group_id, &topic, 3)
        .await
        .expect("Should commit offset");
    assert_eq!(commit_response.status(), 200);

    // Verify offset was committed
    let offset_response = client
        .get_consumer_group_offset(&group_id, &topic)
        .await
        .expect("Should get consumer group offset");

    assert_eq!(offset_response.status(), 200);
    let offset_data: OffsetResponse = offset_response
        .json()
        .await
        .expect("Should parse offset response");
    assert_eq!(offset_data.committed_offset, 3);
}

/// Test that memory and file backends behave consistently
#[tokio::test]
async fn test_memory_vs_file_backend_consistency() {
    // Test with memory backend
    let memory_server = TestServer::start_with_storage("memory")
        .await
        .expect("Server should start with memory backend");

    let memory_client = TestClient::new(&memory_server);

    // Test with file backend
    let file_server = TestServer::start_with_storage("file")
        .await
        .expect("Server should start with file backend");

    let file_client = TestClient::new(&file_server);

    let topic = unique_topic();
    let memory_group = unique_consumer_group();
    let file_group = unique_consumer_group();

    // Post same records to both backends
    for i in 0..3 {
        let message = format!("Consistency test message {i}");

        let memory_response = memory_client
            .post_record(&topic, &message)
            .await
            .expect("Should post to memory backend");

        let file_response = file_client
            .post_record(&topic, &message)
            .await
            .expect("Should post to file backend");

        // Both should return same status and offset
        assert_eq!(memory_response.status(), file_response.status());

        let memory_data: ProduceResponse = memory_response
            .json()
            .await
            .expect("Should parse memory response");
        let file_data: ProduceResponse = file_response
            .json()
            .await
            .expect("Should parse file response");
        assert_eq!(memory_data.offsets[0].offset, file_data.offsets[0].offset);
    }

    // Create consumer groups for both backends
    memory_client
        .create_consumer_group(&memory_group)
        .await
        .expect("Should create memory consumer group");
    file_client
        .create_consumer_group(&file_group)
        .await
        .expect("Should create file consumer group");

    // Fetch records from both backends
    let memory_records_response = memory_client
        .fetch_records_for_consumer_group(&memory_group, &topic, None)
        .await
        .expect("Should fetch from memory backend");

    let file_records_response = file_client
        .fetch_records_for_consumer_group(&file_group, &topic, None)
        .await
        .expect("Should fetch from file backend");

    // Both should return same status
    assert_eq!(
        memory_records_response.status(),
        file_records_response.status()
    );

    let memory_data: FetchResponse = memory_records_response
        .json()
        .await
        .expect("Should parse memory fetch response");
    let file_data: FetchResponse = file_records_response
        .json()
        .await
        .expect("Should parse file fetch response");

    // Results should be identical
    assert_eq!(memory_data.records.len(), file_data.records.len());
    for i in 0..memory_data.records.len() {
        assert_eq!(
            memory_data.records[i].record.value,
            file_data.records[i].record.value
        );
        assert_eq!(memory_data.records[i].offset, file_data.records[i].offset);
    }

    // Clean up file backend data
    let data_dir = Path::new("./data");
    if data_dir.exists() {
        fs::remove_dir_all(data_dir).ok();
    }
}
