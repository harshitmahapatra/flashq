use super::test_utilities::*;
use chrono::{DateTime, Duration, FixedOffset};
use flashq::Record;
use flashq::RecordWithOffset;
use flashq_storage::TopicLog;
use flashq_storage::backend::StorageBackend;
use flashq_storage::file::{FileTopicLog, SyncMode};
use std::thread;
use std::time::Duration as StdDuration;
use test_log::test;

#[test]
fn time_polling_from_mid_returns_tail_values() {
    // Setup: file topic log and three records with increasing timestamps
    let config = TestConfig::new("time_poll_basic");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();

    log.append(Record::new(None, "t0".to_string(), None))
        .unwrap();
    thread::sleep(StdDuration::from_millis(10));
    log.append(Record::new(None, "t1".to_string(), None))
        .unwrap();
    thread::sleep(StdDuration::from_millis(10));
    log.append(Record::new(None, "t2".to_string(), None))
        .unwrap();

    // Setup: capture middle record timestamp
    let all = log.get_records_from_offset(0, None).unwrap();
    assert_eq!(all.len(), 3);
    let mid_ts = all[1].timestamp.clone();

    // Action: poll from middle timestamp
    let from_mid = log.get_records_from_timestamp(&mid_ts, None).unwrap();

    // Expectation: returns two records t1, t2 in order
    assert_eq!(from_mid.len(), 2);
    assert_eq!(from_mid[0].record.value, "t1");
    assert_eq!(from_mid[1].record.value, "t2");
}

#[test]
fn time_polling_count_limit_returns_one() {
    // Setup: file topic log and three records with increasing timestamps
    let config = TestConfig::new("time_poll_limit");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();

    log.append(Record::new(None, "t0".to_string(), None))
        .unwrap();
    thread::sleep(StdDuration::from_millis(10));
    log.append(Record::new(None, "t1".to_string(), None))
        .unwrap();
    thread::sleep(StdDuration::from_millis(10));
    log.append(Record::new(None, "t2".to_string(), None))
        .unwrap();

    // Setup: capture middle record timestamp
    let all = log.get_records_from_offset(0, None).unwrap();
    assert_eq!(all.len(), 3);
    let mid_ts = all[1].timestamp.clone();

    // Action: poll from middle timestamp with count limit 1
    let limited = log.get_records_from_timestamp(&mid_ts, Some(1)).unwrap();

    // Expectation: returns exactly one record t1
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].record.value, "t1");
}

#[test]
fn time_polling_future_timestamp_returns_empty() {
    // Setup: file topic log and three records with increasing timestamps
    let config = TestConfig::new("time_poll_future");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();

    log.append(Record::new(None, "t0".to_string(), None))
        .unwrap();
    thread::sleep(StdDuration::from_millis(10));
    log.append(Record::new(None, "t1".to_string(), None))
        .unwrap();
    thread::sleep(StdDuration::from_millis(10));
    log.append(Record::new(None, "t2".to_string(), None))
        .unwrap();

    // Setup: compute a future timestamp
    let all = log.get_records_from_offset(0, None).unwrap();
    assert_eq!(all.len(), 3);
    let last_ts = all[2].timestamp.clone();
    let dt: DateTime<FixedOffset> = DateTime::parse_from_rfc3339(&last_ts).unwrap();
    let future = (dt + Duration::seconds(1)).to_rfc3339();

    // Action: poll from future timestamp
    let none = log.get_records_from_timestamp(&future, None).unwrap();

    // Expectation: returns empty
    assert_eq!(none.len(), 0);
}

// Merged correctness-focused tests below

#[test]
fn equal_timestamp_returns_full_count() {
    // Setup: file backend with large batch_bytes to keep a single write chunk
    let data_dir = unique_test_dir("time_polling");
    std::fs::create_dir_all(&data_dir).unwrap();
    let backend = StorageBackend::new_file_with_path_and_batch_bytes(
        SyncMode::Immediate,
        &data_dir,
        10 * 1024 * 1024, // 10MB to avoid chunking
    )
    .expect("file backend");

    let topic = "time_polling_topic";
    let topic_log = backend.create(topic).expect("create topic");
    let mut log = topic_log.write();

    // Setup: create and append a batch of records (single batch timestamp)
    let n = 100usize;
    let mut batch = Vec::with_capacity(n);
    for i in 0..n {
        batch.push(Record::new(None, format!("v{i}"), None));
    }
    let last = log.append_batch(batch).expect("append batch");
    assert_eq!(last as usize, n - 1);

    // Setup: read the first record to capture the batch timestamp used in serialization
    let first: Vec<RecordWithOffset> = log
        .get_records_from_offset(0, Some(1))
        .expect("read first record");
    assert_eq!(first.len(), 1);
    let ts = first[0].timestamp.clone();

    // Action: request from the exact timestamp
    let got = log
        .get_records_from_timestamp(&ts, None)
        .expect("read from timestamp");

    // Expectation: returns the full batch count
    assert_eq!(got.len(), n);
}

#[test]
fn equal_timestamp_returns_full_offsets() {
    // Setup: file backend with large batch_bytes to keep a single write chunk
    let data_dir = unique_test_dir("time_polling_offsets");
    std::fs::create_dir_all(&data_dir).unwrap();
    let backend = StorageBackend::new_file_with_path_and_batch_bytes(
        SyncMode::Immediate,
        &data_dir,
        10 * 1024 * 1024, // 10MB to avoid chunking
    )
    .expect("file backend");

    let topic = "time_polling_topic_offsets";
    let topic_log = backend.create(topic).expect("create topic");
    let mut log = topic_log.write();

    // Setup: create and append a batch of records (single batch timestamp)
    let n = 100usize;
    let mut batch = Vec::with_capacity(n);
    for i in 0..n {
        batch.push(Record::new(None, format!("v{i}"), None));
    }
    let last = log.append_batch(batch).expect("append batch");
    assert_eq!(last as usize, n - 1);

    // Setup: read the first record to capture the batch timestamp used in serialization
    let first: Vec<RecordWithOffset> = log
        .get_records_from_offset(0, Some(1))
        .expect("read first record");
    assert_eq!(first.len(), 1);
    let ts = first[0].timestamp.clone();

    // Action: request from the exact timestamp
    let got = log
        .get_records_from_timestamp(&ts, None)
        .expect("read from timestamp");

    // Expectation: offsets span from 0 to n-1
    assert_eq!(got[0].offset, 0);
    assert_eq!(got.last().unwrap().offset, (n - 1) as u64);
}

#[test]
fn spans_segments_returns_only_second_batch_count() {
    // Setup: file backend with tiny segment size to force a roll between batches
    let data_dir = unique_test_dir("time_polling_multi_segment");
    std::fs::create_dir_all(&data_dir).unwrap();

    // small segment size (4KB) to guarantee roll after first batch
    let backend =
        StorageBackend::new_file_with_config(SyncMode::Immediate, &data_dir, 1000, 4 * 1024)
            .expect("file backend");

    let topic = "time_polling_multi_segment_topic";
    let topic_log = backend.create(topic).expect("create topic");
    let mut log = topic_log.write();

    // Setup: first batch large enough so that two batches won't fit in one tiny segment
    let n1 = 4usize;
    let mut batch1 = Vec::with_capacity(n1);
    for i in 0..n1 {
        batch1.push(Record::new(None, format!("{}:{}", i, big_val(1024)), None));
    }
    let last1 = log.append_batch(batch1).expect("append batch1");
    assert!(last1 >= (n1 as u64 - 1));

    // Setup: ensure the next batch has a strictly later timestamp than batch1
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Setup: remember where the next batch will start (start of new segment after roll)
    let start2 = log.next_offset();

    // Setup: second batch should trigger roll due to small segment size
    let n2 = 3usize;
    let mut batch2 = Vec::with_capacity(n2);
    for i in 0..n2 {
        batch2.push(Record::new(
            None,
            format!("b2:{}:{}", i, big_val(1024)),
            None,
        ));
    }
    let last2 = log.append_batch(batch2).expect("append batch2");
    assert_eq!(last2, start2 + (n2 as u64) - 1);

    // Setup: read the first record of the second batch to get its timestamp
    let first_of_seg2: Vec<RecordWithOffset> = log
        .get_records_from_offset(start2, Some(1))
        .expect("read first of seg2");
    assert_eq!(first_of_seg2.len(), 1);
    let ts2 = first_of_seg2[0].timestamp.clone();

    // Action: poll from the exact timestamp of the 2nd segment's first record
    let got = log
        .get_records_from_timestamp(&ts2, None)
        .expect("read from ts2");

    // Expectation: returns exactly the n2 records from the second batch
    assert_eq!(got.len(), n2);
}

#[test]
fn spans_segments_returns_only_second_batch_offsets() {
    // Setup: file backend with tiny segment size to force a roll between batches
    let data_dir = unique_test_dir("time_polling_multi_segment_offsets");
    std::fs::create_dir_all(&data_dir).unwrap();

    // small segment size (4KB) to guarantee roll after first batch
    let backend =
        StorageBackend::new_file_with_config(SyncMode::Immediate, &data_dir, 1000, 4 * 1024)
            .expect("file backend");

    let topic = "time_polling_multi_segment_topic_offsets";
    let topic_log = backend.create(topic).expect("create topic");
    let mut log = topic_log.write();

    // Setup: first batch large enough so that two batches won't fit in one tiny segment
    let n1 = 4usize;
    let mut batch1 = Vec::with_capacity(n1);
    for i in 0..n1 {
        batch1.push(Record::new(None, format!("{}:{}", i, big_val(1024)), None));
    }
    let last1 = log.append_batch(batch1).expect("append batch1");
    assert!(last1 >= (n1 as u64 - 1));

    // Setup: ensure the next batch has a strictly later timestamp than batch1
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Setup: remember where the next batch will start (start of new segment after roll)
    let start2 = log.next_offset();

    // Setup: second batch should trigger roll due to small segment size
    let n2 = 3usize;
    let mut batch2 = Vec::with_capacity(n2);
    for i in 0..n2 {
        batch2.push(Record::new(
            None,
            format!("b2:{}:{}", i, big_val(1024)),
            None,
        ));
    }
    let last2 = log.append_batch(batch2).expect("append batch2");
    assert_eq!(last2, start2 + (n2 as u64) - 1);

    // Setup: read the first record of the second batch to get its timestamp
    let first_of_seg2: Vec<RecordWithOffset> = log
        .get_records_from_offset(start2, Some(1))
        .expect("read first of seg2");
    assert_eq!(first_of_seg2.len(), 1);
    let ts2 = first_of_seg2[0].timestamp.clone();

    // Action: poll from the exact timestamp of the 2nd segment's first record
    let got = log
        .get_records_from_timestamp(&ts2, None)
        .expect("read from ts2");

    // Expectation: offsets align to the second batch range
    assert_eq!(got[0].offset, start2);
    assert_eq!(got.last().unwrap().offset, start2 + (n2 as u64) - 1);
}
