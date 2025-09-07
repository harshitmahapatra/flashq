use flashq::storage::backend::StorageBackend;
use flashq::storage::file::SyncMode;
use flashq::{Record, RecordWithOffset};

fn unique_test_dir(name: &str) -> std::path::PathBuf {
    let mut dir = std::path::PathBuf::from("target/test_data");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    dir.push(format!("{name}_{nanos}"));
    dir
}

#[test]
fn returns_full_batch_on_equal_timestamp() {
    // Arrange: file backend with large batch_bytes to keep a single write chunk
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
    let mut log = topic_log.write().unwrap();

    // Create a batch of records
    let n = 100usize;
    let mut batch = Vec::with_capacity(n);
    for i in 0..n {
        batch.push(Record::new(None, format!("v{i}"), None));
    }

    // Act: append as one batch (single timestamp for the batch)
    let last = log.append_batch(batch).expect("append batch");
    assert_eq!(last as usize, n - 1);

    // Read the first record to capture the batch timestamp used in serialization
    let first: Vec<RecordWithOffset> = log
        .get_records_from_offset(0, Some(1))
        .expect("read first record");
    assert_eq!(first.len(), 1);
    let ts = first[0].timestamp.clone();

    // Now request from the exact timestamp
    let got = log
        .get_records_from_timestamp(&ts, None)
        .expect("read from timestamp");

    // Assert: should return the full batch (no boundary misses)
    assert_eq!(got.len(), n);
    assert_eq!(got[0].offset, 0);
    assert_eq!(got.last().unwrap().offset, (n - 1) as u64);
}

#[test]
fn time_polling_spans_segments() {
    // Arrange: file backend with tiny segment size to force a roll between batches
    let data_dir = unique_test_dir("time_polling_multi_segment");
    std::fs::create_dir_all(&data_dir).unwrap();

    // small segment size (4KB) to guarantee roll after first batch
    let backend =
        StorageBackend::new_file_with_config(SyncMode::Immediate, &data_dir, 1000, 4 * 1024)
            .expect("file backend");

    let topic = "time_polling_multi_segment_topic";
    let topic_log = backend.create(topic).expect("create topic");
    let mut log = topic_log.write().unwrap();

    // helper to build a big string
    fn big_val(len: usize) -> String {
        std::iter::repeat_n('x', len).collect()
    }

    // First batch: large enough so that two batches won't fit in one tiny segment
    let n1 = 4usize;
    let mut batch1 = Vec::with_capacity(n1);
    for i in 0..n1 {
        batch1.push(Record::new(None, format!("{}:{}", i, big_val(1024)), None));
    }
    let last1 = log.append_batch(batch1).expect("append batch1");
    assert!(last1 >= (n1 as u64 - 1));

    // Ensure the next batch has a strictly later timestamp than batch1
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Remember where the next batch will start (start of new segment after roll)
    let start2 = log.next_offset();

    // Second batch: should trigger roll due to small segment size
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

    // Read the first record of the second batch to get its timestamp
    let first_of_seg2: Vec<RecordWithOffset> = log
        .get_records_from_offset(start2, Some(1))
        .expect("read first of seg2");
    assert_eq!(first_of_seg2.len(), 1);
    let ts2 = first_of_seg2[0].timestamp.clone();

    // Act: poll from the exact timestamp of the 2nd segment's first record
    let got = log
        .get_records_from_timestamp(&ts2, None)
        .expect("read from ts2");

    // Assert: should return exactly the n2 records from the second batch, starting at start2
    assert!(
        got.len() >= n2,
        "expected at least {} records, got {}",
        n2,
        got.len()
    );
    assert_eq!(got[0].offset, start2);
    assert_eq!(got[n2 - 1].offset, start2 + (n2 as u64) - 1);
}
