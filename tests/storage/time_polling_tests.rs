use super::test_utilities::*;
use flashq::Record;
use flashq::storage::TopicLog;
use flashq::storage::file::FileTopicLog;
use chrono::{DateTime, Duration, FixedOffset};
use std::thread;
use std::time::Duration as StdDuration;

#[test]
fn test_time_based_polling_basic() {
    let config = TestConfig::new("time_poll_basic");
    let mut log = FileTopicLog::new(
        &config.topic_name,
        config.sync_mode,
        config.temp_dir_path(),
        config.segment_size,
    )
    .unwrap();

    log.append(Record::new(None, "t0".to_string(), None)).unwrap();
    thread::sleep(StdDuration::from_millis(10));
    log.append(Record::new(None, "t1".to_string(), None)).unwrap();
    thread::sleep(StdDuration::from_millis(10));
    log.append(Record::new(None, "t2".to_string(), None)).unwrap();

    // Capture timestamps
    let all = log.get_records_from_offset(0, None).unwrap();
    assert_eq!(all.len(), 3);
    let mid_ts = all[1].timestamp.clone();
    let last_ts = all[2].timestamp.clone();

    // Poll from middle timestamp
    let from_mid = log.get_records_from_timestamp(&mid_ts, None).unwrap();
    assert_eq!(from_mid.len(), 2);
    assert_eq!(from_mid[0].record.value, "t1");
    assert_eq!(from_mid[1].record.value, "t2");

    // Limit count
    let limited = log.get_records_from_timestamp(&mid_ts, Some(1)).unwrap();
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].record.value, "t1");

    // Future timestamp yields empty
    let dt: DateTime<FixedOffset> = DateTime::parse_from_rfc3339(&last_ts).unwrap();
    let future = (dt + Duration::seconds(1)).to_rfc3339();
    let none = log.get_records_from_timestamp(&future, None).unwrap();
    assert_eq!(none.len(), 0);
}

