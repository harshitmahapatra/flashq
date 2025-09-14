use flashq::storage::StorageBackend;
use flashq::storage::file::SyncMode;
use flashq::{FlashQ, Record};
use test_log::test;

fn make_record(i: usize) -> Record {
    Record::new(Some(format!("k{i}")), "x".repeat(16), None)
}

#[test]
fn memory_append_read_batches_boundaries() {
    let backend = StorageBackend::new_memory_with_batch_bytes(128 * 1024);
    let queue = FlashQ::with_storage_backend(backend);
    for &n in &[127usize, 128, 129, 300] {
        let topic = format!("batch_mem_test_{n}");
        let recs: Vec<_> = (0..n).map(make_record).collect();
        let last = queue.post_records(topic.clone(), recs.clone()).unwrap();
        assert!(last as usize >= n - 1);

        let out = queue.poll_records(&topic, Some(n)).unwrap();
        assert_eq!(out.len(), n);
        for (i, r) in out.iter().enumerate() {
            assert_eq!(r.offset as usize, i);
            assert_eq!(r.record.key.as_deref(), Some(format!("k{i}").as_str()));
        }

        // partial read across boundary
        let mid = n / 3;
        let len = (n - mid).min(200);
        let part = queue
            .poll_records_from_offset(&topic, mid as u64, Some(len))
            .unwrap();
        assert_eq!(part.len(), len);
        for (i, r) in part.iter().enumerate() {
            let idx = mid + i;
            assert_eq!(r.offset as usize, idx);
            assert_eq!(r.record.key.as_deref(), Some(format!("k{idx}").as_str()));
        }
    }
}

#[test]
fn file_append_read_batches_boundaries() {
    let tmp = tempfile::tempdir().unwrap();
    let backend =
        StorageBackend::new_file_with_path_and_batch_bytes(SyncMode::None, tmp.path(), 128 * 1024)
            .unwrap();
    let queue = FlashQ::with_storage_backend(backend);
    let topic = "batch_file_test".to_string();

    for &n in &[127usize, 128, 129, 300] {
        let recs: Vec<_> = (0..n).map(make_record).collect();
        let last = queue.post_records(topic.clone(), recs.clone()).unwrap();
        assert!(last as usize >= n - 1);

        let out = queue.poll_records(&topic, Some(n)).unwrap();
        assert_eq!(out.len(), n);

        // partial read across boundary
        let mid = n / 3;
        let len = (n - mid).min(200);
        let part = queue
            .poll_records_from_offset(&topic, mid as u64, Some(len))
            .unwrap();
        assert_eq!(part.len(), len);
    }
}
