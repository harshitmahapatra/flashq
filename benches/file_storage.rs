use divan::{AllocProfiler, Bencher, black_box};
use flashq::storage::{StorageBackend, file::SyncMode};
use flashq::{FlashQ, Record};
use std::collections::HashMap;
use tempfile::TempDir;

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn main() {
    divan::main();
}

fn create_1kb_record(index: usize) -> Record {
    let payload = "x".repeat(1024);
    Record::new(
        Some(format!("key_{index}")),
        payload,
        Some({
            let mut headers = HashMap::new();
            headers.insert("index".to_string(), index.to_string());
            headers
        }),
    )
}

fn create_file_storage_queue() -> (FlashQ, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let storage_backend = StorageBackend::new_file_with_path(
        SyncMode::None, // Use None for best benchmark performance
        temp_dir.path(),
    )
    .expect("Failed to create file storage backend");

    let queue = FlashQ::with_storage_backend(storage_backend);
    (queue, temp_dir)
}

#[divan::bench]
fn empty_topic_write_throughput(bencher: Bencher) {
    bencher.bench(|| {
        let (queue, _temp_dir) = create_file_storage_queue();
        let topic = "benchmark".to_string();

        for i in 0..500 {
            let record = create_1kb_record(i);
            black_box(queue.post_record(topic.clone(), record).unwrap());
        }
    });
}

#[divan::bench]
fn empty_topic_read_throughput(bencher: Bencher) {
    bencher.bench(|| {
        let (queue, _temp_dir) = create_file_storage_queue();
        let topic = "benchmark".to_string();

        // Pre-populate with 500 records (~500KB)
        for i in 0..500 {
            let record = create_1kb_record(i);
            queue.post_record(topic.clone(), record).unwrap();
        }

        // Benchmark reading all records back
        let records = black_box(queue.poll_records(&topic, None).unwrap());
        assert_eq!(records.len(), 500);
    });
}

#[divan::bench]
fn large_file_write_throughput(bencher: Bencher) {
    bencher.bench(|| {
        let (queue, _temp_dir) = create_file_storage_queue();
        let topic = "benchmark".to_string();

        // Pre-populate with 2,000 records (~2MB)
        for i in 0..2_000 {
            let record = create_1kb_record(i);
            queue.post_record(topic.clone(), record).unwrap();
        }

        // Benchmark writing 100 more records
        for i in 2_000..2_100 {
            let record = create_1kb_record(i);
            black_box(queue.post_record(topic.clone(), record).unwrap());
        }
    });
}

#[divan::bench]
fn large_file_read_throughput(bencher: Bencher) {
    bencher.bench(|| {
        let (queue, _temp_dir) = create_file_storage_queue();
        let topic = "benchmark".to_string();

        // Pre-populate with 2,000 records (~2MB)
        for i in 0..2_000 {
            let record = create_1kb_record(i);
            queue.post_record(topic.clone(), record).unwrap();
        }

        // Benchmark reading recent 100 records (measures index memory overhead)
        let records = black_box(
            queue
                .poll_records_from_offset(&topic, 1_900, Some(100))
                .unwrap(),
        );
        assert_eq!(records.len(), 100);
    });
}
