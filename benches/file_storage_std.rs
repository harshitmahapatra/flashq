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

// Minimal smoke benchmark to ensure std backend runs quickly.
// IGNORE THIS TEST FOR PERFORMANCE DOCS SINCE THIS HAS LOW SAMPLE_SIZE AND SAMPLE_COUNT
#[cfg(target_os = "linux")]
#[divan::bench(sample_size = 1, sample_count = 1)]
fn quick_smoke_std(bencher: Bencher) {
    bencher.bench(|| {
        let (queue, _temp_dir) = create_file_storage_queue();
        let topic = "smoke".to_string();

        for i in 0..5000 {
            let record = create_1kb_record(i);
            black_box(queue.post_records(topic.clone(), vec![record]).unwrap());
        }

        let records = black_box(queue.poll_records(&topic, None).unwrap());
        assert_eq!(records.len(), 5000);
    });
}

#[divan::bench]
fn empty_topic_write_throughput(bencher: Bencher) {
    bencher.bench(|| {
        let (queue, _temp_dir) = create_file_storage_queue();
        let topic = "benchmark".to_string();

        for i in 0..5000 {
            let record = create_1kb_record(i);
            black_box(queue.post_records(topic.clone(), vec![record]).unwrap());
        }
    });
}

#[divan::bench]
fn empty_topic_read_throughput(bencher: Bencher) {
    bencher.bench(|| {
        let (queue, _temp_dir) = create_file_storage_queue();
        let topic = "benchmark".to_string();

        // Pre-populate with 5000 records (~5MB)
        for i in 0..5000 {
            let record = create_1kb_record(i);
            queue.post_records(topic.clone(), vec![record]).unwrap();
        }

        // Benchmark reading all records back
        let records = black_box(queue.poll_records(&topic, None).unwrap());
        assert_eq!(records.len(), 5000);
    });
}

#[divan::bench]
fn large_file_write_throughput(bencher: Bencher) {
    bencher.bench(|| {
        let (queue, _temp_dir) = create_file_storage_queue();
        let topic = "benchmark".to_string();

        // Pre-populate with 20,000 records (~20MB)
        for i in 0..20_000 {
            let record = create_1kb_record(i);
            queue.post_records(topic.clone(), vec![record]).unwrap();
        }

        // Benchmark writing 1000 more records
        for i in 20_000..21_000 {
            let record = create_1kb_record(i);
            black_box(queue.post_records(topic.clone(), vec![record]).unwrap());
        }
    });
}

#[divan::bench]
fn large_file_read_throughput(bencher: Bencher) {
    bencher.bench(|| {
        let (queue, _temp_dir) = create_file_storage_queue();
        let topic = "benchmark".to_string();

        // Pre-populate with 20,000 records (~20MB)
        for i in 0..20_000 {
            let record = create_1kb_record(i);
            queue.post_records(topic.clone(), vec![record]).unwrap();
        }

        // Benchmark reading recent 1000 records (measures index memory overhead)
        let records = black_box(
            queue
                .poll_records_from_offset(&topic, 19_000, Some(1000))
                .unwrap(),
        );
        assert_eq!(records.len(), 1000);
    });
}
