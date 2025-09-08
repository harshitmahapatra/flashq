use divan::{AllocProfiler, Bencher, black_box};
use flashq::storage::{StorageBackend, file::SyncMode};
use flashq::{FlashQ, Record};
use std::collections::HashMap;

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

fn gen_records(n: usize) -> Vec<Record> {
    (0..n).map(create_1kb_record).collect()
}

// =======================
// Memory backend benches
// =======================

#[divan::bench]
fn mem_batch_post_1k(bencher: Bencher) {
    bencher.bench(|| {
        let queue = FlashQ::new();
        let topic = "batch_mem".to_string();
        let recs = gen_records(1_000);
        let last = queue.post_records(topic.clone(), recs).unwrap();
        black_box(last);
    });
}

#[divan::bench]
fn mem_batch_post_10k(bencher: Bencher) {
    bencher.bench(|| {
        let queue = FlashQ::new();
        let topic = "batch_mem".to_string();
        let recs = gen_records(10_000);
        let last = queue.post_records(topic.clone(), recs).unwrap();
        black_box(last);
    });
}

#[divan::bench]
fn mem_batch_read_10k(bencher: Bencher) {
    bencher.bench(|| {
        let queue = FlashQ::new();
        let topic = "batch_mem".to_string();
        // Pre-populate
        let recs = gen_records(10_000);
        queue.post_records(topic.clone(), recs).unwrap();

        // Read all in one call (baseline before batching impl)
        let out = queue
            .poll_records_from_offset(&topic, 0, Some(10_000))
            .unwrap();
        black_box(out);
    });
}

// =======================
// File backend benches
// =======================

fn create_file_storage_queue() -> (FlashQ, tempfile::TempDir) {
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
fn file_batch_post_1k(bencher: Bencher) {
    bencher.bench(|| {
        let (queue, _tmp) = create_file_storage_queue();
        let topic = "batch_file".to_string();
        let recs = gen_records(1_000);
        let last = queue.post_records(topic.clone(), recs).unwrap();
        black_box(last);
    });
}

#[divan::bench]
fn file_batch_post_10k(bencher: Bencher) {
    bencher.bench(|| {
        let (queue, _tmp) = create_file_storage_queue();
        let topic = "batch_file".to_string();
        let recs = gen_records(10_000);
        let last = queue.post_records(topic.clone(), recs).unwrap();
        black_box(last);
    });
}

#[divan::bench]
fn file_batch_read_10k(bencher: Bencher) {
    bencher.bench(|| {
        let (queue, _tmp) = create_file_storage_queue();
        let topic = "batch_file".to_string();

        // Pre-populate
        let recs = gen_records(10_000);
        queue.post_records(topic.clone(), recs).unwrap();

        // Read all in one call (baseline before batching impl)
        let out = queue
            .poll_records_from_offset(&topic, 0, Some(10_000))
            .unwrap();
        black_box(out);
    });
}

// =======================
// Time-based polling baselines
// =======================

fn get_timestamp_at_offset(queue: &FlashQ, topic: &str, offset: u64) -> String {
    let rec = queue
        .poll_records_from_offset(topic, offset, Some(1))
        .expect("read record for timestamp");
    rec.first().expect("record exists").timestamp.clone()
}

#[divan::bench]
fn mem_time_read_from_start_baseline(bencher: Bencher) {
    bencher.bench(|| {
        let queue = FlashQ::new();
        let topic = "batch_mem_time".to_string();
        // Pre-populate
        let recs = gen_records(10_000);
        queue.post_records(topic.clone(), recs).unwrap();

        // Read from time at start
        let ts0 = get_timestamp_at_offset(&queue, &topic, 0);
        let out = queue
            .poll_records_from_time(&topic, &ts0, Some(10_000))
            .unwrap();
        black_box(out);
    });
}

#[divan::bench]
fn mem_time_read_from_middle_baseline(bencher: Bencher) {
    bencher.bench(|| {
        let queue = FlashQ::new();
        let topic = "batch_mem_time".to_string();
        // Pre-populate
        let recs = gen_records(10_000);
        queue.post_records(topic.clone(), recs).unwrap();

        // Read from time near middle
        let ts_mid = get_timestamp_at_offset(&queue, &topic, 5_000);
        let out = queue
            .poll_records_from_time(&topic, &ts_mid, Some(10_000))
            .unwrap();
        black_box(out);
    });
}

#[divan::bench]
fn mem_time_read_from_end_baseline(bencher: Bencher) {
    bencher.bench(|| {
        let queue = FlashQ::new();
        let topic = "batch_mem_time".to_string();
        // Pre-populate
        let recs = gen_records(10_000);
        queue.post_records(topic.clone(), recs).unwrap();

        // Read from time at end (last record)
        let ts_last = get_timestamp_at_offset(&queue, &topic, 9_999);
        let out = queue
            .poll_records_from_time(&topic, &ts_last, Some(10_000))
            .unwrap();
        black_box(out);
    });
}

#[divan::bench]
fn file_time_read_from_start_baseline(bencher: Bencher) {
    bencher.bench(|| {
        let (queue, _tmp) = create_file_storage_queue();
        let topic = "batch_file_time".to_string();
        // Pre-populate
        let recs = gen_records(10_000);
        queue.post_records(topic.clone(), recs).unwrap();

        // Read from time at start
        let ts0 = get_timestamp_at_offset(&queue, &topic, 0);
        let out = queue
            .poll_records_from_time(&topic, &ts0, Some(10_000))
            .unwrap();
        black_box(out);
    });
}

#[divan::bench]
fn file_time_read_from_middle_baseline(bencher: Bencher) {
    bencher.bench(|| {
        let (queue, _tmp) = create_file_storage_queue();
        let topic = "batch_file_time".to_string();
        // Pre-populate
        let recs = gen_records(10_000);
        queue.post_records(topic.clone(), recs).unwrap();

        // Read from time near middle
        let ts_mid = get_timestamp_at_offset(&queue, &topic, 5_000);
        let out = queue
            .poll_records_from_time(&topic, &ts_mid, Some(10_000))
            .unwrap();
        black_box(out);
    });
}

#[divan::bench]
fn file_time_read_from_end_baseline(bencher: Bencher) {
    bencher.bench(|| {
        let (queue, _tmp) = create_file_storage_queue();
        let topic = "batch_file_time".to_string();
        // Pre-populate
        let recs = gen_records(10_000);
        queue.post_records(topic.clone(), recs).unwrap();

        // Read from time at end (last record)
        let ts_last = get_timestamp_at_offset(&queue, &topic, 9_999);
        let out = queue
            .poll_records_from_time(&topic, &ts_last, Some(10_000))
            .unwrap();
        black_box(out);
    });
}
