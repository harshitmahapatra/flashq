use divan::{AllocProfiler, Bencher, black_box};
use flashq::{FlashQ, Record};
use std::collections::HashMap;

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn main() {
    flashq::telemetry::init_for_benchmarks();
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

#[divan::bench]
fn empty_topic_write_throughput(bencher: Bencher) {
    bencher.bench(|| {
        let queue = FlashQ::new(); // Uses memory backend by default
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
        let queue = FlashQ::new();
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
fn large_dataset_write_throughput(bencher: Bencher) {
    bencher.bench(|| {
        let queue = FlashQ::new();
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
fn large_dataset_read_throughput(bencher: Bencher) {
    bencher.bench(|| {
        let queue = FlashQ::new();
        let topic = "benchmark".to_string();

        // Pre-populate with 20,000 records (~20MB)
        for i in 0..20_000 {
            let record = create_1kb_record(i);
            queue.post_records(topic.clone(), vec![record]).unwrap();
        }

        // Benchmark reading recent 1000 records
        let records = black_box(
            queue
                .poll_records_from_offset(&topic, 19_000, Some(1000))
                .unwrap(),
        );
        assert_eq!(records.len(), 1000);
    });
}

#[divan::bench]
fn time_read_from_start_memory(bencher: Bencher) {
    bencher.bench(|| {
        let queue = FlashQ::new();
        let topic = "benchmark_time_mem".to_string();

        // Pre-populate with 20,000 records (~20MB)
        for i in 0..20_000 {
            let record = create_1kb_record(i);
            queue.post_records(topic.clone(), vec![record]).unwrap();
        }

        // Capture timestamp at offset 0
        let ts0 = queue.poll_records_from_offset(&topic, 0, Some(1)).unwrap()[0]
            .timestamp
            .clone();

        let out = black_box(
            queue
                .poll_records_from_time(&topic, &ts0, Some(1000))
                .unwrap(),
        );
        assert_eq!(out.len(), 1000);
    });
}

#[divan::bench]
fn time_read_from_middle_memory(bencher: Bencher) {
    bencher.bench(|| {
        let queue = FlashQ::new();
        let topic = "benchmark_time_mem".to_string();

        for i in 0..20_000 {
            let record = create_1kb_record(i);
            queue.post_records(topic.clone(), vec![record]).unwrap();
        }

        // Capture timestamp around the middle
        let ts_mid = queue
            .poll_records_from_offset(&topic, 10_000, Some(1))
            .unwrap()[0]
            .timestamp
            .clone();

        let out = black_box(
            queue
                .poll_records_from_time(&topic, &ts_mid, Some(1000))
                .unwrap(),
        );
        assert_eq!(out.len(), 1000);
    });
}

#[divan::bench]
fn time_read_from_end_memory(bencher: Bencher) {
    bencher.bench(|| {
        let queue = FlashQ::new();
        let topic = "benchmark_time_mem".to_string();

        for i in 0..20_000 {
            let record = create_1kb_record(i);
            queue.post_records(topic.clone(), vec![record]).unwrap();
        }

        // Near end
        let start = 19_950u64;
        let ts_near_end = queue
            .poll_records_from_offset(&topic, start, Some(1))
            .unwrap()[0]
            .timestamp
            .clone();

        let out = black_box(
            queue
                .poll_records_from_time(&topic, &ts_near_end, Some(1000))
                .unwrap(),
        );
        assert!(!out.is_empty());
        assert!(out.len() <= 1000);
        assert_eq!(out.last().unwrap().offset, 19_999);
    });
}
