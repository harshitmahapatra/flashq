use divan::{AllocProfiler, Bencher, black_box};
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

#[divan::bench]
fn empty_topic_write_throughput(bencher: Bencher) {
    bencher.bench(|| {
        let queue = FlashQ::new(); // Uses memory backend by default
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
        let queue = FlashQ::new();
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
fn large_dataset_write_throughput(bencher: Bencher) {
    bencher.bench(|| {
        let queue = FlashQ::new();
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
fn large_dataset_read_throughput(bencher: Bencher) {
    bencher.bench(|| {
        let queue = FlashQ::new();
        let topic = "benchmark".to_string();

        // Pre-populate with 2,000 records (~2MB)
        for i in 0..2_000 {
            let record = create_1kb_record(i);
            queue.post_record(topic.clone(), record).unwrap();
        }

        // Benchmark reading recent 100 records
        let records = black_box(
            queue
                .poll_records_from_offset(&topic, 1_900, Some(100))
                .unwrap(),
        );
        assert_eq!(records.len(), 100);
    });
}
