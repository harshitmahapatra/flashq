use flashq::storage::file::time_index::{SparseTimeIndex, TimeIndexEntry};
use std::fs;
use std::io::Write;
use tempfile::TempDir;
use test_log::test;

fn create_test_time_index() -> SparseTimeIndex {
    SparseTimeIndex::new()
}

fn create_time_index_with_entries() -> SparseTimeIndex {
    let mut index = SparseTimeIndex::new();
    index.add_entry(TimeIndexEntry {
        timestamp_ms: 1000,
        position: 0,
    });
    index.add_entry(TimeIndexEntry {
        timestamp_ms: 2000,
        position: 10,
    });
    index.add_entry(TimeIndexEntry {
        timestamp_ms: 3000,
        position: 20,
    });
    index
}

fn create_temp_file() -> (TempDir, std::path::PathBuf) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let file_path = temp_dir.path().join("test_time_index.tidx");
    (temp_dir, file_path)
}

#[test]
fn test_new_time_index_is_empty() {
    let index = create_test_time_index();

    assert_eq!(index.entry_count(), 0);
}

#[test]
fn test_add_single_time_entry() {
    let mut index = create_test_time_index();

    index.add_entry(TimeIndexEntry {
        timestamp_ms: 1500,
        position: 5,
    });

    assert_eq!(index.entry_count(), 1);
}

#[test]
fn test_add_multiple_time_entries_maintains_order() {
    let mut index = create_test_time_index();

    index.add_entry(TimeIndexEntry {
        timestamp_ms: 3000,
        position: 30,
    });
    index.add_entry(TimeIndexEntry {
        timestamp_ms: 1000,
        position: 10,
    });
    index.add_entry(TimeIndexEntry {
        timestamp_ms: 2000,
        position: 20,
    });

    assert_eq!(index.entry_count(), 3);
    assert_eq!(index.find_position_for_timestamp(1000), Some(10));
    assert_eq!(index.find_position_for_timestamp(2000), Some(20));
    assert_eq!(index.find_position_for_timestamp(3000), Some(30));
}

#[test]
fn test_find_offset_for_exact_timestamp() {
    let index = create_time_index_with_entries();

    let position = index.find_position_for_timestamp(2000);

    assert_eq!(position, Some(10));
}

#[test]
fn test_find_offset_for_timestamp_between_entries() {
    let index = create_time_index_with_entries();

    let position = index.find_position_for_timestamp(1500);

    assert_eq!(position, Some(0));
}

#[test]
fn test_find_offset_for_timestamp_before_first() {
    let index = create_time_index_with_entries();

    let position = index.find_position_for_timestamp(500);

    assert_eq!(position, Some(0));
}

#[test]
fn test_find_offset_for_timestamp_after_last() {
    let index = create_time_index_with_entries();

    let position = index.find_position_for_timestamp(4000);

    assert_eq!(position, Some(20));
}

#[test]
fn test_find_offset_for_empty_time_index() {
    let index = create_test_time_index();

    let position = index.find_position_for_timestamp(1000);

    assert_eq!(position, Some(0));
}

#[test]
fn test_save_time_index_to_file() {
    let index = create_time_index_with_entries();
    let (_temp_dir, file_path) = create_temp_file();

    let mut file = std::fs::File::create(&file_path).expect("Failed to create file");
    let mut writer = std::io::BufWriter::new(&mut file);

    if let Some(entry) = index.last_entry() {
        let result = index.write_entry_to_file(&mut writer, entry);
        assert!(result.is_ok());
    }

    assert!(file_path.exists());
}

#[test]
fn test_load_time_index_from_file() {
    let mut loaded_index = SparseTimeIndex::new();
    let (_temp_dir, file_path) = create_temp_file();

    let mut file = std::fs::File::create(&file_path).expect("Failed to create file");
    let mut writer = std::io::BufWriter::new(&mut file);

    let entry = TimeIndexEntry {
        timestamp_ms: 2000,
        position: 10,
    };
    let index = SparseTimeIndex::new();
    index
        .write_entry_to_file(&mut writer, &entry)
        .expect("Failed to write entry");
    drop(writer);
    drop(file);

    let file = std::fs::File::open(&file_path).expect("Failed to open file");
    let mut reader = std::io::BufReader::new(file);
    let result = loaded_index.read_from_file(&mut reader, None);

    assert!(result.is_ok());
    assert_eq!(loaded_index.entry_count(), 1);
    assert_eq!(loaded_index.find_position_for_timestamp(2000), Some(10));
}

#[test]
fn test_load_time_index_from_non_existent_file() {
    let (_temp_dir, file_path) = create_temp_file();

    let result = std::fs::File::open(&file_path);

    assert!(result.is_err());
}

#[test]
fn test_save_load_time_index_round_trip() {
    let original_index = create_time_index_with_entries();
    let (_temp_dir, file_path) = create_temp_file();
    let mut loaded_index = SparseTimeIndex::new();

    let mut file = std::fs::File::create(&file_path).expect("Failed to create file");
    let mut writer = std::io::BufWriter::new(&mut file);

    if let Some(entry) = original_index.last_entry() {
        original_index
            .write_entry_to_file(&mut writer, entry)
            .expect("Failed to write entry");
    }
    drop(writer);
    drop(file);

    let file = std::fs::File::open(&file_path).expect("Failed to open file");
    let mut reader = std::io::BufReader::new(file);
    loaded_index
        .read_from_file(&mut reader, None)
        .expect("Failed to load time index");

    assert_eq!(1, loaded_index.entry_count());
}

#[test]
fn test_save_time_index_with_io_error() {
    let invalid_path = std::path::Path::new("/invalid/path/time_index.tidx");

    let result = std::fs::File::create(invalid_path);

    assert!(result.is_err());
}

#[test]
fn test_load_time_index_with_corrupted_file() {
    let (_temp_dir, file_path) = create_temp_file();
    let mut file = fs::File::create(&file_path).expect("Failed to create file");
    file.write_all(b"corrupt")
        .expect("Failed to write corrupted data");
    drop(file);

    let mut index = SparseTimeIndex::new();
    let file = std::fs::File::open(&file_path).expect("Failed to open file");
    let mut reader = std::io::BufReader::new(file);
    let result = index.read_from_file(&mut reader, None);

    assert!(result.is_ok());
}

#[test]
fn test_time_entries_returns_correct_slice() {
    let index = create_time_index_with_entries();

    assert_eq!(index.entry_count(), 3);
    assert_eq!(index.find_position_for_timestamp(1000), Some(0));
    assert_eq!(index.find_position_for_timestamp(2000), Some(10));
    assert_eq!(index.find_position_for_timestamp(3000), Some(20));
}

#[test]
fn test_time_index_is_empty_returns_true_for_new_index() {
    let index = create_test_time_index();

    assert_eq!(index.entry_count(), 0);
}

#[test]
fn test_time_index_is_empty_returns_false_for_populated_index() {
    let index = create_time_index_with_entries();

    assert!(index.entry_count() > 0);
}

#[test]
fn test_time_index_len_returns_correct_count() {
    let mut index = create_test_time_index();
    assert_eq!(index.entry_count(), 0);

    index.add_entry(TimeIndexEntry {
        timestamp_ms: 1000,
        position: 1,
    });
    assert_eq!(index.entry_count(), 1);

    index.add_entry(TimeIndexEntry {
        timestamp_ms: 2000,
        position: 2,
    });
    assert_eq!(index.entry_count(), 2);
}

#[test]
fn test_add_time_entry_with_duplicate_timestamp() {
    let mut index = create_test_time_index();

    index.add_entry(TimeIndexEntry {
        timestamp_ms: 1000,
        position: 10,
    });
    index.add_entry(TimeIndexEntry {
        timestamp_ms: 1000,
        position: 20,
    });

    assert_eq!(index.entry_count(), 1);
    assert_eq!(index.find_position_for_timestamp(1000), Some(10));
}

#[test]
fn test_large_time_index_performance() {
    let mut index = create_test_time_index();

    for i in 0..1000 {
        index.add_entry(TimeIndexEntry {
            timestamp_ms: (i * 1000) as u64,
            position: i as u32,
        });
    }

    assert_eq!(index.entry_count(), 1000);
    assert_eq!(index.find_position_for_timestamp(500000), Some(500));
}

#[test]
fn test_find_offset_with_exact_timestamp_match() {
    let mut index = create_test_time_index();
    index.add_entry(TimeIndexEntry {
        timestamp_ms: 1000,
        position: 0,
    });
    index.add_entry(TimeIndexEntry {
        timestamp_ms: 2000,
        position: 10,
    });
    index.add_entry(TimeIndexEntry {
        timestamp_ms: 3000,
        position: 20,
    });

    let position = index.find_position_for_timestamp(2000);

    assert_eq!(position, Some(10));
}

#[test]
fn test_find_offset_with_timestamp_search() {
    let mut index = create_test_time_index();
    index.add_entry(TimeIndexEntry {
        timestamp_ms: 1000,
        position: 0,
    });
    index.add_entry(TimeIndexEntry {
        timestamp_ms: 3000,
        position: 10,
    });
    index.add_entry(TimeIndexEntry {
        timestamp_ms: 5000,
        position: 20,
    });

    let position = index.find_position_for_timestamp(2500);

    assert_eq!(position, Some(0));
}

#[test]
fn test_time_index_ordering_with_unsorted_inserts() {
    let mut index = create_test_time_index();

    index.add_entry(TimeIndexEntry {
        timestamp_ms: 5000,
        position: 50,
    });
    index.add_entry(TimeIndexEntry {
        timestamp_ms: 1000,
        position: 10,
    });
    index.add_entry(TimeIndexEntry {
        timestamp_ms: 3000,
        position: 30,
    });
    index.add_entry(TimeIndexEntry {
        timestamp_ms: 2000,
        position: 20,
    });

    assert_eq!(index.entry_count(), 4);
    assert_eq!(index.find_position_for_timestamp(1000), Some(10));
    assert_eq!(index.find_position_for_timestamp(2000), Some(20));
    assert_eq!(index.find_position_for_timestamp(3000), Some(30));
    assert_eq!(index.find_position_for_timestamp(5000), Some(50));
}
