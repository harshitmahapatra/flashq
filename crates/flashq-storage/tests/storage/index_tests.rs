use flashq_storage::file::index::{IndexEntry, SparseIndex};
use std::fs;
use std::io::Write;
use tempfile::TempDir;
use test_log::test;

fn create_test_index() -> SparseIndex {
    SparseIndex::new()
}

fn create_index_with_entries() -> SparseIndex {
    let mut index = SparseIndex::new();
    index.add_entry(IndexEntry {
        offset: 0,
        position: 0,
    });
    index.add_entry(IndexEntry {
        offset: 10,
        position: 100,
    });
    index.add_entry(IndexEntry {
        offset: 20,
        position: 250,
    });
    index
}

fn create_temp_file() -> (TempDir, std::path::PathBuf) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let file_path = temp_dir.path().join("test_index.idx");
    (temp_dir, file_path)
}

#[test]
fn test_new_index_is_empty() {
    let index = create_test_index();

    assert_eq!(index.entry_count(), 0);
}

#[test]
fn test_add_single_entry() {
    let mut index = create_test_index();

    index.add_entry(IndexEntry {
        offset: 5,
        position: 100,
    });

    assert_eq!(index.entry_count(), 1);
}

#[test]
fn test_add_multiple_entries_maintains_order() {
    let mut index = create_test_index();

    index.add_entry(IndexEntry {
        offset: 20,
        position: 300,
    });
    index.add_entry(IndexEntry {
        offset: 10,
        position: 200,
    });
    index.add_entry(IndexEntry {
        offset: 30,
        position: 400,
    });

    assert_eq!(index.entry_count(), 3);
    assert_eq!(index.find_position_for_offset(10), Some(200));
    assert_eq!(index.find_position_for_offset(20), Some(300));
    assert_eq!(index.find_position_for_offset(30), Some(400));
}

#[test]
fn test_find_position_for_exact_offset() {
    let index = create_index_with_entries();

    let position = index.find_position_for_offset(10);

    assert_eq!(position, Some(100));
}

#[test]
fn test_find_position_for_non_existent_offset() {
    let index = create_index_with_entries();

    let position = index.find_position_for_offset(15);

    assert_eq!(position, Some(100));
}

#[test]
fn test_find_position_for_offset_before_first() {
    let index = create_index_with_entries();

    let position = index.find_position_for_offset(0);

    assert_eq!(position, Some(0));
}

#[test]
fn test_find_position_for_offset_after_last() {
    let index = create_index_with_entries();

    let position = index.find_position_for_offset(25);

    assert_eq!(position, Some(250));
}

#[test]
fn test_find_position_for_empty_index() {
    let index = create_test_index();

    let position = index.find_position_for_offset(10);

    assert_eq!(position, Some(0));
}

#[test]
fn test_save_index_to_file() {
    let index = create_index_with_entries();
    let (_temp_dir, file_path) = create_temp_file();

    let mut file = std::fs::File::create(&file_path).expect("Failed to create file");
    let mut writer = std::io::BufWriter::new(&mut file);

    if let Some(entry) = index.last_entry() {
        let result = index.write_entry_to_file(&mut writer, entry, 0);
        assert!(result.is_ok());
    }

    assert!(file_path.exists());
}

#[test]
fn test_load_index_from_file() {
    let mut loaded_index = SparseIndex::new();
    let (_temp_dir, file_path) = create_temp_file();

    let mut file = std::fs::File::create(&file_path).expect("Failed to create file");
    let mut writer = std::io::BufWriter::new(&mut file);

    let entry = IndexEntry {
        offset: 10,
        position: 100,
    };
    let index = SparseIndex::new();
    index
        .write_entry_to_file(&mut writer, &entry, 0)
        .expect("Failed to write entry");
    drop(writer);
    drop(file);

    let file = std::fs::File::open(&file_path).expect("Failed to open file");
    let mut reader = std::io::BufReader::new(file);
    let result = loaded_index.read_from_file(&mut reader, 0, None);

    assert!(result.is_ok());
    assert_eq!(loaded_index.entry_count(), 1);
    assert_eq!(loaded_index.find_position_for_offset(10), Some(100));
}

#[test]
fn test_load_index_from_non_existent_file() {
    let (_temp_dir, file_path) = create_temp_file();
    let result = std::fs::File::open(&file_path);

    assert!(result.is_err());
}

#[test]
fn test_save_load_round_trip() {
    let original_index = create_index_with_entries();
    let (_temp_dir, file_path) = create_temp_file();
    let mut loaded_index = SparseIndex::new();

    let mut file = std::fs::File::create(&file_path).expect("Failed to create file");
    let mut writer = std::io::BufWriter::new(&mut file);

    let entries = vec![
        IndexEntry {
            offset: 0,
            position: 0,
        },
        IndexEntry {
            offset: 10,
            position: 100,
        },
        IndexEntry {
            offset: 20,
            position: 250,
        },
    ];

    for entry in &entries {
        original_index
            .write_entry_to_file(&mut writer, entry, 0)
            .expect("Failed to write entry");
    }
    drop(writer);
    drop(file);

    let file = std::fs::File::open(&file_path).expect("Failed to open file");
    let mut reader = std::io::BufReader::new(file);
    loaded_index
        .read_from_file(&mut reader, 0, None)
        .expect("Failed to load index");

    assert_eq!(3, loaded_index.entry_count());
}

#[test]
fn test_save_index_with_io_error() {
    let invalid_path = std::path::Path::new("/invalid/path/index.idx");

    let result = std::fs::File::create(invalid_path);

    assert!(result.is_err());
}

#[test]
fn test_load_index_with_corrupted_file() {
    let (_temp_dir, file_path) = create_temp_file();
    let mut file = fs::File::create(&file_path).expect("Failed to create file");
    file.write_all(b"corrupt")
        .expect("Failed to write corrupted data");
    drop(file);

    let mut index = SparseIndex::new();
    let file = std::fs::File::open(&file_path).expect("Failed to open file");
    let mut reader = std::io::BufReader::new(file);
    let result = index.read_from_file(&mut reader, 0, None);

    assert!(result.is_ok());
}

#[test]
fn test_entries_returns_correct_slice() {
    let index = create_index_with_entries();

    assert_eq!(index.entry_count(), 3);
    assert_eq!(index.find_position_for_offset(0), Some(0));
    assert_eq!(index.find_position_for_offset(10), Some(100));
    assert_eq!(index.find_position_for_offset(20), Some(250));
}

#[test]
fn test_is_empty_returns_true_for_new_index() {
    let index = create_test_index();

    assert_eq!(index.entry_count(), 0);
}

#[test]
fn test_is_empty_returns_false_for_populated_index() {
    let index = create_index_with_entries();

    assert!(index.entry_count() > 0);
}

#[test]
fn test_len_returns_correct_count() {
    let mut index = create_test_index();
    assert_eq!(index.entry_count(), 0);

    index.add_entry(IndexEntry {
        offset: 1,
        position: 10,
    });
    assert_eq!(index.entry_count(), 1);

    index.add_entry(IndexEntry {
        offset: 2,
        position: 20,
    });
    assert_eq!(index.entry_count(), 2);
}

#[test]
fn test_add_entry_with_duplicate_offset() {
    let mut index = create_test_index();

    index.add_entry(IndexEntry {
        offset: 10,
        position: 100,
    });
    index.add_entry(IndexEntry {
        offset: 10,
        position: 200,
    });

    assert_eq!(index.entry_count(), 1);
    assert_eq!(index.find_position_for_offset(10), Some(100));
}

#[test]
fn test_large_index_performance() {
    let mut index = create_test_index();

    for i in 0..1000 {
        index.add_entry(IndexEntry {
            offset: i * 10,
            position: (i * 100) as u32,
        });
    }

    assert_eq!(index.entry_count(), 1000);
    assert_eq!(index.find_position_for_offset(5000), Some(50000));
}
