use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

use crate::{Record, RecordWithOffset, error::StorageError};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SyncMode {
    None,
    Immediate,
    Periodic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FileIoMode {
    IoUring,
    #[default]
    Std,
}

// ================================================================================================
// FILE I/O UTILITIES
// ================================================================================================

pub fn ensure_directory_exists<P: AsRef<Path>>(dir: P) -> Result<(), std::io::Error> {
    let dir = dir.as_ref();
    if !dir.exists() {
        std::fs::create_dir_all(dir)?;
    }
    Ok(())
}

pub fn sync_file_if_needed(file: &File, sync_mode: SyncMode) -> Result<(), std::io::Error> {
    if sync_mode == SyncMode::Immediate {
        file.sync_all()
    } else {
        Ok(())
    }
}

pub fn deserialize_record<R: Read>(
    reader: &mut BufReader<R>,
) -> Result<RecordWithOffset, StorageError> {
    let payload_size = read_u32(reader, "Failed to read payload size")?;
    let offset = read_u64(reader, "Failed to read offset")?;

    let (timestamp, timestamp_len) = read_timestamp(reader)?;

    let record = read_json_payload(reader, payload_size, timestamp_len)?;

    Ok(RecordWithOffset {
        record,
        offset,
        timestamp,
    })
}

fn read_bytes<const N: usize>(
    reader: &mut impl Read,
    error_context: &str,
) -> Result<[u8; N], StorageError> {
    let mut bytes = [0u8; N];
    reader
        .read_exact(&mut bytes)
        .map_err(|e| StorageError::from_io_error(e, error_context))?;
    Ok(bytes)
}

fn read_u32(reader: &mut impl Read, error_context: &str) -> Result<u32, StorageError> {
    read_bytes::<4>(reader, error_context).map(u32::from_be_bytes)
}

fn read_u64(reader: &mut impl Read, error_context: &str) -> Result<u64, StorageError> {
    read_bytes::<8>(reader, error_context).map(u64::from_be_bytes)
}

fn read_timestamp(reader: &mut impl Read) -> Result<(String, u32), StorageError> {
    let timestamp_len = read_u32(reader, "Failed to read timestamp length")?;
    let mut timestamp_bytes = vec![0u8; timestamp_len as usize];
    reader
        .read_exact(&mut timestamp_bytes)
        .map_err(|e| StorageError::from_io_error(e, "Failed to read timestamp"))?;
    let timestamp = String::from_utf8(timestamp_bytes).map_err(|e| {
        StorageError::from_serialization_error(e, "Failed to parse timestamp as UTF-8")
    })?;
    Ok((timestamp, timestamp_len))
}

fn read_json_payload(
    reader: &mut impl Read,
    payload_size: u32,
    timestamp_len: u32,
) -> Result<Record, StorageError> {
    let json_len = payload_size - 4 - timestamp_len; // subtract timestamp_len field + timestamp
    let mut json_bytes = vec![0u8; json_len as usize];
    reader
        .read_exact(&mut json_bytes)
        .map_err(|e| StorageError::from_io_error(e, "Failed to read JSON payload"))?;
    serde_json::from_slice(&json_bytes).map_err(|e| {
        StorageError::from_serialization_error(e, "Failed to deserialize record from JSON")
    })
}

pub fn serialize_record(record: &Record, offset: u64) -> Result<Vec<u8>, StorageError> {
    let json_payload = serialize_record_payload(record)?;
    let timestamp_bytes = create_timestamp_bytes();
    let timestamp_len = timestamp_bytes.len() as u32;
    let payload_size = json_payload.len() as u32 + 4 + timestamp_len;

    Ok(assemble_record_buffer(
        payload_size,
        offset,
        timestamp_len,
        &timestamp_bytes,
        &json_payload,
    ))
}

fn serialize_record_payload(record: &Record) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(record).map_err(|e| {
        StorageError::from_serialization_error(e, "Failed to serialize record to JSON")
    })
}

fn create_timestamp_bytes() -> Vec<u8> {
    chrono::Utc::now().to_rfc3339().into_bytes()
}

fn assemble_record_buffer(
    payload_size: u32,
    offset: u64,
    timestamp_len: u32,
    timestamp_bytes: &[u8],
    json_payload: &[u8],
) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(4 + 8 + 4 + timestamp_bytes.len() + json_payload.len());
    buffer.extend_from_slice(&payload_size.to_be_bytes());
    buffer.extend_from_slice(&offset.to_be_bytes());
    buffer.extend_from_slice(&timestamp_len.to_be_bytes());
    buffer.extend_from_slice(timestamp_bytes);
    buffer.extend_from_slice(json_payload);
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Cursor;

    #[test]
    fn test_assemble_record_buffer_structure() {
        let payload_size = 100u32;
        let offset = 12345u64;
        let timestamp_len = 20u32;
        let timestamp_bytes = vec![0; timestamp_len as usize];
        let json_payload = vec![1; 76];

        let buffer = assemble_record_buffer(
            payload_size,
            offset,
            timestamp_len,
            &timestamp_bytes,
            &json_payload,
        );

        let mut expected_buffer = Vec::new();
        expected_buffer.extend_from_slice(&payload_size.to_be_bytes());
        expected_buffer.extend_from_slice(&offset.to_be_bytes());
        expected_buffer.extend_from_slice(&timestamp_len.to_be_bytes());
        expected_buffer.extend_from_slice(&timestamp_bytes);
        expected_buffer.extend_from_slice(&json_payload);

        assert_eq!(buffer, expected_buffer);
    }

    #[test]
    fn test_serialize_record_payload_valid_json() {
        let record = Record {
            value: json!({ "test": "data" }).to_string(),
            key: None,
            headers: None,
        };

        let serialized = serialize_record_payload(&record).unwrap();

        let deserialized: Record = serde_json::from_slice(&serialized).unwrap();
        assert_eq!(deserialized, record);
    }

    #[test]
    fn test_record_serialization_deserialization_roundtrip() {
        let record = Record {
            value: json!({ "a": 1, "b": "hello" }).to_string(),
            key: Some("my-key".to_string()),
            headers: None,
        };
        let offset = 999;

        let serialized_data = serialize_record(&record, offset).unwrap();
        let mut reader = BufReader::new(Cursor::new(serialized_data));
        let deserialized_record_with_offset = deserialize_record(&mut reader).unwrap();

        assert_eq!(deserialized_record_with_offset.record, record);
        assert_eq!(deserialized_record_with_offset.offset, offset);
        assert!(
            chrono::DateTime::parse_from_rfc3339(&deserialized_record_with_offset.timestamp)
                .is_ok()
        );
    }
}
