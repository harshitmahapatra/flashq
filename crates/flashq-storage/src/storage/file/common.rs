use std::{
    io::{BufReader, Read, Seek, SeekFrom},
    path::Path,
};

use crate::{Record, RecordWithOffset, error::StorageError};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SyncMode {
    None,
    Immediate,
    Periodic,
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

pub fn deserialize_record<R: Read>(
    reader: &mut BufReader<R>,
) -> Result<RecordWithOffset, StorageError> {
    let payload_size = read_u32(reader, "Failed to read payload size")?;
    let offset = read_u64(reader, "Failed to read offset")?;
    // New header field: timestamp_ms (u64)
    let _timestamp_ms = read_u64(reader, "Failed to read timestamp_ms")?;

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
    // Compute timestamp_ms from timestamp string for header
    let ts_str = String::from_utf8(timestamp_bytes.clone()).map_err(|e| {
        StorageError::from_serialization_error(e, "Failed to parse timestamp bytes")
    })?;
    let ts_ms_i64 = chrono::DateTime::parse_from_rfc3339(&ts_str)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(0);
    let timestamp_ms = if ts_ms_i64 < 0 { 0 } else { ts_ms_i64 as u64 };

    Ok(assemble_record_buffer(
        payload_size,
        offset,
        timestamp_ms,
        timestamp_len,
        ts_str.as_bytes(),
        &json_payload,
    ))
}

/// Zero-copy style: append the serialized record directly into `buf` and return the number of bytes written.
/// Writes a placeholder for payload size and backfills it after writing JSON.
pub fn serialize_record_into_buffer(
    buf: &mut Vec<u8>,
    record: &Record,
    offset: u64,
    timestamp: &str,
) -> Result<u32, StorageError> {
    let ts_bytes = timestamp.as_bytes();
    let ts_len_u32 = ts_bytes.len() as u32;

    let start = buf.len();
    // Reserve header + timestamp upfront
    buf.extend_from_slice(&0u32.to_be_bytes()); // payload size placeholder
    buf.extend_from_slice(&offset.to_be_bytes());
    // New: write timestamp_ms into header
    let ts_ms_i64 = chrono::DateTime::parse_from_rfc3339(timestamp)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(0);
    let ts_ms = if ts_ms_i64 < 0 { 0 } else { ts_ms_i64 as u64 };
    buf.extend_from_slice(&ts_ms.to_be_bytes());
    buf.extend_from_slice(&ts_len_u32.to_be_bytes());
    buf.extend_from_slice(ts_bytes);

    let json_start = buf.len();
    serde_json::to_writer(&mut *buf, record).map_err(|e| {
        StorageError::from_serialization_error(e, "Failed to serialize record to JSON")
    })?;
    let json_len = (buf.len() - json_start) as u32;
    let payload_size = json_len + 4 + ts_len_u32;
    // Backfill payload size
    buf[start..start + 4].copy_from_slice(&payload_size.to_be_bytes());
    Ok((buf.len() - start) as u32)
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
    timestamp_ms: u64,
    timestamp_len: u32,
    timestamp_bytes: &[u8],
    json_payload: &[u8],
) -> Vec<u8> {
    // [4B payload][8B offset][8B timestamp_ms][4B ts_len][ts][json]
    let mut buffer = Vec::with_capacity(4 + 8 + 8 + 4 + timestamp_bytes.len() + json_payload.len());
    buffer.extend_from_slice(&payload_size.to_be_bytes());
    buffer.extend_from_slice(&offset.to_be_bytes());
    buffer.extend_from_slice(&timestamp_ms.to_be_bytes());
    buffer.extend_from_slice(&timestamp_len.to_be_bytes());
    buffer.extend_from_slice(timestamp_bytes);
    buffer.extend_from_slice(json_payload);
    buffer
}

// Fast header peek + skip helpers for time-based scanning
pub fn read_record_header<R: Read + Seek>(
    reader: &mut BufReader<R>,
) -> Result<(u32, u64, u64, u32, u64), StorageError> {
    let record_start = reader
        .stream_position()
        .map_err(|e| StorageError::from_io_error(e, "Failed to get record start pos"))?;
    let payload_size = read_u32(reader, "Failed to read payload size")?;
    let offset = read_u64(reader, "Failed to read offset")?;
    let ts_ms = read_u64(reader, "Failed to read timestamp_ms")?;
    let ts_len = read_u32(reader, "Failed to read timestamp length")?;
    Ok((payload_size, offset, ts_ms, ts_len, record_start))
}

pub fn skip_record_after_header<R: Read + Seek>(
    reader: &mut BufReader<R>,
    payload_size: u32,
) -> Result<(), StorageError> {
    // After peek, we are positioned just after ts_len; remaining = ts bytes + json
    let to_skip = (payload_size - 4) as i64;
    reader
        .seek(SeekFrom::Current(to_skip))
        .map_err(|e| StorageError::from_io_error(e, "Failed to skip record"))?;
    Ok(())
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
        let timestamp_ms = 1_700_000_000_000u64;
        let timestamp_len = 20u32;
        let timestamp_bytes = vec![0; timestamp_len as usize];
        let json_payload = vec![1; 76];

        let buffer = assemble_record_buffer(
            payload_size,
            offset,
            timestamp_ms,
            timestamp_len,
            &timestamp_bytes,
            &json_payload,
        );

        let mut expected_buffer = Vec::new();
        expected_buffer.extend_from_slice(&payload_size.to_be_bytes());
        expected_buffer.extend_from_slice(&offset.to_be_bytes());
        expected_buffer.extend_from_slice(&timestamp_ms.to_be_bytes());
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
