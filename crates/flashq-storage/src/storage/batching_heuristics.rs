/// Default storage batching configuration (bytes-based)
#[cfg(unix)]
pub fn default_batch_bytes() -> usize {
    // Derive from OS page size (e.g., 4 KiB * 32 = 128 KiB)
    unsafe {
        let ps = libc::sysconf(libc::_SC_PAGESIZE);
        let ps = if ps <= 0 { 4096 } else { ps as usize };
        let target = ps.saturating_mul(32);
        target.clamp(64 * 1024, 1024 * 1024)
    }
}

#[cfg(not(unix))]
pub fn default_batch_bytes() -> usize {
    128 * 1024
}

/// Estimate the serialized size of a Record, used for batching heuristics
pub fn estimate_record_size(r: &crate::Record) -> usize {
    // Approximate header (offset + size) + timestamp + value length
    let mut len = 16 + 32 + r.value.len();
    if let Some(k) = &r.key {
        len += k.len();
    }
    if let Some(h) = &r.headers {
        for (k, v) in h {
            len += k.len() + v.len();
        }
    }
    // JSON/structural overhead and potential escaping room
    len + 64
}
