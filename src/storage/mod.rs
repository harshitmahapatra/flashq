pub mod backend;
pub mod file;
pub mod memory;
pub mod r#trait;

pub use backend::StorageBackend;
pub use memory::{InMemoryConsumerGroup, InMemoryTopicLog};
pub use r#trait::{ConsumerGroup, TopicLog};

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

pub const DEFAULT_BATCH_BYTES: usize = 128 * 1024;
