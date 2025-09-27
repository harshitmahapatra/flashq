//! Metadata store implementations and backends.

pub mod backend;
pub mod file;
pub mod memory;
pub mod r#trait;

// Re-exports for ergonomics
pub use backend::MetadataBackend;
pub use file::FileMetadataStore;
pub use memory::InMemoryMetadataStore;
pub use r#trait::MetadataStore;
