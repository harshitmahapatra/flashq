//! Metadata store backend implementations and configuration.

use crate::{
    ClusterError,
    manifest::types::ClusterManifest,
    metadata_store::{memory::InMemoryMetadataStore, r#trait::MetadataStore},
};
use std::sync::{Arc, RwLock};

/// Backend storage configuration for cluster metadata.
#[derive(Debug)]
pub enum MetadataBackend {
    /// In-memory storage backend.
    ///
    /// Fast but ephemeral - all metadata is lost on restart.
    /// Suitable for development, testing, and single-node deployments.
    Memory,
}

impl MetadataBackend {
    /// Create a new in-memory metadata backend.
    pub fn new_memory() -> Self {
        MetadataBackend::Memory
    }

    /// Create a metadata store instance from this backend configuration.
    pub fn create(&self) -> Result<Arc<RwLock<dyn MetadataStore + Send + Sync>>, ClusterError> {
        match self {
            MetadataBackend::Memory => Ok(Arc::new(RwLock::new(InMemoryMetadataStore::new()))),
        }
    }

    /// Create a metadata store instance initialized with the given manifest.
    pub fn create_with_manifest(
        &self,
        manifest: ClusterManifest,
    ) -> Result<Arc<RwLock<dyn MetadataStore + Send + Sync>>, ClusterError> {
        match self {
            MetadataBackend::Memory => {
                let store = InMemoryMetadataStore::new_with_manifest(manifest)?;
                Ok(Arc::new(RwLock::new(store)))
            }
        }
    }
}

impl Default for MetadataBackend {
    fn default() -> Self {
        Self::new_memory()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        manifest::types::{BrokerSpec, PartitionAssignment, TopicAssignment},
        types::*,
    };
    use std::collections::HashMap;

    fn create_test_manifest() -> ClusterManifest {
        let brokers = vec![
            BrokerSpec {
                id: BrokerId(1),
                host: "127.0.0.1".to_string(),
                port: 6001,
            },
            BrokerSpec {
                id: BrokerId(2),
                host: "127.0.0.1".to_string(),
                port: 6002,
            },
        ];

        let mut topics = HashMap::new();
        topics.insert(
            "test-topic".to_string(),
            TopicAssignment {
                replication_factor: 2,
                partitions: vec![PartitionAssignment {
                    id: PartitionId::new(0),
                    leader: BrokerId(1),
                    replicas: vec![BrokerId(1), BrokerId(2)],
                    in_sync_replicas: vec![BrokerId(1), BrokerId(2)],
                    epoch: Epoch(1),
                }],
            },
        );

        ClusterManifest { brokers, topics }
    }

    #[test]
    fn test_memory_backend_creation() {
        let backend = MetadataBackend::new_memory();
        let store = backend.create().unwrap();

        // Should be able to access the store
        let store_guard = store.read().unwrap();
        let manifest = store_guard.export_to_manifest().unwrap();
        assert!(manifest.brokers.is_empty());
        assert!(manifest.topics.is_empty());
    }

    #[test]
    fn test_memory_backend_with_manifest() {
        let manifest = create_test_manifest();
        let backend = MetadataBackend::new_memory();
        let store = backend.create_with_manifest(manifest.clone()).unwrap();

        let store_guard = store.read().unwrap();
        let exported_manifest = store_guard.export_to_manifest().unwrap();

        assert_eq!(exported_manifest.brokers.len(), 2);
        assert_eq!(exported_manifest.topics.len(), 1);
        assert!(exported_manifest.topics.contains_key("test-topic"));
    }

    #[test]
    fn test_default_backend() {
        let backend = MetadataBackend::default();
        matches!(backend, MetadataBackend::Memory);
    }
}
