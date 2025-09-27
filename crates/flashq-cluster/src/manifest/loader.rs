//! Cluster manifest loading and file I/O operations.

use crate::ClusterError;
use std::path::Path;
use super::types::ClusterManifest;

/// Manifest loader with file I/O operations.
pub struct ManifestLoader;

impl ManifestLoader {
    /// Load manifest from file path.
    /// Supports both JSON (.json) and YAML (.yaml/.yml) formats based on file extension.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<ClusterManifest, ClusterError> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)
            .map_err(|e| ClusterError::from_io_error(e, "manifest loading"))?;

        let extension = path
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("");

        match extension.to_lowercase().as_str() {
            "json" => serde_json::from_str(&content)
                .map_err(|e| ClusterError::from_parse_error(e, "JSON manifest parsing")),
            "yaml" | "yml" => serde_yaml::from_str(&content)
                .map_err(|e| ClusterError::from_parse_error(e, "YAML manifest parsing")),
            _ => {
                // Try JSON first, then YAML as fallback
                serde_json::from_str(&content)
                    .or_else(|_| serde_yaml::from_str(&content))
                    .map_err(|e| ClusterError::from_parse_error(e, "manifest parsing (tried both JSON and YAML)"))
            }
        }
    }

    /// Reload manifest from the same path (placeholder for future watch functionality).
    pub fn reload<P: AsRef<Path>>(path: P) -> Result<ClusterManifest, ClusterError> {
        Self::from_path(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    fn create_test_manifest() -> ClusterManifest {
        use super::super::types::*;
        ClusterManifest {
            brokers: vec![
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
            ],
            topics: [(
                "orders".to_string(),
                TopicAssignment {
                    replication_factor: 3,
                    partitions: vec![PartitionAssignment {
                        id: PartitionId::new(0),
                        leader: BrokerId(1),
                        replicas: vec![BrokerId(1), BrokerId(2)],
                        in_sync_replicas: vec![BrokerId(1), BrokerId(2)],
                        epoch: Epoch(4),
                    }],
                },
            )]
            .into_iter()
            .collect(),
        }
    }

    #[test]
    fn test_manifest_loading() {
        let manifest = create_test_manifest();
        let json = serde_json::to_string_pretty(&manifest).unwrap();

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();

        let loaded = ManifestLoader::from_path(temp_file.path()).unwrap();
        assert_eq!(manifest, loaded);
    }
}