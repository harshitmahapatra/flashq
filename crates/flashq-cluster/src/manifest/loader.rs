//! Cluster manifest loading and file I/O operations.

use crate::ClusterError;
use std::path::Path;
use super::types::ClusterManifest;

pub struct ManifestLoader;

impl ManifestLoader {
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

    /// Placeholder for future watch functionality.
    pub fn reload<P: AsRef<Path>>(path: P) -> Result<ClusterManifest, ClusterError> {
        Self::from_path(path)
    }
}
