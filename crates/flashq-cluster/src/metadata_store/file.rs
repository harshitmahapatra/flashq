//! File-based metadata store implementation.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use log::{debug, info};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::{
    ClusterError,
    manifest::types::{ClusterManifest, PartitionAssignment},
    metadata_store::r#trait::MetadataStore,
    types::*,
};

/// Serializable cluster metadata combining manifest and runtime state.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClusterMetadataData {
    /// Static cluster configuration
    manifest: ClusterManifest,
    /// Broker runtime status indexed by broker ID
    broker_runtime: HashMap<u32, BrokerRuntimeStatus>, // Using u32 for JSON serialization
    /// Partition runtime state indexed by topic and partition ID
    partition_runtime: HashMap<String, HashMap<u32, PartitionRuntimeState>>, // Flattened for JSON
}

impl Default for ClusterMetadataData {
    fn default() -> Self {
        Self {
            manifest: ClusterManifest {
                brokers: Vec::new(),
                topics: HashMap::new(),
            },
            broker_runtime: HashMap::new(),
            partition_runtime: HashMap::new(),
        }
    }
}

/// File-based implementation of the MetadataStore trait.
///
/// This implementation stores cluster metadata in a JSON file within the broker's
/// data directory. It follows the same pattern as consumer groups, avoiding
/// directory locking conflicts by using the existing broker-owned data directory.
#[derive(Debug)]
pub struct FileMetadataStore {
    /// Internal cluster state protected by RwLock for concurrent access
    state: Arc<RwLock<ClusterMetadataData>>,
    /// Path to the metadata file
    file_path: PathBuf,
}

impl FileMetadataStore {
    /// Create a new file-based metadata store.
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self, ClusterError> {
        let file_path = Self::setup_metadata_file(data_dir)?;
        let state = Self::load_existing_metadata(&file_path)?;

        info!("Loaded cluster metadata from: {}", file_path.display());

        let store = Self {
            state: Arc::new(RwLock::new(state)),
            file_path,
        };

        // Ensure the file exists on disk
        store.persist_to_disk()?;
        Ok(store)
    }

    /// Create a new file-based metadata store initialized with the given manifest.
    pub fn new_with_manifest<P: AsRef<Path>>(
        data_dir: P,
        manifest: ClusterManifest,
    ) -> Result<Self, ClusterError> {
        let store = Self::new(data_dir)?;
        store.load_from_manifest(manifest)?;
        Ok(store)
    }

    /// Setup the metadata file path and ensure the directory exists.
    fn setup_metadata_file<P: AsRef<Path>>(data_dir: P) -> Result<PathBuf, ClusterError> {
        let data_dir = data_dir.as_ref().to_path_buf();

        debug!(
            "Setting up cluster metadata file in: {}",
            data_dir.display()
        );

        // Ensure data directory exists (same pattern as consumer groups)
        if !data_dir.exists() {
            std::fs::create_dir_all(&data_dir)
                .map_err(|e| ClusterError::from_io_error(e, "setup metadata directory"))?;
        }

        let file_path = data_dir.join("cluster_metadata.json");
        debug!("Cluster metadata file path: {}", file_path.display());

        Ok(file_path)
    }

    /// Load existing metadata from file, or return default if file doesn't exist.
    fn load_existing_metadata(file_path: &Path) -> Result<ClusterMetadataData, ClusterError> {
        if !file_path.exists() {
            debug!("Metadata file doesn't exist, starting with empty state");
            return Ok(ClusterMetadataData::default());
        }

        debug!("Loading existing metadata from: {}", file_path.display());

        let content = std::fs::read_to_string(file_path)
            .map_err(|e| ClusterError::from_io_error(e, "load metadata file"))?;

        if content.trim().is_empty() {
            debug!("Metadata file is empty, starting with default state");
            return Ok(ClusterMetadataData::default());
        }

        let data: ClusterMetadataData = serde_json::from_str(&content)
            .map_err(|e| ClusterError::from_parse_error(e, "parse metadata file"))?;

        info!(
            "Loaded {} brokers, {} topics from metadata file",
            data.manifest.brokers.len(),
            data.manifest.topics.len()
        );

        Ok(data)
    }

    /// Persist current state to disk.
    fn persist_to_disk(&self) -> Result<(), ClusterError> {
        let state = self.state.read();
        let content = serde_json::to_string_pretty(&*state)
            .map_err(|e| ClusterError::from_parse_error(e, "serialize metadata"))?;

        std::fs::write(&self.file_path, content)
            .map_err(|e| ClusterError::from_io_error(e, "write metadata file"))?;

        debug!(
            "Persisted cluster metadata to: {}",
            self.file_path.display()
        );
        Ok(())
    }

    /// Helper to get partition from the manifest data.
    fn get_partition<'a>(
        state: &'a ClusterMetadataData,
        topic: &str,
        partition_id: PartitionId,
    ) -> Result<&'a PartitionAssignment, ClusterError> {
        let topic_assignment =
            state
                .manifest
                .topics
                .get(topic)
                .ok_or(ClusterError::TopicNotFound {
                    topic: topic.to_string(),
                })?;

        topic_assignment
            .partitions
            .iter()
            .find(|p| p.id == partition_id)
            .ok_or(ClusterError::PartitionNotFound {
                topic: topic.to_string(),
                partition_id: partition_id.into(),
            })
    }

    /// Helper to get mutable partition from the manifest data.
    fn get_partition_mut<'a>(
        state: &'a mut ClusterMetadataData,
        topic: &str,
        partition_id: PartitionId,
    ) -> Result<&'a mut PartitionAssignment, ClusterError> {
        let topic_assignment =
            state
                .manifest
                .topics
                .get_mut(topic)
                .ok_or(ClusterError::TopicNotFound {
                    topic: topic.to_string(),
                })?;

        topic_assignment
            .partitions
            .iter_mut()
            .find(|p| p.id == partition_id)
            .ok_or(ClusterError::PartitionNotFound {
                topic: topic.to_string(),
                partition_id: partition_id.into(),
            })
    }
}

impl MetadataStore for FileMetadataStore {
    fn get_partition_leader(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<BrokerId, ClusterError> {
        let state = self.state.read();
        let partition = Self::get_partition(&state, topic, partition)?;
        Ok(partition.leader)
    }

    fn get_in_sync_replicas(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<HashSet<BrokerId>, ClusterError> {
        let state = self.state.read();
        let partition = Self::get_partition(&state, topic, partition)?;
        Ok(partition.in_sync_replicas.iter().copied().collect())
    }

    fn bump_leader_epoch(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<Epoch, ClusterError> {
        let mut state = self.state.write();
        let partition = Self::get_partition_mut(&mut state, topic, partition)?;

        // Increment epoch atomically
        let new_epoch = Epoch(partition.epoch.0 + 1);
        partition.epoch = new_epoch;

        drop(state);
        self.persist_to_disk()?;

        Ok(new_epoch)
    }

    fn update_in_sync_replica(
        &self,
        topic: &str,
        partition_id: PartitionId,
        replica: BrokerId,
        in_sync: bool,
    ) -> Result<(), ClusterError> {
        let mut state = self.state.write();
        let partition = Self::get_partition_mut(&mut state, topic, partition_id)?;

        // Verify the replica is actually part of the replica set
        if !partition.replicas.contains(&replica) {
            return Err(ClusterError::InvalidReplica {
                topic: topic.to_string(),
                partition_id: partition_id.into(),
                replica_id: replica.into(),
            });
        }

        if in_sync {
            // Add to in-sync replicas if not already present
            if !partition.in_sync_replicas.contains(&replica) {
                partition.in_sync_replicas.push(replica);
            }
        } else {
            // Remove from in-sync replicas
            partition.in_sync_replicas.retain(|&r| r != replica);
        }

        drop(state);
        self.persist_to_disk()?;
        Ok(())
    }

    fn get_partition_epoch(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<Epoch, ClusterError> {
        let state = self.state.read();
        let partition = Self::get_partition(&state, topic, partition)?;
        Ok(partition.epoch)
    }

    fn get_all_replicas(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<Vec<BrokerId>, ClusterError> {
        let state = self.state.read();
        let partition = Self::get_partition(&state, topic, partition)?;
        Ok(partition.replicas.clone())
    }

    fn load_from_manifest(&self, manifest: ClusterManifest) -> Result<(), ClusterError> {
        let mut state = self.state.write();

        // Clear existing state including runtime state
        state.manifest = manifest;
        state.broker_runtime.clear();
        state.partition_runtime.clear();

        drop(state);
        self.persist_to_disk()?;
        Ok(())
    }

    fn export_to_manifest(&self) -> Result<ClusterManifest, ClusterError> {
        let state = self.state.read();
        Ok(state.manifest.clone())
    }

    // ===========================
    // Broker Runtime Tracking
    // ===========================

    fn record_broker_heartbeat(
        &self,
        broker: BrokerId,
        ts: DateTime<Utc>,
        draining: bool,
    ) -> Result<(), ClusterError> {
        let mut state = self.state.write();

        // Verify broker exists in the cluster
        if !state.manifest.brokers.iter().any(|b| b.id == broker) {
            return Err(ClusterError::UnknownBroker {
                broker_id: broker.into(),
            });
        }

        // Update or insert broker runtime status
        state.broker_runtime.insert(
            broker.0,
            BrokerRuntimeStatus {
                last_heartbeat: ts,
                is_draining: draining,
            },
        );

        drop(state);
        self.persist_to_disk()?;
        Ok(())
    }

    fn list_brokers_with_status(
        &self,
    ) -> Result<Vec<(BrokerId, BrokerRuntimeStatus)>, ClusterError> {
        let state = self.state.read();
        let heartbeat_timeout = chrono::Duration::seconds(10); // 10 second timeout for testing

        let mut result = Vec::new();
        for broker_spec in &state.manifest.brokers {
            let broker_id = broker_spec.id;
            let status = if let Some(runtime_status) = state.broker_runtime.get(&broker_id.0) {
                // Check if broker is alive based on heartbeat timeout
                let _is_alive = Utc::now().signed_duration_since(runtime_status.last_heartbeat)
                    < heartbeat_timeout;
                BrokerRuntimeStatus {
                    last_heartbeat: runtime_status.last_heartbeat,
                    is_draining: runtime_status.is_draining,
                }
            } else {
                // No heartbeat recorded yet - consider dead
                BrokerRuntimeStatus {
                    last_heartbeat: DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now),
                    is_draining: false,
                }
            };
            result.push((broker_id, status));
        }

        Ok(result)
    }

    // ===========================
    // Enhanced Partition Operations
    // ===========================

    fn set_partition_leader(
        &self,
        topic: &str,
        partition: PartitionId,
        leader: BrokerId,
        epoch: Epoch,
    ) -> Result<(), ClusterError> {
        let mut state = self.state.write();
        let partition_assignment = Self::get_partition_mut(&mut state, topic, partition)?;

        // Verify the new leader is in the replica set
        if !partition_assignment.replicas.contains(&leader) {
            return Err(ClusterError::InvalidReplica {
                topic: topic.to_string(),
                partition_id: partition.into(),
                replica_id: leader.into(),
            });
        }

        // Verify epoch is not going backwards
        if epoch < partition_assignment.epoch {
            return Err(ClusterError::InvalidEpoch {
                topic: topic.to_string(),
                partition_id: partition.into(),
                current_epoch: partition_assignment.epoch.into(),
                new_epoch: epoch.into(),
            });
        }

        partition_assignment.leader = leader;
        partition_assignment.epoch = epoch;

        drop(state);
        self.persist_to_disk()?;
        Ok(())
    }

    fn compare_and_set_epoch(
        &self,
        topic: &str,
        partition: PartitionId,
        expected: Epoch,
        new: Epoch,
    ) -> Result<bool, ClusterError> {
        let mut state = self.state.write();
        let partition_assignment = Self::get_partition_mut(&mut state, topic, partition)?;

        if partition_assignment.epoch != expected {
            return Ok(false); // CAS failed - epoch didn't match
        }

        // Verify new epoch is greater than current
        if new <= expected {
            return Err(ClusterError::InvalidEpoch {
                topic: topic.to_string(),
                partition_id: partition.into(),
                current_epoch: expected.into(),
                new_epoch: new.into(),
            });
        }

        partition_assignment.epoch = new;
        let success = true;

        drop(state);
        if success {
            self.persist_to_disk()?;
        }
        Ok(success)
    }

    fn update_partition_offsets(
        &self,
        topic: &str,
        partition: PartitionId,
        high_water_mark: u64,
        log_start_offset: u64,
    ) -> Result<(), ClusterError> {
        {
            let state = self.state.read();
            // Verify partition exists
            let _ = Self::get_partition(&state, topic, partition)?;
        }

        // Update partition runtime state
        let mut state = self.state.write();
        state
            .partition_runtime
            .entry(topic.to_string())
            .or_default()
            .insert(
                partition.0,
                PartitionRuntimeState {
                    high_water_mark,
                    log_start_offset,
                },
            );

        drop(state);
        self.persist_to_disk()?;
        Ok(())
    }
}
