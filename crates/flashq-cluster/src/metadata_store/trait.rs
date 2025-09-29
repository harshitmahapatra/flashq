//! Metadata store trait definitions.

use crate::{ClusterError, manifest::types::ClusterManifest, types::*};
use chrono::{DateTime, Utc};
use std::collections::HashSet;

/// Trait for storing and managing cluster metadata.
///
/// This trait provides the core operations needed for Phase 2 cluster metadata management,
/// including partition leadership tracking, epoch management, and in-sync replica coordination.
pub trait MetadataStore: Send + Sync {
    /// Get the current leader for a specific partition.
    fn get_partition_leader(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<BrokerId, ClusterError>;

    /// Get the set of in-sync replicas for a specific partition.
    fn get_in_sync_replicas(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<HashSet<BrokerId>, ClusterError>;

    /// Increment the leader epoch for a partition and return the new epoch value.
    /// This operation is atomic and ensures monotonic epoch progression.
    fn bump_leader_epoch(&self, topic: &str, partition: PartitionId)
    -> Result<Epoch, ClusterError>;

    /// Update the in-sync replica status for a specific broker and partition.
    /// When `in_sync` is true, the broker is added to the in-sync replica set.
    /// When `in_sync` is false, the broker is removed from the in-sync replica set.
    fn update_in_sync_replica(
        &self,
        topic: &str,
        partition: PartitionId,
        replica: BrokerId,
        in_sync: bool,
    ) -> Result<(), ClusterError>;

    /// Get the current epoch for a specific partition.
    fn get_partition_epoch(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<Epoch, ClusterError>;

    /// Get all replicas (both in-sync and out-of-sync) for a specific partition.
    fn get_all_replicas(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<Vec<BrokerId>, ClusterError>;

    /// Load cluster state from a manifest.
    /// This replaces the current state with the manifest data.
    fn load_from_manifest(&self, manifest: ClusterManifest) -> Result<(), ClusterError>;

    /// Export current cluster state as a manifest.
    /// This allows persisting the current state back to storage.
    fn export_to_manifest(&self) -> Result<ClusterManifest, ClusterError>;

    // ===========================
    // Broker Runtime Tracking
    // ===========================

    /// Record a broker heartbeat with timestamp and draining status.
    fn record_broker_heartbeat(
        &self,
        broker: BrokerId,
        ts: DateTime<Utc>,
        draining: bool,
    ) -> Result<(), ClusterError>;

    /// List all brokers with their runtime status information.
    /// Returns broker ID and runtime status pairs.
    fn list_brokers_with_status(
        &self,
    ) -> Result<Vec<(BrokerId, BrokerRuntimeStatus)>, ClusterError>;

    // ===========================
    // Enhanced Partition Operations
    // ===========================

    /// Set the partition leader explicitly.
    /// This is used when leadership changes are reported.
    fn set_partition_leader(
        &self,
        topic: &str,
        partition: PartitionId,
        leader: BrokerId,
        epoch: Epoch,
    ) -> Result<(), ClusterError>;

    /// Compare-and-set operation for partition epoch.
    /// Returns true if the epoch was successfully updated, false if the expected epoch didn't match.
    fn compare_and_set_epoch(
        &self,
        topic: &str,
        partition: PartitionId,
        expected: Epoch,
        new: Epoch,
    ) -> Result<bool, ClusterError>;

    /// Update partition offsets (high water mark and log start offset).
    fn update_partition_offsets(
        &self,
        topic: &str,
        partition: PartitionId,
        high_water_mark: u64,
        log_start_offset: u64,
    ) -> Result<(), ClusterError>;

    /// Get all partitions assigned to a specific broker.
    /// Returns a list of (topic, partition_id) tuples for which this broker is a replica.
    fn get_broker_partitions(&self, broker: BrokerId) -> Result<Vec<(String, PartitionId)>, ClusterError>;
}
