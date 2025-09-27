//! Traits for cluster service integration.
//!
//! This module defines the core traits that connect the metadata store to gRPC services
//! and broker implementations using the generated proto types.

use crate::{
    ClusterError,
    metadata_store::MetadataStore,
    proto::{
        DescribeClusterResponse, HeartbeatRequest, HeartbeatResponse, ReportPartitionStatusRequest,
        ReportPartitionStatusResponse,
    },
    types::*,
};
use async_trait::async_trait;

/// Trait that a FlashQ broker must implement to integrate with cluster services.
///
/// This trait captures the essential operations that the cluster management layer
/// needs to coordinate with individual broker instances.
#[async_trait]
pub trait FlashQBroker: Send + Sync {
    /// Fetch the current high water mark for a partition.
    async fn get_high_water_mark(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<u64, ClusterError>;

    /// Get the log start offset for a partition.
    async fn get_log_start_offset(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<u64, ClusterError>;

    /// Check if this broker is the leader for a given partition.
    async fn is_partition_leader(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<bool, ClusterError>;

    /// Get the list of partitions this broker is responsible for.
    async fn get_assigned_partitions(&self) -> Result<Vec<(String, PartitionId)>, ClusterError>;

    /// Acknowledge that a record batch has been successfully replicated.
    async fn acknowledge_replication(
        &self,
        topic: &str,
        partition: PartitionId,
        offset: u64,
    ) -> Result<(), ClusterError>;

    /// Initiate graceful shutdown of the broker.
    async fn initiate_shutdown(&self) -> Result<(), ClusterError>;
}

/// Trait that maps gRPC cluster service operations to metadata store operations.
///
/// This trait provides the business logic layer between the gRPC service endpoints
/// and the underlying metadata store, handling the conversion between proto messages
/// and internal data structures.
#[async_trait]
pub trait ClusterService: Send + Sync {
    /// Get a snapshot of the current cluster state.
    ///
    /// This maps to the DescribeCluster gRPC call and returns information about
    /// all brokers, topics, and partition assignments.
    async fn describe_cluster(&self) -> Result<DescribeClusterResponse, ClusterError>;

    /// Handle a heartbeat from a broker and return any epoch updates.
    ///
    /// This processes broker liveness information and partition status updates,
    /// returning any epoch changes that the broker needs to be aware of.
    async fn handle_heartbeat(
        &self,
        request: HeartbeatRequest,
    ) -> Result<HeartbeatResponse, ClusterError>;

    /// Process a partition status report from a broker.
    ///
    /// This allows brokers to report changes in partition state, such as
    /// leadership changes or ISR updates.
    async fn report_partition_status(
        &self,
        request: ReportPartitionStatusRequest,
    ) -> Result<ReportPartitionStatusResponse, ClusterError>;

    /// Get the underlying metadata store.
    ///
    /// This provides access to the raw metadata operations for advanced use cases
    /// or integration with other components.
    fn metadata_store(&self) -> &dyn MetadataStore;
}
