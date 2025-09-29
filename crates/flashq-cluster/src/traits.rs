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
/// Defines the cluster integration interface for FlashQ brokers.
///
/// Any broker implementation (gRPC, HTTP, etc.) must implement this trait
/// to provide runtime state (not cluster metadata) to the cluster management layer.
/// Cluster metadata queries (partition assignments, leadership) are handled directly
/// by the MetadataStore.
#[async_trait]
pub trait ClusterBroker: Send + Sync {
    /// Fetch the current high water mark for a partition from the broker's local log.
    async fn get_high_water_mark(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<u64, ClusterError>;

    /// Get the log start offset for a partition from the broker's local log.
    async fn get_log_start_offset(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<u64, ClusterError>;

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

/// Defines the cluster coordination service interface for broker communication.
///
/// This trait exposes the core operations needed for inter-broker coordination,
/// such as cluster discovery, heartbeat management, and partition status reporting.
/// Implementations use ClusterBroker methods and persistent state management
/// to orchestrate distributed broker behavior.
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
