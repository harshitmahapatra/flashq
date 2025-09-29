# FlashQ Cluster

A distributed cluster coordination system for FlashQ message queues, providing metadata management, broker coordination, and partition leadership protocols.

## Overview

The `flashq-cluster` crate implements the cluster coordination layer for FlashQ, enabling:

- **Cluster Metadata Management**: Track brokers, topics, and partition assignments
- **Leader Election**: Coordinate partition leadership across brokers
- **Heartbeat Protocol**: Monitor broker health and propagate cluster state updates
- **Manifest-based Bootstrap**: Initialize cluster state from configuration files

## Architecture

The cluster coordination system consists of several key components:

- **MetadataStore**: Persistent storage for cluster state (in-memory and file-based backends)
- **ClusterServiceImpl**: Implementation of cluster coordination protocol that integrates with brokers
- **ClusterBroker trait**: Interface for broker implementations to provide runtime state (offsets, leadership)
- **ClusterServer**: gRPC server adapter wrapping ClusterServiceImpl for network communication
- **ClusterClient**: gRPC client for broker-to-broker or broker-to-controller communication
- **ManifestLoader**: Bootstrap cluster state from manifest files

## Cluster Coordination Scenarios

### 1. Startup: Manifest-Based Initialization

During broker startup, the cluster state is initialized from a manifest file that defines the desired cluster topology.

```mermaid
sequenceDiagram
    participant Broker as Broker (ClusterBroker impl)
    participant ML as ManifestLoader
    participant MS as MetadataStore
    participant CSI as ClusterServiceImpl
    participant CSrv as ClusterServer

    Note over Broker: Broker Startup Process

    Broker->>ML: load_manifest("cluster.yaml")
    ML->>ML: Parse YAML/JSON manifest
    ML-->>Broker: ClusterManifest { brokers, topics }

    Broker->>MS: initialize_from_manifest(manifest)
    MS->>MS: Create broker entries
    MS->>MS: Create topic/partition assignments
    MS->>MS: Set initial partition leaders and ISRs
    MS-->>Broker: Cluster state initialized

    Broker->>CSI: new(metadata_store, broker_id, flashq_broker)
    CSI-->>Broker: ClusterServiceImpl ready

    Broker->>CSrv: new(cluster_service_impl)
    CSrv-->>Broker: ClusterServer ready for gRPC

    Note over Broker,CSrv: Broker registers ClusterServer with gRPC router
    Note over Broker: Ready to handle cluster coordination
```

**Key Steps:**
1. **Manifest Loading**: Parse YAML/JSON manifest defining brokers and topic assignments
2. **State Initialization**: Populate MetadataStore with broker specs and partition assignments
3. **Service Initialization**: Create ClusterServiceImpl with MetadataStore and ClusterBroker implementation
4. **gRPC Server Setup**: Wrap ClusterServiceImpl in ClusterServer for network communication
5. **Readiness**: Broker is ready to participate in cluster coordination

### 2. Message Production: Partition Leadership Coordination

When a message is added to a topic, brokers coordinate through ClusterServiceImpl to verify leadership and report partition state.

```mermaid
sequenceDiagram
    participant Producer as Producer
    participant Broker as Broker (ClusterBroker)
    participant CSI as ClusterServiceImpl
    participant MS as MetadataStore

    Producer->>Broker: POST /topics/news/records
    Broker->>CSI: describe_cluster()
    CSI->>MS: get_topic_assignments("news")
    MS-->>CSI: PartitionInfo { leader: 1, replicas: [1,2,3] }
    CSI-->>Broker: Current partition leadership

    alt Broker is partition leader
        Broker->>Broker: Write record to partition log
        Broker->>CSI: report_partition_status(topic, partition, high_water_mark)
        CSI->>MS: update_partition_offset(topic, partition, offset)
        MS-->>CSI: Offset updated
        CSI-->>Broker: Status accepted
        Broker-->>Producer: 200 OK
    else Broker is not leader
        Broker-->>Producer: Redirect to leader broker
    end

    Note over Broker,MS: Periodic heartbeat task maintains broker state
    Broker->>CSI: heartbeat(broker_id, partition_status)
    CSI->>MS: update_broker_heartbeat(broker_id, timestamp)
    CSI->>MS: update_partition_offsets(partition_status)
    MS-->>CSI: State updated
    CSI-->>Broker: epoch_updates, directives
```

**Key Steps:**
1. **Leadership Discovery**: Query ClusterServiceImpl for current partition leader
2. **Record Handling**: Leader broker writes to partition log via ClusterBroker interface
3. **Status Reporting**: Broker reports high water mark through ClusterServiceImpl
4. **Heartbeat Maintenance**: Periodic heartbeat updates broker liveness and partition state in MetadataStore

### 3. Message Consumption: Offset Coordination

When a message is read from a topic, consumer group offsets are coordinated through the broker's local storage (current implementation) or cluster metadata (planned Phase 3.5).

```mermaid
sequenceDiagram
    participant Consumer as Consumer
    participant Broker as Broker (ClusterBroker)
    participant CSI as ClusterServiceImpl
    participant MS as MetadataStore
    participant LocalCG as Local ConsumerGroup

    Consumer->>Broker: GET /topics/news/records?group=analytics
    Broker->>CSI: describe_cluster()
    CSI->>MS: get_topic_assignments("news")
    MS-->>CSI: PartitionInfo { leader: 1, in_sync_replicas: [1,2] }
    CSI-->>Broker: Partition assignments

    Note over Broker,LocalCG: Current: Broker-local consumer offsets
    Broker->>LocalCG: get_consumer_offset("analytics", partition)
    LocalCG-->>Broker: Current offset: 42

    Broker->>Broker: Read records from offset 42
    Broker-->>Consumer: Records [43, 44, 45]

    Consumer->>Broker: POST /consumer-groups/analytics/offsets
    Broker->>LocalCG: commit_offset("analytics", partition, 45)
    LocalCG-->>Broker: Offset committed locally

    Note over Broker,MS: Planned Phase 3.5: Cluster-wide consumer coordination
    Note over Broker,MS: Will use MetadataStore for consumer offsets
```

**Key Steps:**
1. **Partition Discovery**: Query ClusterServiceImpl for partition assignments and leadership
2. **Offset Retrieval**: Get current consumer group offset (currently from broker-local storage)
3. **Record Delivery**: Serve records starting from the consumer's last committed offset
4. **Offset Commit**: Update consumer group progress (currently stored locally per broker)

**Future (Phase 3.5)**: Consumer group coordination will move to MetadataStore for cluster-wide consistency, enabling proper partition rebalancing and failure recovery across brokers.

### 4. Follower Replication: Leader-Follower Synchronization

Follower brokers maintain bidirectional heartbeat connections with the leader to stay in sync with partition state and receive replication directives.

```mermaid
sequenceDiagram
    participant Follower as Follower Broker
    participant Leader as Leader Broker
    participant LeaderCSI as Leader ClusterServiceImpl
    participant MS as MetadataStore
    participant FollowerCSI as Follower ClusterServiceImpl

    Note over Follower,Leader: Follower initiates bidirectional heartbeat stream

    Follower->>Leader: Open bidirectional Heartbeat stream
    Leader->>LeaderCSI: Forward stream to ClusterServiceImpl

    loop Periodic Heartbeat (every N seconds)
        Follower->>FollowerCSI: Query local partition status
        FollowerCSI-->>Follower: Local high water marks and offsets

        Follower->>Leader: HeartbeatRequest(broker_id, partition_status[])
        Leader->>LeaderCSI: handle_heartbeat(request)
        LeaderCSI->>MS: update_broker_heartbeat(follower_broker_id, timestamp)
        LeaderCSI->>MS: update_partition_offsets(partition_status)
        MS-->>LeaderCSI: State updated

        LeaderCSI->>MS: check_epoch_changes(follower_broker_id)
        MS-->>LeaderCSI: Epoch updates for partitions

        LeaderCSI->>MS: get_broker_directives(follower_broker_id)
        MS-->>LeaderCSI: Directives (RESYNC, DRAIN, SHUTDOWN)

        LeaderCSI-->>Leader: HeartbeatResponse(epoch_updates, directives)
        Leader-->>Follower: HeartbeatResponse(epoch_updates, directives)

        Follower->>Follower: Process epoch updates
        Follower->>Follower: Execute directives (if any)
    end

    Note over Follower,MS: Leader tracks follower liveness and ISR membership
    Note over Follower,MS: Follower updates local state based on epoch changes
```

**Key Steps:**
1. **Stream Initialization**: Follower opens bidirectional heartbeat stream with leader's ClusterServiceImpl
2. **Status Collection**: Follower queries its ClusterServiceImpl for local partition state (high water marks, offsets)
3. **Heartbeat Send**: Follower sends HeartbeatRequest with broker ID and partition status array
4. **Leader Processing**: Leader's ClusterServiceImpl updates broker liveness and partition offsets in MetadataStore
5. **Epoch Check**: Leader checks for any partition epoch changes the follower needs to know about
6. **Directive Generation**: Leader determines if follower needs special directives (RESYNC, DRAIN, SHUTDOWN)
7. **Response Delivery**: Leader sends HeartbeatResponse with epoch updates and directives
8. **Follower Sync**: Follower applies epoch updates and executes directives to stay in sync

**Replication Semantics:**
- **In-Sync Replica (ISR) Tracking**: Leader tracks which followers are keeping up based on heartbeat frequency and offset lag
- **Epoch-Based Consistency**: Epoch numbers prevent split-brain scenarios during leadership changes
- **Directive-Based Control**: Leader can instruct followers to RESYNC (catch up), DRAIN (prepare for shutdown), or SHUTDOWN (graceful termination)

## Cluster Protocol Messages

### DescribeCluster
Returns current cluster topology including broker health and topic assignments.

```protobuf
message DescribeClusterResponse {
  repeated BrokerInfo brokers = 1;
  repeated TopicAssignment topics = 2;
  uint32 controller_id = 3;
}
```

### Heartbeat (Bidirectional Streaming)
Maintains broker liveness and propagates partition state updates.

```protobuf
message HeartbeatRequest {
  uint32 broker_id = 1;
  repeated PartitionHeartbeat partitions = 2;
  string timestamp = 3;
}

message HeartbeatResponse {
  repeated PartitionEpochUpdate epoch_updates = 1;
  repeated BrokerDirective directives = 2;
  string timestamp = 3;
}
```

### ReportPartitionStatus
Notifies controller of partition state changes (leadership, ISR updates, offsets).

```protobuf
message ReportPartitionStatusRequest {
  string topic = 1;
  uint32 partition = 2;
  uint32 leader = 3;
  repeated uint32 in_sync_replicas = 5;
  uint64 high_water_mark = 6;
  string timestamp = 8;
}
```

## MetadataStore Backends

### In-Memory Store
- **Use Case**: Development, testing, single-broker deployments
- **Features**: Fast access, no persistence
- **Limitations**: State lost on restart

### File-Based Store
- **Use Case**: Production deployments requiring persistence
- **Features**: JSON persistence, directory locking, crash recovery
- **Storage**: Cluster state persisted to `metadata.json`

## Error Handling

The cluster coordination system provides comprehensive error handling:

- **Stale Epoch Rejection**: Prevents outdated leadership claims
- **Heartbeat Timeout**: Automatic broker failure detection
- **Split-Brain Prevention**: Epoch-based consistency guarantees
- **Network Partition Tolerance**: Graceful degradation during connectivity issues

## Integration

The cluster service integrates with FlashQ brokers through multiple layers:

### Broker-Side Integration

1. **ClusterBroker Trait Implementation**: Brokers implement the `ClusterBroker` trait to expose runtime state (offsets, leadership, partition assignments) to the cluster coordination layer
2. **ClusterServiceImpl Initialization**: Create `ClusterServiceImpl` with shared `MetadataStore`, broker ID, and `ClusterBroker` implementation
3. **ClusterServer Registration**: Wrap `ClusterServiceImpl` in `ClusterServer` and register with the broker's gRPC router
4. **Bidirectional Heartbeat**: Brokers spawn background tasks that maintain streaming heartbeat connections with `ClusterServiceImpl`

### Inter-Broker Communication

1. **ClusterClient**: Brokers use `ClusterClient` for communicating with other brokers or dedicated controller nodes
2. **gRPC Protocol**: All cluster coordination uses the gRPC protocol defined in `cluster.proto`
3. **Streaming RPCs**: Heartbeat uses bidirectional streaming for efficient real-time state synchronization

### Data Flow

- **Metadata Store**: Shared instance between broker runtime and `ClusterServiceImpl` ensures consistency
- **ClusterBroker Interface**: Service queries broker state through trait methods without tight coupling
- **Partition Status**: Broker reports partition state via `report_partition_status` and heartbeat messages
- **Epoch Updates**: Service returns epoch changes and directives through heartbeat responses

## Testing

Comprehensive test coverage includes:

- **Unit Tests**: MetadataStore operations, manifest loading, service logic
- **Integration Tests**: End-to-end cluster coordination scenarios
- **Error Simulation**: Network failures, epoch conflicts, timeout handling
- **Persistence Tests**: File-based store recovery and consistency

## Example Manifest

```yaml
brokers:
  - id: 1
    host: "broker-1.cluster.local"
    port: 9092
  - id: 2
    host: "broker-2.cluster.local"
    port: 9092
  - id: 3
    host: "broker-3.cluster.local"
    port: 9092

topics:
  news:
    partitions:
      - partition: 0
        leader: 1
        replicas: [1, 2, 3]
        in_sync_replicas: [1, 2]
      - partition: 1
        leader: 2
        replicas: [2, 3, 1]
        in_sync_replicas: [2, 3]

  analytics:
    partitions:
      - partition: 0
        leader: 3
        replicas: [3, 1, 2]
        in_sync_replicas: [3, 1, 2]
```

This cluster coordination system ensures reliable message delivery, consistent partition leadership, and robust failure handling across the FlashQ distributed message queue.