use std::net::SocketAddr;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tower_http::trace::TraceLayer;

use crate::flashq::v1::admin_server::Admin;
use crate::flashq::v1::consumer_server::Consumer;
use crate::flashq::v1::producer_server::Producer;
use crate::flashq::v1::*;

mod validation {
    use tonic::Status;

    pub const MAX_KEY_SIZE: usize = 1024;
    pub const MAX_VALUE_SIZE: usize = 1_048_576; // 1MB
    pub const MAX_HEADER_VALUE_SIZE: usize = 1024;

    pub fn validate_record_for_grpc(record: &flashq_cluster::Record) -> Result<(), Box<Status>> {
        // Validate key size
        if let Some(key) = &record.key {
            if key.len() > MAX_KEY_SIZE {
                return Err(Box::new(Status::invalid_argument(format!(
                    "Key exceeds maximum length of {} characters (got {})",
                    MAX_KEY_SIZE,
                    key.len()
                ))));
            }
        }

        // Validate value size
        if record.value.len() > MAX_VALUE_SIZE {
            return Err(Box::new(Status::invalid_argument(format!(
                "Value exceeds maximum length of {} bytes (got {})",
                MAX_VALUE_SIZE,
                record.value.len()
            ))));
        }

        // Validate header values
        if let Some(headers) = &record.headers {
            for (header_key, header_value) in headers {
                if header_value.len() > MAX_HEADER_VALUE_SIZE {
                    return Err(Box::new(Status::invalid_argument(format!(
                        "Header '{}' value exceeds maximum length of {} characters (got {})",
                        header_key,
                        MAX_HEADER_VALUE_SIZE,
                        header_value.len()
                    ))));
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct FlashQGrpcBroker {
    pub core: Arc<flashq_cluster::FlashQ>,
}

impl FlashQGrpcBroker {
    pub fn new(core: Arc<flashq_cluster::FlashQ>) -> Self {
        Self { core }
    }
}

fn to_proto_record(
    record: &flashq_cluster::Record,
    include_headers: bool,
) -> Result<Record, Box<Status>> {
    // Validate record before conversion
    validation::validate_record_for_grpc(record)?;

    let headers = if include_headers {
        record.headers.clone().unwrap_or_default()
    } else {
        std::collections::HashMap::new()
    };

    Ok(Record {
        key: record.key.clone().unwrap_or_default(),
        value: record.value.clone(),
        headers,
    })
}

fn to_proto_rwo(
    r: &flashq_cluster::RecordWithOffset,
    include_headers: bool,
) -> Result<RecordWithOffset, Box<Status>> {
    Ok(RecordWithOffset {
        record: Some(to_proto_record(&r.record, include_headers)?),
        offset: r.offset,
        timestamp: r.timestamp.clone(),
    })
}

#[tonic::async_trait]
impl Producer for FlashQGrpcBroker {
    async fn produce(
        &self,
        request: Request<ProduceRequest>,
    ) -> Result<Response<ProduceResponse>, Status> {
        let req = request.into_inner();
        if req.topic.is_empty() {
            return Err(Status::invalid_argument("topic is required"));
        }
        if req.records.is_empty() {
            return Err(Status::invalid_argument("records must be non-empty"));
        }

        // Map records to core type with validation
        let mut records = Vec::with_capacity(req.records.len());
        for (i, rec) in req.records.into_iter().enumerate() {
            if rec.value.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "records[{i}].value must be non-empty"
                )));
            }

            // Validate record sizes using the same limits as HTTP API
            if !rec.key.is_empty() && rec.key.len() > validation::MAX_KEY_SIZE {
                return Err(Status::invalid_argument(format!(
                    "Record at index {} key exceeds maximum length of {} characters (got {})",
                    i,
                    validation::MAX_KEY_SIZE,
                    rec.key.len()
                )));
            }

            if rec.value.len() > validation::MAX_VALUE_SIZE {
                return Err(Status::invalid_argument(format!(
                    "Record at index {} value exceeds maximum length of {} bytes (got {})",
                    i,
                    validation::MAX_VALUE_SIZE,
                    rec.value.len()
                )));
            }

            for (header_key, header_value) in &rec.headers {
                if header_value.len() > validation::MAX_HEADER_VALUE_SIZE {
                    return Err(Status::invalid_argument(format!(
                        "Record at index {} header '{}' value exceeds maximum length of {} characters (got {})",
                        i,
                        header_key,
                        validation::MAX_HEADER_VALUE_SIZE,
                        header_value.len()
                    )));
                }
            }

            let key = if rec.key.is_empty() {
                None
            } else {
                Some(rec.key)
            };
            let headers = if rec.headers.is_empty() {
                None
            } else {
                Some(rec.headers)
            };
            records.push(flashq_cluster::Record {
                key,
                value: rec.value,
                headers,
            });
        }

        let last = self
            .core
            .post_records(req.topic.clone(), records)
            .map_err(|e| Status::internal(format!("produce failed: {e}")))?;
        // Timestamp: we return "now" in RFC3339 as HTTP does for the last record
        let timestamp = chrono::Utc::now().to_rfc3339();
        Ok(Response::new(ProduceResponse {
            offset: last,
            timestamp,
        }))
    }
}

#[tonic::async_trait]
impl Consumer for FlashQGrpcBroker {
    async fn create_consumer_group(
        &self,
        request: Request<ConsumerGroupId>,
    ) -> Result<Response<ConsumerGroupResponse>, Status> {
        let req = request.into_inner();
        if req.group_id.is_empty() {
            return Err(Status::invalid_argument("group_id is required"));
        }
        self.core
            .create_consumer_group(req.group_id.clone())
            .map_err(|e| Status::internal(format!("create_consumer_group failed: {e}")))?;
        Ok(Response::new(ConsumerGroupResponse {
            group_id: req.group_id,
        }))
    }

    async fn delete_consumer_group(
        &self,
        request: Request<ConsumerGroupId>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        if req.group_id.is_empty() {
            return Err(Status::invalid_argument("group_id is required"));
        }
        self.core
            .delete_consumer_group(&req.group_id)
            .map_err(|e| Status::internal(format!("delete_consumer_group failed: {e}")))?;
        Ok(Response::new(Empty {}))
    }

    async fn fetch_by_offset(
        &self,
        request: Request<FetchByOffsetRequest>,
    ) -> Result<Response<FetchResponse>, Status> {
        let req = request.into_inner();
        if req.group_id.is_empty() || req.topic.is_empty() {
            return Err(Status::invalid_argument("group_id and topic are required"));
        }
        // Determine starting offset
        let mut offset = req.from_offset;
        if offset == 0 {
            offset = self
                .core
                .get_consumer_group_offset(&req.group_id, &req.topic)
                .map_err(|e| Status::internal(format!("get_consumer_group_offset failed: {e}")))?;
        }
        let limit = if req.max_records == 0 {
            100
        } else {
            req.max_records as usize
        };
        let include_headers = req.include_headers;

        let records = self
            .core
            .poll_records_from_offset(&req.topic, offset, Some(limit))
            .map_err(|e| Status::internal(format!("poll_records_from_offset failed: {e}")))?;

        let next_offset = records
            .last()
            .map(|r| r.offset.saturating_add(1))
            .unwrap_or(offset);
        let high_water_mark = self.core.get_high_water_mark(&req.topic);
        let lag = high_water_mark.saturating_sub(next_offset);

        let records: Result<Vec<_>, Box<Status>> = records
            .iter()
            .map(|r| to_proto_rwo(r, include_headers))
            .collect();
        let records =
            records.map_err(|e| Status::internal(format!("Record validation failed: {e}")))?;

        Ok(Response::new(FetchResponse {
            records,
            next_offset,
            high_water_mark,
            lag,
        }))
    }

    async fn fetch_by_time(
        &self,
        request: Request<FetchByTimeRequest>,
    ) -> Result<Response<FetchResponse>, Status> {
        let req = request.into_inner();
        if req.group_id.is_empty() || req.topic.is_empty() || req.from_time.is_empty() {
            return Err(Status::invalid_argument(
                "group_id, topic and from_time are required",
            ));
        }
        let limit = if req.max_records == 0 {
            100
        } else {
            req.max_records as usize
        };
        let include_headers = req.include_headers;
        let records = self
            .core
            .poll_records_from_time(&req.topic, &req.from_time, Some(limit))
            .map_err(|e| Status::internal(format!("poll_records_from_time failed: {e}")))?;
        let next_offset = records
            .last()
            .map(|r| r.offset.saturating_add(1))
            .unwrap_or_else(|| {
                self.core
                    .get_consumer_group_offset(&req.group_id, &req.topic)
                    .unwrap_or(0)
            });
        let high_water_mark = self.core.get_high_water_mark(&req.topic);
        let lag = high_water_mark.saturating_sub(next_offset);
        let records: Result<Vec<_>, Box<Status>> = records
            .iter()
            .map(|r| to_proto_rwo(r, include_headers))
            .collect();
        let records =
            records.map_err(|e| Status::internal(format!("Record validation failed: {e}")))?;
        Ok(Response::new(FetchResponse {
            records,
            next_offset,
            high_water_mark,
            lag,
        }))
    }

    async fn commit_offset(
        &self,
        request: Request<CommitOffsetRequest>,
    ) -> Result<Response<CommitOffsetResponse>, Status> {
        let req = request.into_inner();
        if req.group_id.is_empty() || req.topic.is_empty() {
            return Err(Status::invalid_argument("group_id and topic are required"));
        }
        self.core
            .update_consumer_group_offset(&req.group_id, req.topic.clone(), req.offset)
            .map_err(|e| Status::internal(format!("update_consumer_group_offset failed: {e}")))?;
        let ts = chrono::Utc::now().to_rfc3339();
        Ok(Response::new(CommitOffsetResponse {
            topic: req.topic,
            committed_offset: req.offset,
            timestamp: ts,
        }))
    }

    async fn get_consumer_group_offset(
        &self,
        request: Request<GetOffsetRequest>,
    ) -> Result<Response<GetOffsetResponse>, Status> {
        let req = request.into_inner();
        if req.group_id.is_empty() || req.topic.is_empty() {
            return Err(Status::invalid_argument("group_id and topic are required"));
        }
        let offset = self
            .core
            .get_consumer_group_offset(&req.group_id, &req.topic)
            .map_err(|e| Status::internal(format!("get_consumer_group_offset failed: {e}")))?;
        Ok(Response::new(GetOffsetResponse {
            group_id: req.group_id,
            topic: req.topic,
            offset,
        }))
    }

    type SubscribeStream = tokio_stream::wrappers::ReceiverStream<Result<RecordWithOffset, Status>>;

    async fn subscribe(
        &self,
        request: Request<FetchByOffsetRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        if req.group_id.is_empty() || req.topic.is_empty() {
            return Err(Status::invalid_argument("group_id and topic are required"));
        }

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let core = self.core.clone();

        tokio::spawn(async move {
            let mut current = if req.from_offset == 0 {
                core.get_consumer_group_offset(&req.group_id, &req.topic)
                    .unwrap_or(0)
            } else {
                req.from_offset
            };

            // Circuit breaker state
            let mut consecutive_errors = 0;
            const MAX_CONSECUTIVE_ERRORS: u32 = 5;
            const INITIAL_RETRY_DELAY: u64 = 200;
            const MAX_RETRY_DELAY: u64 = 5000;

            loop {
                match core.poll_records_from_offset(&req.topic, current, Some(100)) {
                    Ok(records) if !records.is_empty() => {
                        consecutive_errors = 0; // Reset on success

                        for r in records.iter() {
                            let msg = match to_proto_rwo(r, req.include_headers) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    let _ = tx.send(Err(*e)).await;
                                    return;
                                }
                            };
                            if tx.send(Ok(msg)).await.is_err() {
                                return; // Client disconnected
                            }
                            current = r.offset.saturating_add(1);
                        }
                    }
                    Ok(_) => {
                        // No records, wait before polling again
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    }
                    Err(e) => {
                        consecutive_errors += 1;

                        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                            // Circuit breaker triggered
                            let _ = tx
                                .send(Err(Status::internal(format!(
                                    "Too many consecutive errors ({consecutive_errors}): {e}"
                                ))))
                                .await;
                            return;
                        }

                        // Exponential backoff with jitter
                        let delay = std::cmp::min(
                            INITIAL_RETRY_DELAY * 2_u64.pow(consecutive_errors - 1),
                            MAX_RETRY_DELAY,
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    }
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}

#[tonic::async_trait]
impl Admin for FlashQGrpcBroker {
    async fn list_topics(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        Ok(Response::new(ListTopicsResponse {
            topics: self.core.get_topics(),
        }))
    }

    async fn high_water_mark(
        &self,
        request: Request<HighWaterMarkRequest>,
    ) -> Result<Response<HighWaterMarkResponse>, Status> {
        let req = request.into_inner();
        if req.topic.is_empty() {
            return Err(Status::invalid_argument("topic is required"));
        }
        let hwm = self.core.get_high_water_mark(&req.topic);
        Ok(Response::new(HighWaterMarkResponse {
            topic: req.topic,
            high_water_mark: hwm,
        }))
    }

    async fn health(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }
}
// FlashQBroker trait implementation
#[async_trait::async_trait]
impl flashq_cluster::ClusterBroker for FlashQGrpcBroker {
    async fn get_high_water_mark(
        &self,
        topic: &str,
        partition: flashq_cluster::types::PartitionId,
    ) -> Result<u64, flashq_cluster::ClusterError> {
        // For now, FlashQ only supports single partition (partition 0)
        if partition.0 != 0 {
            return Err(flashq_cluster::ClusterError::PartitionNotFound {
                topic: topic.to_string(),
                partition_id: partition.0,
            });
        }

        Ok(self.core.get_high_water_mark(topic))
    }

    async fn get_log_start_offset(
        &self,
        topic: &str,
        partition: flashq_cluster::types::PartitionId,
    ) -> Result<u64, flashq_cluster::ClusterError> {
        // For now, FlashQ only supports single partition (partition 0)
        if partition.0 != 0 {
            return Err(flashq_cluster::ClusterError::PartitionNotFound {
                topic: topic.to_string(),
                partition_id: partition.0,
            });
        }

        // FlashQ currently starts at offset 0 for all topics
        Ok(0)
    }

    async fn is_partition_leader(
        &self,
        _topic: &str,
        partition: flashq_cluster::types::PartitionId,
    ) -> Result<bool, flashq_cluster::ClusterError> {
        // For now, FlashQ only supports single partition (partition 0)
        if partition.0 != 0 {
            return Err(flashq_cluster::ClusterError::PartitionNotFound {
                topic: _topic.to_string(),
                partition_id: partition.0,
            });
        }

        // In standalone mode, this broker is always the leader
        // TODO: Implement proper leadership logic when cluster mode is fully supported
        Ok(true)
    }

    async fn get_assigned_partitions(
        &self,
    ) -> Result<Vec<(String, flashq_cluster::types::PartitionId)>, flashq_cluster::ClusterError>
    {
        let topics = self.core.get_topics();
        let mut partitions = Vec::new();

        for topic in topics {
            // For now, FlashQ only supports single partition (partition 0) per topic
            partitions.push((topic, flashq_cluster::types::PartitionId(0)));
        }

        Ok(partitions)
    }

    async fn acknowledge_replication(
        &self,
        topic: &str,
        partition: flashq_cluster::types::PartitionId,
        _offset: u64,
    ) -> Result<(), flashq_cluster::ClusterError> {
        // For now, FlashQ only supports single partition (partition 0)
        if partition.0 != 0 {
            return Err(flashq_cluster::ClusterError::PartitionNotFound {
                topic: topic.to_string(),
                partition_id: partition.0,
            });
        }

        // TODO: Implement replication acknowledgment logic
        // For now, we'll just return success since FlashQ doesn't have replication yet
        Ok(())
    }

    async fn initiate_shutdown(&self) -> Result<(), flashq_cluster::ClusterError> {
        // TODO: Implement graceful shutdown logic
        // This should signal the broker to stop accepting new requests and drain existing ones
        tracing::info!("Graceful shutdown initiated");
        Ok(())
    }
}

/// Run a gRPC server with the FlashQ services on the given address.
pub async fn serve(
    addr: SocketAddr,
    core: Arc<flashq_cluster::FlashQ>,
    cluster_service: Arc<flashq_cluster::service::ClusterServiceImpl>,
) -> Result<(), Box<dyn std::error::Error>> {
    let svc = FlashQGrpcBroker::new(core);
    tonic::transport::Server::builder()
        .layer(TraceLayer::new_for_http())
        .add_service(crate::flashq::v1::producer_server::ProducerServer::new(
            svc.clone(),
        ))
        .add_service(crate::flashq::v1::consumer_server::ConsumerServer::new(
            svc.clone(),
        ))
        .add_service(crate::flashq::v1::admin_server::AdminServer::new(svc))
        .add_service(crate::ClusterServer::new(
            flashq_cluster::ClusterServer::new(cluster_service),
        ))
        .serve(addr)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service() -> FlashQGrpcBroker {
        let core = Arc::new(flashq_cluster::FlashQ::new()); // defaults to in-memory storage
        FlashQGrpcBroker::new(core)
    }

    #[tokio::test]
    async fn test_producer_consumer_offset_flow() {
        let svc = service();
        let topic = "unit-topic".to_string();
        // produce
        let req = ProduceRequest {
            topic: topic.clone(),
            records: vec![
                Record {
                    key: String::new(),
                    value: "a".into(),
                    headers: Default::default(),
                },
                Record {
                    key: String::new(),
                    value: "b".into(),
                    headers: Default::default(),
                },
            ],
        };
        let resp = Producer::produce(&svc, Request::new(req))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.offset >= 1);

        // create group
        let group = "g-unit".to_string();
        let _ = Consumer::create_consumer_group(
            &svc,
            Request::new(ConsumerGroupId {
                group_id: group.clone(),
            }),
        )
        .await
        .unwrap();

        // fetch by offset from committed (0)
        let fetched = Consumer::fetch_by_offset(
            &svc,
            Request::new(FetchByOffsetRequest {
                group_id: group.clone(),
                topic: topic.clone(),
                from_offset: 0,
                max_records: 10,
                include_headers: true,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(fetched.records.len(), 2);
        assert_eq!(fetched.next_offset, 2);
        assert!(fetched.high_water_mark >= 2);
    }

    #[tokio::test]
    async fn test_commit_and_get_offset() {
        let svc = service();
        let topic = "unit-offset".to_string();
        let group = "g2".to_string();
        let _ = Consumer::create_consumer_group(
            &svc,
            Request::new(ConsumerGroupId {
                group_id: group.clone(),
            }),
        )
        .await
        .unwrap();

        let _ = Producer::produce(
            &svc,
            Request::new(ProduceRequest {
                topic: topic.clone(),
                records: vec![Record {
                    key: String::new(),
                    value: "x".into(),
                    headers: Default::default(),
                }],
            }),
        )
        .await
        .unwrap();

        let resp = Consumer::commit_offset(
            &svc,
            Request::new(CommitOffsetRequest {
                group_id: group.clone(),
                topic: topic.clone(),
                offset: 1,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(resp.committed_offset, 1);

        let got = Consumer::get_consumer_group_offset(
            &svc,
            Request::new(GetOffsetRequest {
                group_id: group,
                topic,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(got.offset, 1);
    }

    #[tokio::test]
    async fn test_admin_endpoints() {
        let svc = service();
        let topic = "unit-admin".to_string();
        // Before produce: topics empty
        let list = Admin::list_topics(&svc, Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();
        assert!(list.topics.is_empty());
        // Produce a record creates topic lazily
        let _ = Producer::produce(
            &svc,
            Request::new(ProduceRequest {
                topic: topic.clone(),
                records: vec![Record {
                    key: String::new(),
                    value: "r".into(),
                    headers: Default::default(),
                }],
            }),
        )
        .await
        .unwrap();
        let list = Admin::list_topics(&svc, Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();
        assert!(list.topics.contains(&topic));
        let hwm = Admin::high_water_mark(&svc, Request::new(HighWaterMarkRequest { topic }))
            .await
            .unwrap()
            .into_inner();
        assert!(hwm.high_water_mark >= 1);
        let _ = Admin::health(&svc, Request::new(Empty {})).await.unwrap();
    }

    #[tokio::test]
    async fn test_fetch_by_time_unit() {
        let svc = service();
        let topic = "unit-time".to_string();
        let group = "g3".to_string();
        let _ = Consumer::create_consumer_group(
            &svc,
            Request::new(ConsumerGroupId {
                group_id: group.clone(),
            }),
        )
        .await
        .unwrap();
        let _ = Producer::produce(
            &svc,
            Request::new(ProduceRequest {
                topic: topic.clone(),
                records: vec![Record {
                    key: String::new(),
                    value: "tv".into(),
                    headers: Default::default(),
                }],
            }),
        )
        .await
        .unwrap();
        let fetched = Consumer::fetch_by_time(
            &svc,
            Request::new(FetchByTimeRequest {
                group_id: group,
                topic,
                from_time: "1970-01-01T00:00:00Z".to_string(),
                max_records: 10,
                include_headers: true,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(!fetched.records.is_empty());
    }
}
