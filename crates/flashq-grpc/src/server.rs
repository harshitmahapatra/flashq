use std::net::SocketAddr;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tower_http::trace::TraceLayer;

use crate::flashq::v1::admin_server::Admin;
use crate::flashq::v1::consumer_server::Consumer;
use crate::flashq::v1::producer_server::Producer;
use crate::flashq::v1::*;

#[derive(Clone)]
pub struct FlashqGrpcService {
    pub core: Arc<flashq::FlashQ>,
}

impl FlashqGrpcService {
    pub fn new(core: Arc<flashq::FlashQ>) -> Self {
        Self { core }
    }
}

fn to_proto_record(record: &flashq::Record, include_headers: bool) -> Record {
    let headers = if include_headers {
        record.headers.clone().unwrap_or_default()
    } else {
        std::collections::HashMap::new()
    };
    Record {
        key: record.key.clone().unwrap_or_default(),
        value: record.value.clone(),
        headers,
    }
}

fn to_proto_rwo(r: &flashq::RecordWithOffset, include_headers: bool) -> RecordWithOffset {
    RecordWithOffset {
        record: Some(to_proto_record(&r.record, include_headers)),
        offset: r.offset,
        timestamp: r.timestamp.clone(),
    }
}

#[tonic::async_trait]
impl Producer for FlashqGrpcService {
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
        // Map records to core type
        let mut records = Vec::with_capacity(req.records.len());
        for (i, rec) in req.records.into_iter().enumerate() {
            if rec.value.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "records[{i}].value must be non-empty"
                )));
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
            records.push(flashq::Record {
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
impl Consumer for FlashqGrpcService {
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

        let records = records
            .iter()
            .map(|r| to_proto_rwo(r, include_headers))
            .collect();

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
        let records = records
            .iter()
            .map(|r| to_proto_rwo(r, include_headers))
            .collect();
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
            loop {
                match core.poll_records_from_offset(&req.topic, current, Some(100)) {
                    Ok(records) if !records.is_empty() => {
                        for r in records.iter() {
                            let msg = to_proto_rwo(r, req.include_headers);
                            if tx.send(Ok(msg)).await.is_err() {
                                return; // receiver dropped
                            }
                            current = r.offset.saturating_add(1);
                        }
                    }
                    Ok(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    }
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
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
impl Admin for FlashqGrpcService {
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

/// Run a gRPC server with the FlashQ services on the given address.
pub async fn serve(
    addr: SocketAddr,
    core: Arc<flashq::FlashQ>,
) -> Result<(), Box<dyn std::error::Error>> {
    let svc = FlashqGrpcService::new(core);
    tonic::transport::Server::builder()
        .layer(TraceLayer::new_for_http())
        .add_service(crate::flashq::v1::producer_server::ProducerServer::new(
            svc.clone(),
        ))
        .add_service(crate::flashq::v1::consumer_server::ConsumerServer::new(
            svc.clone(),
        ))
        .add_service(crate::flashq::v1::admin_server::AdminServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service() -> FlashqGrpcService {
        let core = Arc::new(flashq::FlashQ::new()); // defaults to in-memory storage
        FlashqGrpcService::new(core)
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
