use crate::test_utilities::TestServer;
use flashq_grpc::flashq::v1 as proto;
use uuid::Uuid;

fn unique_group() -> String {
    format!("test_group_{}", Uuid::new_v4().to_string().replace('-', ""))
}
fn unique_topic() -> String {
    format!("test_topic_{}", Uuid::new_v4().to_string().replace('-', ""))
}

#[tokio::test]
async fn test_memory_backend_basic_operations() {
    let srv = TestServer::start().await.expect("start server");
    let addr = format!("http://127.0.0.1:{}", srv.port);
    let topic = unique_topic();

    let mut producer = proto::producer_client::ProducerClient::connect(addr.clone())
        .await
        .unwrap();
    let mut consumer = proto::consumer_client::ConsumerClient::connect(addr)
        .await
        .unwrap();

    producer
        .produce(proto::ProduceRequest {
            topic: topic.clone(),
            records: vec![proto::Record {
                key: String::new(),
                value: "Memory test record".into(),
                headers: Default::default(),
            }],
        })
        .await
        .unwrap();

    let group = unique_group();
    consumer
        .create_consumer_group(proto::ConsumerGroupId {
            group_id: group.clone(),
        })
        .await
        .unwrap();
    let fetched = consumer
        .fetch_by_offset(proto::FetchByOffsetRequest {
            group_id: group,
            topic,
            from_offset: 0,
            max_records: 1,
            include_headers: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(
        fetched.records[0].record.as_ref().unwrap().value,
        "Memory test record"
    );
}

#[tokio::test]
async fn test_file_backend_basic_operations() {
    let srv = TestServer::start_with_storage("file")
        .await
        .expect("start file server");
    let addr = format!("http://127.0.0.1:{}", srv.port);
    let topic = unique_topic();
    let mut producer = proto::producer_client::ProducerClient::connect(addr.clone())
        .await
        .unwrap();
    let mut consumer = proto::consumer_client::ConsumerClient::connect(addr)
        .await
        .unwrap();

    producer
        .produce(proto::ProduceRequest {
            topic: topic.clone(),
            records: vec![proto::Record {
                key: String::new(),
                value: "File test record".into(),
                headers: Default::default(),
            }],
        })
        .await
        .unwrap();

    let group = unique_group();
    consumer
        .create_consumer_group(proto::ConsumerGroupId {
            group_id: group.clone(),
        })
        .await
        .unwrap();
    let fetched = consumer
        .fetch_by_offset(proto::FetchByOffsetRequest {
            group_id: group,
            topic: topic.clone(),
            from_offset: 0,
            max_records: 1,
            include_headers: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(
        fetched.records[0].record.as_ref().unwrap().value,
        "File test record"
    );

    if let Some(dir) = srv.data_dir() {
        assert!(dir.exists());
        assert!(dir.is_dir());
    }
}

#[tokio::test]
async fn test_file_backend_persistence_across_restarts() {
    let topic = unique_topic();
    let tmp = tempfile::Builder::new()
        .prefix("flashq_persist_grpc_")
        .tempdir()
        .unwrap();

    // First server
    let srv = TestServer::start_with_data_dir(tmp.path())
        .await
        .expect("start server");
    let addr = format!("http://127.0.0.1:{}", srv.port);
    let mut producer = proto::producer_client::ProducerClient::connect(addr.clone())
        .await
        .unwrap();
    let mut consumer = proto::consumer_client::ConsumerClient::connect(addr)
        .await
        .unwrap();
    let group = unique_group();
    consumer
        .create_consumer_group(proto::ConsumerGroupId {
            group_id: group.clone(),
        })
        .await
        .unwrap();
    for i in 0..5 {
        let _ = producer
            .produce(proto::ProduceRequest {
                topic: topic.clone(),
                records: vec![proto::Record {
                    key: String::new(),
                    value: format!("Persistent record {i}"),
                    headers: Default::default(),
                }],
            })
            .await
            .unwrap();
    }
    let fetched = consumer
        .fetch_by_offset(proto::FetchByOffsetRequest {
            group_id: group.clone(),
            topic: topic.clone(),
            from_offset: 0,
            max_records: 100,
            include_headers: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.high_water_mark, 5);
    drop(consumer);
    drop(producer);
    drop(srv);

    // Restart server on same dir
    let srv2 = TestServer::start_with_data_dir(tmp.path())
        .await
        .expect("restart server");
    let addr2 = format!("http://127.0.0.1:{}", srv2.port);
    let mut consumer2 = proto::consumer_client::ConsumerClient::connect(addr2)
        .await
        .unwrap();
    let fetched2 = consumer2
        .fetch_by_offset(proto::FetchByOffsetRequest {
            group_id: group,
            topic: topic.clone(),
            from_offset: 0,
            max_records: 100,
            include_headers: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched2.high_water_mark, 5);
    assert_eq!(
        fetched2.records[0].record.as_ref().unwrap().value,
        "Persistent record 0"
    );
    assert_eq!(
        fetched2.records[4].record.as_ref().unwrap().value,
        "Persistent record 4"
    );
}

#[tokio::test]
async fn test_consumer_group_persistence_across_restarts() {
    let topic = unique_topic();
    let group = unique_group();
    let tmp = tempfile::Builder::new()
        .prefix("flashq_consumer_persist_grpc_")
        .tempdir()
        .unwrap();

    let srv = TestServer::start_with_data_dir(tmp.path())
        .await
        .expect("start server");
    let addr = format!("http://127.0.0.1:{}", srv.port);
    let mut producer = proto::producer_client::ProducerClient::connect(addr.clone())
        .await
        .unwrap();
    let mut consumer = proto::consumer_client::ConsumerClient::connect(addr)
        .await
        .unwrap();
    consumer
        .create_consumer_group(proto::ConsumerGroupId {
            group_id: group.clone(),
        })
        .await
        .unwrap();
    for i in 0..3 {
        let _ = producer
            .produce(proto::ProduceRequest {
                topic: topic.clone(),
                records: vec![proto::Record {
                    key: String::new(),
                    value: format!("Consumer record {i}"),
                    headers: Default::default(),
                }],
            })
            .await
            .unwrap();
    }
    consumer
        .commit_offset(proto::CommitOffsetRequest {
            group_id: group.clone(),
            topic: topic.clone(),
            offset: 2,
        })
        .await
        .unwrap();
    let got = consumer
        .get_consumer_group_offset(proto::GetOffsetRequest {
            group_id: group.clone(),
            topic: topic.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.offset, 2);
    drop(consumer);
    drop(producer);
    drop(srv);

    // Restart
    let srv2 = TestServer::start_with_data_dir(tmp.path())
        .await
        .expect("restart");
    let addr2 = format!("http://127.0.0.1:{}", srv2.port);
    let mut consumer2 = proto::consumer_client::ConsumerClient::connect(addr2)
        .await
        .unwrap();
    let got2 = consumer2
        .get_consumer_group_offset(proto::GetOffsetRequest {
            group_id: group.clone(),
            topic: topic.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got2.offset, 2);
    let hwm = flashq_grpc::flashq::v1::admin_client::AdminClient::connect(format!(
        "http://127.0.0.1:{}",
        srv2.port
    ))
    .await
    .unwrap()
    .high_water_mark(proto::HighWaterMarkRequest { topic })
    .await
    .unwrap()
    .into_inner()
    .high_water_mark;
    assert_eq!(hwm, 3);
}

#[tokio::test]
async fn test_memory_vs_file_backend_consistency() {
    let topic = unique_topic();
    // memory
    let srv_mem = TestServer::start().await.expect("start mem");
    let addr_mem = format!("http://127.0.0.1:{}", srv_mem.port);
    let mut prod_mem = proto::producer_client::ProducerClient::connect(addr_mem.clone())
        .await
        .unwrap();
    let mut cons_mem = proto::consumer_client::ConsumerClient::connect(addr_mem)
        .await
        .unwrap();
    // file
    let srv_file = TestServer::start_with_storage("file")
        .await
        .expect("start file");
    let addr_file = format!("http://127.0.0.1:{}", srv_file.port);
    let mut prod_file = proto::producer_client::ProducerClient::connect(addr_file.clone())
        .await
        .unwrap();
    let mut cons_file = proto::consumer_client::ConsumerClient::connect(addr_file)
        .await
        .unwrap();

    let recs = vec!["Record A", "Record B", "Record C"];
    for r in &recs {
        let rec = proto::Record {
            key: String::new(),
            value: r.to_string(),
            headers: Default::default(),
        };
        let _ = prod_mem
            .produce(proto::ProduceRequest {
                topic: topic.clone(),
                records: vec![rec.clone()],
            })
            .await
            .unwrap();
        let _ = prod_file
            .produce(proto::ProduceRequest {
                topic: topic.clone(),
                records: vec![rec],
            })
            .await
            .unwrap();
    }

    // fetch all from 0 on both
    let group1 = unique_group();
    cons_mem
        .create_consumer_group(proto::ConsumerGroupId {
            group_id: group1.clone(),
        })
        .await
        .unwrap();
    let d1 = cons_mem
        .fetch_by_offset(proto::FetchByOffsetRequest {
            group_id: group1,
            topic: topic.clone(),
            from_offset: 0,
            max_records: 100,
            include_headers: true,
        })
        .await
        .unwrap()
        .into_inner();

    let group2 = unique_group();
    cons_file
        .create_consumer_group(proto::ConsumerGroupId {
            group_id: group2.clone(),
        })
        .await
        .unwrap();
    let d2 = cons_file
        .fetch_by_offset(proto::FetchByOffsetRequest {
            group_id: group2,
            topic: topic.clone(),
            from_offset: 0,
            max_records: 100,
            include_headers: true,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(d1.records.len(), d2.records.len());
    assert_eq!(d1.high_water_mark, d2.high_water_mark);
    for (a, b) in d1.records.iter().zip(d2.records.iter()) {
        assert_eq!(a.offset, b.offset);
        assert_eq!(
            a.record.as_ref().unwrap().value,
            b.record.as_ref().unwrap().value
        );
    }
}

#[tokio::test]
async fn test_file_backend_creates_data_directory() {
    let srv = TestServer::start_with_storage("file")
        .await
        .expect("start file");
    let addr = format!("http://127.0.0.1:{}", srv.port);
    let topic = unique_topic();
    let mut producer = proto::producer_client::ProducerClient::connect(addr)
        .await
        .unwrap();
    producer
        .produce(proto::ProduceRequest {
            topic: topic.clone(),
            records: vec![proto::Record {
                key: String::new(),
                value: "Directory test record".into(),
                headers: Default::default(),
            }],
        })
        .await
        .unwrap();
    if let Some(data_dir) = srv.data_dir() {
        assert!(data_dir.exists());
        assert!(data_dir.is_dir());
        let topic_dir = data_dir.join(&topic);
        assert!(topic_dir.exists());
        assert!(topic_dir.is_dir());
    } else {
        panic!("Expected data directory to be set for file backend");
    }
}
