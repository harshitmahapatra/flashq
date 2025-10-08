use crate::test_utilities::TestServer;
use flashq_broker::flashq::v1 as proto;
#[tokio::test]
async fn test_offsets_commit_and_get() {
    let srv = TestServer::start().await.expect("start server");
    let addr = format!("http://127.0.0.1:{}", srv.port);
    let topic = "offset-topic".to_string();
    let group = "grp-offset".to_string();

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

    // Produce some records
    for i in 0..5 {
        let _ = producer
            .produce(proto::ProduceRequest {
                topic: topic.clone(),
                records: vec![proto::Record {
                    key: String::new(),
                    value: format!("r{i}"),
                    headers: Default::default(),
                }],
            })
            .await
            .unwrap();
    }

    // Commit offset 3 and read back
    let resp = consumer
        .commit_offset(proto::CommitOffsetRequest {
            group_id: group.clone(),
            topic: topic.clone(),
            offset: 3,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.committed_offset, 3);

    let got = consumer
        .get_consumer_group_offset(proto::GetOffsetRequest {
            group_id: group.clone(),
            topic: topic.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.offset, 3);
}

#[tokio::test]
async fn test_fetch_by_time() {
    let srv = TestServer::start().await.expect("start server");
    let addr = format!("http://127.0.0.1:{}", srv.port);
    let topic = "time-topic".to_string();
    let group = "grp-time".to_string();

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

    // Produce 2 records
    for i in 0..2 {
        let _ = producer
            .produce(proto::ProduceRequest {
                topic: topic.clone(),
                records: vec![proto::Record {
                    key: String::new(),
                    value: format!("tv{i}"),
                    headers: Default::default(),
                }],
            })
            .await
            .unwrap();
    }

    // Fetch from epoch start to ensure we get all
    let fetched = consumer
        .fetch_by_time(proto::FetchByTimeRequest {
            group_id: group,
            topic,
            from_time: "1970-01-01T00:00:00Z".to_string(),
            max_records: 10,
            include_headers: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(fetched.records.len() >= 2);
}
