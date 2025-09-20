use super::test_utilities::TestServer;
use flashq_grpc::flashq::v1 as proto;

#[tokio::test]
async fn test_produce_and_fetch_offset() {
    let srv = TestServer::start().await.expect("start server");
    let addr = format!("http://127.0.0.1:{}", srv.port);

    let mut producer = proto::producer_client::ProducerClient::connect(addr.clone())
        .await
        .expect("connect producer");
    let topic = "test-topic".to_string();

    // Produce 3 records
    let recs = vec![
        proto::Record {
            key: "k1".into(),
            value: "v1".into(),
            headers: Default::default(),
        },
        proto::Record {
            key: "".into(),
            value: "v2".into(),
            headers: Default::default(),
        },
        proto::Record {
            key: "k3".into(),
            value: "v3".into(),
            headers: Default::default(),
        },
    ];
    let resp = producer
        .produce(proto::ProduceRequest {
            topic: topic.clone(),
            records: recs,
        })
        .await
        .expect("produce")
        .into_inner();
    assert!(resp.offset >= 2);

    // Create group and fetch by offset
    let mut consumer = proto::consumer_client::ConsumerClient::connect(addr)
        .await
        .expect("connect consumer");
    let group_id = "g1".to_string();
    consumer
        .create_consumer_group(proto::ConsumerGroupId {
            group_id: group_id.clone(),
        })
        .await
        .expect("create group");

    let fetched = consumer
        .fetch_by_offset(proto::FetchByOffsetRequest {
            group_id: group_id.clone(),
            topic: topic.clone(),
            from_offset: 0, // use committed; initially 0
            max_records: 10,
            include_headers: true,
        })
        .await
        .expect("fetch")
        .into_inner();

    assert_eq!(fetched.records.len(), 3);
    assert_eq!(fetched.next_offset, 3);
    assert!(fetched.high_water_mark >= 3);
}
