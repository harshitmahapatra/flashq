use super::test_utilities::TestServer;
use flashq_grpc::flashq::v1 as proto;

#[tokio::test]
async fn test_subscribe_receives_new_records() {
    let srv = TestServer::start().await.expect("start server");
    let addr = format!("http://127.0.0.1:{}", srv.port);

    let mut producer = proto::producer_client::ProducerClient::connect(addr.clone())
        .await
        .unwrap();
    let mut consumer = proto::consumer_client::ConsumerClient::connect(addr)
        .await
        .unwrap();

    let topic = "sub-topic".to_string();
    let group = "grp-sub".to_string();
    consumer
        .create_consumer_group(proto::ConsumerGroupId {
            group_id: group.clone(),
        })
        .await
        .unwrap();

    // Start subscription from beginning
    let req = proto::FetchByOffsetRequest {
        group_id: group,
        topic: topic.clone(),
        from_offset: 0,
        max_records: 100,
        include_headers: true,
    };
    let mut stream = consumer.subscribe(req).await.unwrap().into_inner();

    // Produce a record and expect it to appear in the stream
    let _ = producer
        .produce(proto::ProduceRequest {
            topic: topic.clone(),
            records: vec![proto::Record {
                key: String::new(),
                value: "hello-sub".into(),
                headers: Default::default(),
            }],
        })
        .await
        .unwrap();

    // Receive at least one message
    let item = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        stream.message().await
    })
    .await
    .expect("timely message")
    .expect("stream result ok")
    .expect("message present");

    assert_eq!(item.record.unwrap().value, "hello-sub");
}
