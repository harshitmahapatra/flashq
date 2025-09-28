use crate::test_utilities::TestServer;

#[tokio::test]
async fn test_admin_health_and_topics() {
    let srv = TestServer::start().await.expect("start server");
    let addr = format!("http://127.0.0.1:{}", srv.port);
    let mut admin = flashq_grpc::flashq::v1::admin_client::AdminClient::connect(addr)
        .await
        .expect("connect admin");
    // Health should succeed
    let _ = admin
        .health(flashq_grpc::flashq::v1::Empty {})
        .await
        .expect("health ok");
    // List topics initially empty
    let topics = admin
        .list_topics(flashq_grpc::flashq::v1::Empty {})
        .await
        .expect("list topics")
        .into_inner();
    assert!(topics.topics.is_empty());
}
