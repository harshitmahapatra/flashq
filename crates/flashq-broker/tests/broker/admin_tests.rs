use crate::test_utilities::TestServer;
use flashq_broker::flashq::v1 as proto;
#[tokio::test]
async fn test_admin_health_and_topics() {
    let srv = TestServer::start().await.expect("start server");
    let addr = format!("http://127.0.0.1:{}", srv.port);
    let mut admin = proto::admin_client::AdminClient::connect(addr)
        .await
        .expect("connect admin");
    // Health should succeed
    let _ = admin.health(proto::Empty {}).await.expect("health ok");
    // List topics initially empty
    let topics = admin
        .list_topics(proto::Empty {})
        .await
        .expect("list topics")
        .into_inner();
    assert!(topics.topics.is_empty());
}
