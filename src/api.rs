use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct PostMessageRequest {
    pub content: String,
}

#[derive(Serialize, Deserialize)]
pub struct PostMessageResponse {
    pub id: u64,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize)]
pub struct MessageResponse {
    pub id: u64,
    pub content: String,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize)]
pub struct PollMessagesResponse {
    pub messages: Vec<MessageResponse>,
    pub count: usize,
}

#[derive(Serialize, Deserialize)]
pub struct PollQuery {
    pub count: Option<usize>,
}

#[derive(Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}
