use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct PostMessageRequest {
    pub key: Option<String>,
    pub value: String,
    pub headers: Option<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize)]
pub struct PostMessageResponse {
    pub offset: u64,       // replaces 'id'
    pub timestamp: String, // ISO 8601 format, replaces u64
}

#[derive(Serialize, Deserialize)]
pub struct MessageResponse {
    pub key: Option<String>,
    pub value: String,
    pub headers: Option<HashMap<String, String>>,
    pub offset: u64,       // replaces 'id'
    pub timestamp: String, // ISO 8601 format, replaces u64
}

#[derive(Serialize, Deserialize)]
pub struct PollMessagesResponse {
    pub messages: Vec<MessageResponse>,
    pub count: usize,
}

#[derive(Serialize, Deserialize)]
pub struct PollQuery {
    pub count: Option<usize>,
    pub from_offset: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

// Consumer Group API Types
#[derive(Serialize, Deserialize)]
pub struct CreateConsumerGroupRequest {
    pub group_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct CreateConsumerGroupResponse {
    pub group_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct GetConsumerGroupOffsetResponse {
    pub group_id: String,
    pub topic: String,
    pub offset: u64,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateConsumerGroupOffsetRequest {
    pub offset: u64,
}

#[derive(Serialize, Deserialize)]
pub struct ConsumerGroupPollResponse {
    pub messages: Vec<MessageResponse>,
    pub count: usize,
    pub new_offset: u64,
}
