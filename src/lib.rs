use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

pub mod demo;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Message {
    pub content: String,
    pub timestamp: u64,
    pub id: u64,
}

impl Message {
    pub fn new(content: String, id: u64) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Message {
            content,
            timestamp,
            id,
        }
    }
}

pub struct MessageQueue {
    topics: Arc<Mutex<HashMap<String, VecDeque<Message>>>>,
    next_id: Arc<Mutex<u64>>,
}

impl Default for MessageQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageQueue {
    pub fn new() -> Self {
        MessageQueue {
            topics: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(0)),
        }
    }

    pub fn post_message(&self, topic: String, content: String) -> Result<u64, String> {
        let mut topics = self
            .topics
            .lock()
            .map_err(|_| format!("Failed to acquire topics lock while posting to '{topic}"))?;
        let mut id_counter = self
            .next_id
            .lock()
            .map_err(|_| format!("Failed to acquire ID counter lock while posting to '{topic}'"))?;

        let message_id = *id_counter;
        *id_counter += 1;

        let message = Message::new(content, message_id);

        topics
            .entry(topic)
            .or_insert_with(VecDeque::new)
            .push_back(message);

        Ok(message_id)
    }

    pub fn poll_messages(&self, topic: &str, count: Option<usize>) -> Result<Vec<Message>, String> {
        let topics = self
            .topics
            .lock()
            .map_err(|_| format!("Failed to acquire lock while polling topic '{topic}"))?;
        if let Some(queue) = topics.get(topic) {
            if queue.is_empty() {
                Err(format!("The topic '{topic}' does not have any messages."))?
            }
            Ok(queue
                .iter()
                .take(count.unwrap_or(usize::MAX))
                .cloned()
                .collect())
        } else {
            Err(format!("The topic '{topic}' does not exist!"))
        }
    }
}

pub mod api {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let message = Message::new("test content".to_string(), 42);
        assert_eq!(message.content, "test content");
        assert_eq!(message.id, 42);
        assert!(message.timestamp > 0);
    }

    #[test]
    fn test_queue_creation() {
        let queue = MessageQueue::new();
        // Queue should be created successfully - no panic means success
        drop(queue);
    }

    #[test]
    fn test_post_single_message() {
        let queue = MessageQueue::new();
        let result = queue.post_message("test_topic".to_string(), "test message".to_string());

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0); // First message should have ID 0
    }

    #[test]
    fn test_post_multiple_messages_increment_id() {
        let queue = MessageQueue::new();

        let id1 = queue
            .post_message("topic".to_string(), "msg1".to_string())
            .unwrap();
        let id2 = queue
            .post_message("topic".to_string(), "msg2".to_string())
            .unwrap();
        let id3 = queue
            .post_message("different_topic".to_string(), "msg3".to_string())
            .unwrap();

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id3, 2);
    }

    #[test]
    fn test_poll_messages_from_existing_topic() {
        let queue = MessageQueue::new();

        queue
            .post_message("news".to_string(), "first news".to_string())
            .unwrap();
        queue
            .post_message("news".to_string(), "second news".to_string())
            .unwrap();

        let messages = queue.poll_messages("news", None).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].content, "first news");
        assert_eq!(messages[1].content, "second news");
    }

    #[test]
    fn test_poll_messages_with_count_limit() {
        let queue = MessageQueue::new();

        queue
            .post_message("news".to_string(), "first news".to_string())
            .unwrap();
        queue
            .post_message("news".to_string(), "second news".to_string())
            .unwrap();

        let messages = queue.poll_messages("news", Some(1)).unwrap();
        assert_eq!(messages.len(), 1);
    }

    #[test]
    fn test_poll_nonexistent_topic() {
        let queue = MessageQueue::new();
        let poll_result = queue.poll_messages("news", None);
        poll_result.expect_err("Testing: The topic news does not exist!");
    }

    #[test]
    fn test_fifo_ordering() {
        let queue = MessageQueue::new();

        queue
            .post_message("ordered".to_string(), "first".to_string())
            .unwrap();
        queue
            .post_message("ordered".to_string(), "second".to_string())
            .unwrap();
        queue
            .post_message("ordered".to_string(), "third".to_string())
            .unwrap();

        let messages = queue.poll_messages("ordered", None).unwrap();
        assert_eq!(messages[0].content, "first");
        assert_eq!(messages[1].content, "second");
        assert_eq!(messages[2].content, "third");
    }

    #[test]
    fn test_messages_persist_after_polling() {
        let queue = MessageQueue::new();
        queue
            .post_message("news".to_string(), "first news".to_string())
            .unwrap();
        let first_polling_messages = queue.poll_messages("news", None).unwrap();
        let second_polling_messages = queue.poll_messages("news", None).unwrap();
        assert_eq!(first_polling_messages[0], second_polling_messages[0])
    }

    #[test]
    fn test_different_topics_isolated() {
        let queue = MessageQueue::new();

        queue
            .post_message("topic_a".to_string(), "message for A".to_string())
            .unwrap();
        queue
            .post_message("topic_b".to_string(), "message for B".to_string())
            .unwrap();

        let messages_a = queue.poll_messages("topic_a", None).unwrap();
        let messages_b = queue.poll_messages("topic_b", None).unwrap();

        assert_eq!(messages_a.len(), 1);
        assert_eq!(messages_b.len(), 1);
        assert_eq!(messages_a[0].content, "message for A");
        assert_eq!(messages_b[0].content, "message for B");
    }
}
