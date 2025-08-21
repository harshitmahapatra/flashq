use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
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
#[derive(Debug, Clone)]
pub struct TopicLog {
    messages: Vec<Message>,
    next_offset: u64,
}

impl TopicLog {
    pub fn new() -> Self {
        TopicLog {
            messages: Vec::new(),
            next_offset: 0,
        }
    }

    pub fn append(&mut self, content: String, timestamp: u64) -> u64 {
        let current_offset = self.next_offset;
        let message = Message {
            content: content,
            timestamp: timestamp,
            id: current_offset,
        };
        self.messages.push(message);
        self.next_offset += 1;
        current_offset
    }

    pub fn get_messages_from_offset(&self, offset: u64, count: Option<usize>) -> Vec<&Message> {
        let start_index = offset as usize;
        if start_index >= self.messages.len() {
            return Vec::new();
        }
        let slice = &self.messages[start_index..];
        let limited_slice = match count {
            Some(limit) => &slice[..limit.min(slice.len())],
            None => slice,
        };
        limited_slice.iter().collect()
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    group_id: String,
    topic_offsets: HashMap<String, u64>,
}

impl ConsumerGroup {
    pub fn new(group_id: String) -> Self {
        ConsumerGroup {
            group_id: group_id,
            topic_offsets: HashMap::new(),
        }
    }

    pub fn get_offset(&self, topic: &str) -> u64 {
        self.topic_offsets.get(topic).copied().unwrap_or(0)
    }

    pub fn set_offset(&mut self, topic: String, offset: u64) {
        self.topic_offsets.insert(topic, offset);
    }

    pub fn group_id(&self) -> &str {
        &self.group_id
    }
}

pub struct MessageQueue {
    topics: Arc<Mutex<HashMap<String, TopicLog>>>,
    consumer_groups: Arc<Mutex<HashMap<String, ConsumerGroup>>>,
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
            consumer_groups: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn post_message(&self, topic: String, content: String) -> Result<u64, String> {
        let current_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut topic_log_map = self.topics.lock().unwrap();
        let topic_log = topic_log_map.entry(topic).or_insert_with(TopicLog::new);
        Ok(topic_log.append(content, current_timestamp))
    }

    pub fn poll_messages(&self, topic: &str, count: Option<usize>) -> Result<Vec<Message>, String> {
        self.poll_messages_from_offset(topic, 0, count)
    }

    pub fn poll_messages_from_offset(
        &self,
        topic: &str,
        offset: u64,
        count: Option<usize>,
    ) -> Result<Vec<Message>, String> {
        let topic_log_map = self.topics.lock().unwrap();
        match topic_log_map.get(topic) {
            Some(topic_log) => Ok(topic_log
                .get_messages_from_offset(offset, count)
                .into_iter()
                .cloned()
                .collect()),
            None => Err(format!("Topic {topic} does not exist!")),
        }
    }

    pub fn create_consumer_group(&self, group_id: String) -> Result<(), String> {
        let mut consumer_group_map = self.consumer_groups.lock().unwrap();
        match consumer_group_map.entry(group_id.clone()) {
            Vacant(entry) => {
                entry.insert(ConsumerGroup::new(group_id));
                Ok(())
            }
            Occupied(_) => Err(format!(
                "A consumer group already exists for group_id {group_id}"
            )),
        }
    }

    pub fn get_consumer_group_offset(&self, group_id: &str, topic: &str) -> Result<u64, String> {
        let consumer_group_map = self.consumer_groups.lock().unwrap();
        match consumer_group_map.get(group_id) {
            Some(consumer_group) => Ok(consumer_group.get_offset(topic)),
            None => Err(format!("No consumer group exists for group_id {group_id}")),
        }
    }

    pub fn update_consumer_group_offset(
        &self,
        group_id: &str,
        topic: String,
        offset: u64,
    ) -> Result<(), String> {
        let mut consumer_group_map = self.consumer_groups.lock().unwrap();
        match consumer_group_map.get_mut(group_id) {
            Some(consumer_group) => {
                consumer_group.set_offset(topic, offset);
                Ok(())
            }
            None => Err(format!("No consumer group exists for group_id {group_id}")),
        }
    }

    pub fn poll_messages_for_consumer_group(
        &self,
        group_id: &str,
        topic: &str,
        count: Option<usize>,
    ) -> Result<Vec<Message>, String> {
        let current_offset = self.get_consumer_group_offset(group_id, topic)?;
        let messages = self.poll_messages_from_offset(topic, current_offset, count)?;
        let new_offset = current_offset + messages.len() as u64;
        self.update_consumer_group_offset(group_id, topic.to_string(), new_offset)?;
        Ok(messages)
    }
}

pub mod api;

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
        assert_eq!(id3, 0);
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

    // TopicLog Tests
    #[test]
    fn test_topic_log_creation() {
        let log = TopicLog::new();
        assert_eq!(log.len(), 0);
        assert_eq!(log.next_offset(), 0);
    }

    #[test]
    fn test_topic_log_append_single_message() {
        let mut log = TopicLog::new();
        let offset = log.append("first message".to_string(), 1234567890);

        assert_eq!(offset, 0);
        assert_eq!(log.len(), 1);
        assert_eq!(log.next_offset(), 1);
    }

    #[test]
    fn test_topic_log_append_multiple_messages() {
        let mut log = TopicLog::new();
        let offset1 = log.append("message 1".to_string(), 1234567890);
        let offset2 = log.append("message 2".to_string(), 1234567891);
        let offset3 = log.append("message 3".to_string(), 1234567892);

        assert_eq!(offset1, 0);
        assert_eq!(offset2, 1);
        assert_eq!(offset3, 2);
        assert_eq!(log.len(), 3);
        assert_eq!(log.next_offset(), 3);
    }

    #[test]
    fn test_topic_log_get_messages_from_beginning() {
        let mut log = TopicLog::new();
        log.append("first".to_string(), 100);
        log.append("second".to_string(), 200);
        log.append("third".to_string(), 300);

        let messages = log.get_messages_from_offset(0, None);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].content, "first");
        assert_eq!(messages[1].content, "second");
        assert_eq!(messages[2].content, "third");
    }

    #[test]
    fn test_topic_log_get_messages_from_middle_offset() {
        let mut log = TopicLog::new();
        log.append("first".to_string(), 100);
        log.append("second".to_string(), 200);
        log.append("third".to_string(), 300);

        let messages = log.get_messages_from_offset(1, None);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].content, "second");
        assert_eq!(messages[1].content, "third");
    }

    #[test]
    fn test_topic_log_get_messages_with_count_limit() {
        let mut log = TopicLog::new();
        log.append("first".to_string(), 100);
        log.append("second".to_string(), 200);
        log.append("third".to_string(), 300);

        let messages = log.get_messages_from_offset(0, Some(2));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].content, "first");
        assert_eq!(messages[1].content, "second");
    }

    #[test]
    fn test_topic_log_get_messages_beyond_log() {
        let mut log = TopicLog::new();
        log.append("only message".to_string(), 100);

        let messages = log.get_messages_from_offset(5, None);
        assert_eq!(messages.len(), 0);
    }

    #[test]
    fn test_topic_log_message_ids_match_offsets() {
        let mut log = TopicLog::new();
        let offset1 = log.append("msg1".to_string(), 100);
        let offset2 = log.append("msg2".to_string(), 200);

        let messages = log.get_messages_from_offset(0, None);
        assert_eq!(messages[0].id, offset1);
        assert_eq!(messages[1].id, offset2);
    }

    // ConsumerGroup Tests
    #[test]
    fn test_consumer_group_creation() {
        let group = ConsumerGroup::new("test-group".to_string());
        assert_eq!(group.group_id(), "test-group");
        assert_eq!(group.get_offset("any-topic"), 0);
    }

    #[test]
    fn test_consumer_group_set_and_get_offset() {
        let mut group = ConsumerGroup::new("test-group".to_string());

        group.set_offset("topic1".to_string(), 5);
        group.set_offset("topic2".to_string(), 10);

        assert_eq!(group.get_offset("topic1"), 5);
        assert_eq!(group.get_offset("topic2"), 10);
        assert_eq!(group.get_offset("nonexistent"), 0);
    }

    #[test]
    fn test_consumer_group_update_offset() {
        let mut group = ConsumerGroup::new("test-group".to_string());

        group.set_offset("topic".to_string(), 3);
        assert_eq!(group.get_offset("topic"), 3);

        group.set_offset("topic".to_string(), 8);
        assert_eq!(group.get_offset("topic"), 8);
    }

    #[test]
    fn test_consumer_group_multiple_topics() {
        let mut group = ConsumerGroup::new("multi-topic-group".to_string());

        group.set_offset("news".to_string(), 15);
        group.set_offset("alerts".to_string(), 7);
        group.set_offset("logs".to_string(), 42);

        assert_eq!(group.get_offset("news"), 15);
        assert_eq!(group.get_offset("alerts"), 7);
        assert_eq!(group.get_offset("logs"), 42);
        assert_eq!(group.get_offset("unknown"), 0);
    }
}
