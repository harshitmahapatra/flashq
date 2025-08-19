use message_queue_rs::MessageQueue;

fn main() {
    let queue = MessageQueue::new();

    println!("Message Queue started!");
    println!("Run 'cargo test' to execute unit tests.");

    // Example usage
    queue
        .post_message("demo".to_string(), "Hello, World!".to_string())
        .ok();
    if let Ok(messages) = queue.poll_messages("demo", Some(1)) {
        println!("Demo message: {}", messages[0].content);
    }
}

