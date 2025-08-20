use crate::MessageQueue;
use std::collections::HashMap;
use std::io::{self, Write};

pub fn run_interactive_demo() {
    let queue = std::sync::Arc::new(MessageQueue::new());
    let mut topics_created: HashMap<String, usize> = HashMap::new();

    println!("🚀 Message Queue Interactive Demo");
    println!("=================================");
    println!("Welcome to the Rust Message Queue demonstration!");
    println!("This interactive demo lets you explore the core library functionality.\n");

    loop {
        println!("\n📋 Menu Options:");
        println!("1) Post a message");
        println!("2) Poll messages from a topic");
        println!("3) View all topics");
        println!("4) Run quick demo");
        println!("5) Exit");
        print!("\nEnter your choice (1-5): ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => match input.trim() {
                "1" => post_message_interactive(&queue, &mut topics_created),
                "2" => poll_messages_interactive(&queue),
                "3" => view_topics(&topics_created),
                "4" => run_demo(&queue, &mut topics_created),
                "5" => break,
                _ => println!("❌ Invalid choice! Please enter a number between 1-5."),
            },
            Err(_) => println!("❌ Error reading input. Please try again."),
        }
    }

    println!("\n👋 Thank you for using the Message Queue demo!");
}

fn post_message_interactive(queue: &MessageQueue, topics_created: &mut HashMap<String, usize>) {
    print!("📝 Enter topic name: ");
    io::stdout().flush().unwrap();
    let mut topic = String::new();
    if io::stdin().read_line(&mut topic).is_err() {
        println!("❌ Error reading topic name.");
        return;
    }
    let topic = topic.trim().to_string();

    if topic.is_empty() {
        println!("❌ Topic name cannot be empty!");
        return;
    }

    print!("💬 Enter message content: ");
    io::stdout().flush().unwrap();
    let mut content = String::new();
    if io::stdin().read_line(&mut content).is_err() {
        println!("❌ Error reading message content.");
        return;
    }
    let content = content.trim().to_string();

    if content.is_empty() {
        println!("❌ Message content cannot be empty!");
        return;
    }

    match queue.post_message(topic.clone(), content.clone()) {
        Ok(message_id) => {
            *topics_created.entry(topic.clone()).or_insert(0) += 1;
            println!("✅ Message posted successfully!");
            println!("   📌 Topic: {topic}");
            println!("   🆔 Message ID: {message_id}");
            println!("   📄 Content: \"{content}\"");
        }
        Err(e) => println!("❌ Failed to post message: {e}"),
    }
}

fn poll_messages_interactive(queue: &MessageQueue) {
    print!("📝 Enter topic name to poll from: ");
    io::stdout().flush().unwrap();
    let mut topic = String::new();
    if io::stdin().read_line(&mut topic).is_err() {
        println!("❌ Error reading topic name.");
        return;
    }
    let topic = topic.trim().to_string();

    if topic.is_empty() {
        println!("❌ Topic name cannot be empty!");
        return;
    }

    print!("🔢 Enter max number of messages (or press Enter for all): ");
    io::stdout().flush().unwrap();
    let mut count_str = String::new();
    if io::stdin().read_line(&mut count_str).is_err() {
        println!("❌ Error reading count.");
        return;
    }

    let count = if count_str.trim().is_empty() {
        None
    } else {
        match count_str.trim().parse::<usize>() {
            Ok(n) if n > 0 => Some(n),
            _ => {
                println!("❌ Invalid count! Using unlimited.");
                None
            }
        }
    };

    match queue.poll_messages(&topic, count) {
        Ok(messages) => {
            if messages.is_empty() {
                println!("📭 No messages found in topic '{topic}'");
            } else {
                println!(
                    "📬 Found {} message(s) in topic '{}':",
                    messages.len(),
                    topic
                );
                println!("─────────────────────────────────────");
                for (i, message) in messages.iter().enumerate() {
                    println!("Message {} of {}:", i + 1, messages.len());
                    println!("  🆔 ID: {}", message.id);
                    println!("  ⏰ Timestamp: {}", message.timestamp);
                    println!("  📄 Content: \"{}\"", message.content);
                    if i < messages.len() - 1 {
                        println!();
                    }
                }
                println!("─────────────────────────────────────");
            }
        }
        Err(e) => println!("❌ Failed to poll messages: {e}"),
    }
}

fn view_topics(topics_created: &HashMap<String, usize>) {
    if topics_created.is_empty() {
        println!("📭 No topics have been created yet.");
        println!("💡 Tip: Use option 1 to post a message and create a topic!");
    } else {
        println!("📋 Topics created in this session:");
        println!("─────────────────────────────────────");
        for (topic, count) in topics_created {
            println!(
                "  📌 {} ({} message{})",
                topic,
                count,
                if *count == 1 { "" } else { "s" }
            );
        }
        println!("─────────────────────────────────────");
        println!("💡 Tip: Use option 2 to poll messages from any topic!");
    }
}

fn run_demo(queue: &MessageQueue, topics_created: &mut HashMap<String, usize>) {
    println!("🎬 Running quick demonstration...");
    println!("─────────────────────────────────────");

    let demo_topic = "demo".to_string();
    let demo_messages = [
        "Hello, World!",
        "This is the second message",
        "Message queue is working great!",
    ];

    println!(
        "📝 Posting {} demo messages to topic '{}'...",
        demo_messages.len(),
        demo_topic
    );

    for (i, content) in demo_messages.iter().enumerate() {
        match queue.post_message(demo_topic.clone(), content.to_string()) {
            Ok(message_id) => {
                println!("  ✅ Message {} posted (ID: {})", i + 1, message_id);
                *topics_created.entry(demo_topic.clone()).or_insert(0) += 1;
            }
            Err(e) => println!("  ❌ Failed to post message {}: {}", i + 1, e),
        }
    }

    println!("\n📬 Polling all messages from topic '{demo_topic}'...");
    match queue.poll_messages(&demo_topic, None) {
        Ok(messages) => {
            println!("📋 Retrieved {} message(s):", messages.len());
            for (i, message) in messages.iter().enumerate() {
                println!(
                    "  {}. \"{}\" (ID: {}, Time: {})",
                    i + 1,
                    message.content,
                    message.id,
                    message.timestamp
                );
            }
        }
        Err(e) => println!("❌ Failed to poll messages: {e}"),
    }

    println!("─────────────────────────────────────");
    println!("🎉 Demo completed! You can now explore the menu options.");
}
