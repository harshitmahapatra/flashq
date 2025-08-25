use crate::{FlashQ, Record};
use std::collections::HashMap;
use std::io::{self, Write};

pub fn run_interactive_demo() {
    let queue = std::sync::Arc::new(FlashQ::new());
    let mut topics_created: HashMap<String, usize> = HashMap::new();

    println!("⚡ FlashQ Interactive Demo");
    println!("=================================");
    println!("Welcome to the ⚡ FlashQ demonstration!");
    println!("This interactive demo lets you explore the core library functionality.\n");

    loop {
        println!("\n📋 Menu Options:");
        println!("1) Post a record");
        println!("2) Poll records from a topic");
        println!("3) View all topics");
        println!("4) Run quick demo");
        println!("5) Exit");
        print!("\nEnter your choice (1-5): ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => match input.trim() {
                "1" => post_record_interactive(&queue, &mut topics_created),
                "2" => poll_records_interactive(&queue),
                "3" => view_topics(&topics_created),
                "4" => run_demo(&queue, &mut topics_created),
                "5" => break,
                _ => println!("❌ Invalid choice! Please enter a number between 1-5."),
            },
            Err(_) => println!("❌ Error reading input. Please try again."),
        }
    }

    println!("\n👋 Thank you for using the FlashQ demo!");
}

fn post_record_interactive(queue: &FlashQ, topics_created: &mut HashMap<String, usize>) {
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

    print!("💬 Enter record content: ");
    io::stdout().flush().unwrap();
    let mut content = String::new();
    if io::stdin().read_line(&mut content).is_err() {
        println!("❌ Error reading record content.");
        return;
    }
    let content = content.trim().to_string();

    if content.is_empty() {
        println!("❌ Record content cannot be empty!");
        return;
    }

    let record = Record {
        key: None,
        value: content.clone(),
        headers: None,
    };

    match queue.post_record(topic.clone(), record) {
        Ok(record_id) => {
            *topics_created.entry(topic.clone()).or_insert(0) += 1;
            println!("✅ Record posted successfully!");
            println!("   📌 Topic: {topic}");
            println!("   🆔 Record ID: {record_id}");
            println!("   📄 Content: \"{content}\"");
        }
        Err(e) => println!("❌ Failed to post record: {e}"),
    }
}

fn poll_records_interactive(queue: &FlashQ) {
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

    print!("🔢 Enter max number of records (or press Enter for all): ");
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

    match queue.poll_records(&topic, count) {
        Ok(records) => {
            if records.is_empty() {
                println!("📭 No records found in topic '{topic}'");
            } else {
                println!("📬 Found {} record(s) in topic '{}':", records.len(), topic);
                println!("─────────────────────────────────────");
                for (i, record) in records.iter().enumerate() {
                    println!("Record {} of {}:", i + 1, records.len());
                    println!("  🏷️  Offset: {}", record.offset);

                    // Display key if present
                    if let Some(ref key) = record.record.key {
                        println!("  🔑 Key: \"{key}\"");
                    }

                    println!("  📄 Value: \"{}\"", record.record.value);

                    // Display headers if present
                    if let Some(ref headers) = record.record.headers
                        && !headers.is_empty()
                    {
                        println!("  🏷️  Headers:");
                        for (header_key, header_value) in headers {
                            println!("    {header_key}: \"{header_value}\"");
                        }
                    }

                    println!("  ⏰ Timestamp: {}", record.timestamp);
                    if i < records.len() - 1 {
                        println!();
                    }
                }
                println!("─────────────────────────────────────");
            }
        }
        Err(e) => println!("❌ Failed to poll records: {e}"),
    }
}

fn view_topics(topics_created: &HashMap<String, usize>) {
    if topics_created.is_empty() {
        println!("📭 No topics have been created yet.");
        println!("💡 Tip: Use option 1 to post a record and create a topic!");
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

fn run_demo(queue: &FlashQ, topics_created: &mut HashMap<String, usize>) {
    println!("🎬 Running quick demonstration...");
    println!("─────────────────────────────────────");

    let demo_topic = "demo".to_string();
    let demo_records = [
        "Hello, World!",
        "This is the second record",
        "FlashQ is working great!",
    ];

    println!(
        "📝 Posting {} demo records to topic '{}'...",
        demo_records.len(),
        demo_topic
    );

    for (i, content) in demo_records.iter().enumerate() {
        let record = Record {
            key: None,
            value: content.to_string(),
            headers: None,
        };
        match queue.post_record(demo_topic.clone(), record) {
            Ok(record_id) => {
                println!("  ✅ Record {} posted (ID: {})", i + 1, record_id);
                *topics_created.entry(demo_topic.clone()).or_insert(0) += 1;
            }
            Err(e) => println!("  ❌ Failed to post record {}: {}", i + 1, e),
        }
    }

    println!("\n📬 Polling all records from topic '{demo_topic}'...");
    match queue.poll_records(&demo_topic, None) {
        Ok(records) => {
            println!("📋 Retrieved {} record(s):", records.len());
            for (i, record) in records.iter().enumerate() {
                println!(
                    "  {}. \"{}\" (Offset: {}, Time: {})",
                    i + 1,
                    record.record.value,
                    record.offset,
                    record.timestamp
                );
            }
        }
        Err(e) => println!("❌ Failed to poll records: {e}"),
    }

    println!("─────────────────────────────────────");
    println!("🎉 Demo completed! You can now explore the menu options.");
}
