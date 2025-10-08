use clap::{Args, Parser, Subcommand};
use flashq_client::FlashqClient;
use flashq_proto::flashq::v1 as proto;
use std::collections::HashMap;

#[derive(Parser, Debug)]
#[command(name = "flashq-client", version, author, about = "FlashQ client")]
struct Cli {
    /// Server address, e.g. http://127.0.0.1:50051
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    addr: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Test connectivity (establish channel)
    Connect,
    /// Produce one or more records to a topic
    Produce(ProduceCmd),
    /// Create a consumer group
    CreateGroup(GroupCmd),
    /// Delete a consumer group
    DeleteGroup(GroupCmd),
    /// Fetch by offset
    FetchOffset(FetchOffsetCmd),
    /// Fetch by time (RFC3339)
    FetchTime(FetchTimeCmd),
    /// Commit offset for a group/topic
    CommitOffset(CommitOffsetCmd),
    /// Get committed offset for a group/topic
    GetOffset(GetOffsetCmd),
    /// List topics
    ListTopics,
    /// Get topic high water mark
    HighWaterMark(HighWaterMarkCmd),
    /// Subscribe and print records continuously
    Subscribe(SubscribeCmd),
}

#[derive(Args, Debug)]
struct ProduceCmd {
    #[arg(long)]
    topic: String,
    /// Record value(s). Repeat to send multiple records.
    #[arg(long, required = true)]
    value: Vec<String>,
    /// Optional key applied to all records
    #[arg(long)]
    key: Option<String>,
    /// Optional headers KEY=VALUE (repeatable)
    #[arg(long = "header")]
    headers: Vec<String>,
}

#[derive(Args, Debug)]
struct GroupCmd {
    #[arg(long, value_name = "GROUP_ID")]
    group_id: String,
}

#[derive(Args, Debug)]
struct FetchOffsetCmd {
    #[arg(long)]
    group_id: String,
    #[arg(long)]
    topic: String,
    /// Start offset (0 = use committed)
    #[arg(long, default_value_t = 0)]
    from_offset: u64,
    #[arg(long, default_value_t = 100)]
    max_records: u32,
    #[arg(long, default_value_t = true)]
    include_headers: bool,
}

#[derive(Args, Debug)]
struct FetchTimeCmd {
    #[arg(long)]
    group_id: String,
    #[arg(long)]
    topic: String,
    /// RFC3339 timestamp
    #[arg(long)]
    from_time: String,
    #[arg(long, default_value_t = 100)]
    max_records: u32,
    #[arg(long, default_value_t = true)]
    include_headers: bool,
}

#[derive(Args, Debug)]
struct CommitOffsetCmd {
    #[arg(long)]
    group_id: String,
    #[arg(long)]
    topic: String,
    #[arg(long)]
    offset: u64,
}

#[derive(Args, Debug)]
struct GetOffsetCmd {
    #[arg(long)]
    group_id: String,
    #[arg(long)]
    topic: String,
}

#[derive(Args, Debug)]
struct HighWaterMarkCmd {
    #[arg(long)]
    topic: String,
}

#[derive(Args, Debug)]
struct SubscribeCmd {
    #[arg(long)]
    group_id: String,
    #[arg(long)]
    topic: String,
    /// Start offset (0 = use committed)
    #[arg(long, default_value_t = 0)]
    from_offset: u64,
    #[arg(long, default_value_t = true)]
    include_headers: bool,
}

fn parse_headers(pairs: &[String]) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for p in pairs {
        if let Some((k, v)) = p.split_once('=') {
            out.insert(k.to_string(), v.to_string());
        }
    }
    out
}

fn print_record(r: &proto::RecordWithOffset) {
    let ts = &r.timestamp;
    let offset = r.offset;
    if let Some(ref rec) = r.record {
        print!("{} [{}] {}", ts, offset, rec.value);
        if !rec.key.is_empty() {
            print!(" (key: {})", rec.key);
        }
        if !rec.headers.is_empty() {
            print!(" (headers: {:?})", rec.headers);
        }
        println!();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Connect => {
            let _clients = FlashqClient::connect(cli.addr).await?;
            println!("connected");
        }
        Commands::Produce(args) => {
            let clients = FlashqClient::connect(cli.addr.clone()).await?;
            let mut producer = clients.producer();
            let headers = parse_headers(&args.headers);
            let mut records = Vec::with_capacity(args.value.len());
            for v in args.value {
                records.push(proto::Record {
                    key: args.key.clone().unwrap_or_default(),
                    value: v,
                    headers: headers.clone(),
                });
            }
            let req = proto::ProduceRequest {
                topic: args.topic,
                records,
            };
            let resp = producer.produce(req).await?.into_inner();
            println!("offset: {}\ntimestamp: {}", resp.offset, resp.timestamp);
        }
        Commands::CreateGroup(args) => {
            let clients = FlashqClient::connect(cli.addr.clone()).await?;
            let mut consumer = clients.consumer();
            let req = proto::ConsumerGroupId {
                group_id: args.group_id,
            };
            let resp = consumer.create_consumer_group(req).await?.into_inner();
            println!("group_id: {}", resp.group_id);
        }
        Commands::DeleteGroup(args) => {
            let clients = FlashqClient::connect(cli.addr.clone()).await?;
            let mut consumer = clients.consumer();
            let req = proto::ConsumerGroupId {
                group_id: args.group_id,
            };
            let _ = consumer.delete_consumer_group(req).await?.into_inner();
            println!("deleted");
        }
        Commands::FetchOffset(args) => {
            let clients = FlashqClient::connect(cli.addr.clone()).await?;
            let mut consumer = clients.consumer();
            let req = proto::FetchByOffsetRequest {
                group_id: args.group_id,
                topic: args.topic,
                from_offset: args.from_offset,
                max_records: args.max_records,
                include_headers: args.include_headers,
            };
            let resp = consumer.fetch_by_offset(req).await?.into_inner();
            for r in &resp.records {
                print_record(r);
            }
            println!(
                "next_offset: {}\nhigh_water_mark: {}\nlag: {}",
                resp.next_offset, resp.high_water_mark, resp.lag
            );
        }
        Commands::FetchTime(args) => {
            let clients = FlashqClient::connect(cli.addr.clone()).await?;
            let mut consumer = clients.consumer();
            let req = proto::FetchByTimeRequest {
                group_id: args.group_id,
                topic: args.topic,
                from_time: args.from_time,
                max_records: args.max_records,
                include_headers: args.include_headers,
            };
            let resp = consumer.fetch_by_time(req).await?.into_inner();
            for r in &resp.records {
                print_record(r);
            }
            println!(
                "next_offset: {}\nhigh_water_mark: {}\nlag: {}",
                resp.next_offset, resp.high_water_mark, resp.lag
            );
        }
        Commands::CommitOffset(args) => {
            let clients = FlashqClient::connect(cli.addr.clone()).await?;
            let mut consumer = clients.consumer();
            let req = proto::CommitOffsetRequest {
                group_id: args.group_id,
                topic: args.topic,
                offset: args.offset,
            };
            let resp = consumer.commit_offset(req).await?.into_inner();
            println!(
                "topic: {}\ncommitted_offset: {}\ntimestamp: {}",
                resp.topic, resp.committed_offset, resp.timestamp
            );
        }
        Commands::GetOffset(args) => {
            let clients = FlashqClient::connect(cli.addr.clone()).await?;
            let mut consumer = clients.consumer();
            let req = proto::GetOffsetRequest {
                group_id: args.group_id,
                topic: args.topic,
            };
            let resp = consumer.get_consumer_group_offset(req).await?.into_inner();
            println!(
                "group_id: {}\ntopic: {}\noffset: {}",
                resp.group_id, resp.topic, resp.offset
            );
        }
        Commands::ListTopics => {
            let clients = FlashqClient::connect(cli.addr.clone()).await?;
            let mut admin = clients.admin();
            let resp = admin.list_topics(proto::Empty {}).await?.into_inner();
            for t in resp.topics {
                println!("{t}");
            }
        }
        Commands::HighWaterMark(args) => {
            let clients = FlashqClient::connect(cli.addr.clone()).await?;
            let mut admin = clients.admin();
            let req = proto::HighWaterMarkRequest { topic: args.topic };
            let resp = admin.high_water_mark(req).await?.into_inner();
            println!(
                "topic: {}\nhigh_water_mark: {}",
                resp.topic, resp.high_water_mark
            );
        }
        Commands::Subscribe(args) => {
            let clients = FlashqClient::connect(cli.addr.clone()).await?;
            let mut consumer = clients.consumer();
            let req = proto::FetchByOffsetRequest {
                group_id: args.group_id,
                topic: args.topic,
                from_offset: args.from_offset,
                max_records: 100,
                include_headers: args.include_headers,
            };
            let mut stream = consumer.subscribe(req).await?.into_inner();
            while let Some(item) = stream.message().await? {
                print_record(&item);
            }
        }
    }
    Ok(())
}
