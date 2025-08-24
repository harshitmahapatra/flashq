use flashq::Record;
use flashq::api::*;
use std::env;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, Once};
use std::time::Duration;
use tokio::process::Command as TokioCommand;
use tokio::time::sleep;

static SERVER_INIT: Once = Once::new();
static CLIENT_INIT: Once = Once::new();
static SERVER_BINARY_PATH: Mutex<Option<PathBuf>> = Mutex::new(None);
static CLIENT_BINARY_PATH: Mutex<Option<PathBuf>> = Mutex::new(None);

pub fn find_available_port() -> Result<u16, Box<dyn std::error::Error>> {
    // Bind to port 0 to let the OS choose an available port
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    Ok(addr.port())
}

pub fn ensure_server_binary() -> Result<PathBuf, Box<dyn std::error::Error>> {
    SERVER_INIT.call_once(|| {
        eprintln!("Building server binary for integration tests...");
        let output = Command::new("cargo")
            .args(["build", "--bin", "server"])
            .output()
            .expect("Failed to build server binary");

        if !output.status.success() {
            panic!(
                "Failed to build server binary: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let binary_path = PathBuf::from("target/debug/server");
        *SERVER_BINARY_PATH.lock().unwrap() = Some(binary_path);
        eprintln!("Server binary built successfully");
    });

    let binary_path = SERVER_BINARY_PATH
        .lock()
        .unwrap()
        .as_ref()
        .expect("Server binary path should be set")
        .clone();

    Ok(binary_path)
}

pub fn ensure_client_binary() -> Result<PathBuf, Box<dyn std::error::Error>> {
    CLIENT_INIT.call_once(|| {
        eprintln!("Building client binary for integration tests...");
        let output = Command::new("cargo")
            .args(["build", "--bin", "client"])
            .output()
            .expect("Failed to build client binary");

        if !output.status.success() {
            panic!(
                "Failed to build client binary: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let binary_path = PathBuf::from("target/debug/client");
        *CLIENT_BINARY_PATH.lock().unwrap() = Some(binary_path);
        eprintln!("Client binary built successfully");
    });

    let binary_path = CLIENT_BINARY_PATH
        .lock()
        .unwrap()
        .as_ref()
        .expect("Client binary path should be set")
        .clone();

    Ok(binary_path)
}

pub fn get_timeout_config() -> (u32, u64) {
    // Returns (max_attempts, sleep_ms)
    if env::var("CI").is_ok() {
        eprintln!("Running in CI environment - using extended timeouts");
        (60, 500) // 30 seconds total in CI
    } else {
        (30, 500) // 15 seconds locally
    }
}

pub struct TestServer {
    process: Child,
    pub port: u16,
}

impl TestServer {
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let port = find_available_port()?;
        let server_binary = ensure_server_binary()?;
        let (max_attempts, sleep_ms) = get_timeout_config();

        eprintln!("Starting server on port {port} using binary: {server_binary:?}");

        let mut process = Command::new(&server_binary)
            .args([&port.to_string()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        // Wait for server to start and verify it's responding
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        let health_url = format!("http://127.0.0.1:{port}/health");

        for attempt in 0..max_attempts {
            sleep(Duration::from_millis(sleep_ms)).await;

            // Check if process is still running
            if let Ok(Some(exit_status)) = process.try_wait() {
                eprintln!("Server process exited with status: {exit_status}");
                return Err("Server process exited".into());
            }

            // Try to connect to health endpoint with retry logic
            match Self::try_health_check(&client, &health_url, attempt + 1).await {
                Ok(true) => {
                    eprintln!(
                        "Server started successfully on port {} after {} attempts",
                        port,
                        attempt + 1
                    );
                    return Ok(TestServer { process, port });
                }
                Ok(false) => continue, // Retry
                Err(e) => {
                    eprintln!("Health check attempt {} failed: {}", attempt + 1, e);
                    continue;
                }
            }
        }

        eprintln!("Server failed to start within {max_attempts} attempts");
        let _ = process.kill();
        Err("Server failed to start within timeout".into())
    }

    async fn try_health_check(
        client: &reqwest::Client,
        health_url: &str,
        attempt: u32,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        // Exponential backoff for network retries within each attempt
        for retry in 0..3 {
            let backoff_ms = 100 * (2_u64.pow(retry));
            if retry > 0 {
                sleep(Duration::from_millis(backoff_ms)).await;
            }

            match client.get(health_url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        return Ok(true);
                    } else {
                        eprintln!(
                            "Health check attempt {}.{}: HTTP {}",
                            attempt,
                            retry + 1,
                            response.status()
                        );
                    }
                }
                Err(e) if retry < 2 => {
                    eprintln!(
                        "Health check attempt {}.{} failed, retrying: {}",
                        attempt,
                        retry + 1,
                        e
                    );
                    continue;
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        Ok(false) // All retries failed
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

// Helper struct for common test operations
pub struct TestHelper {
    pub client: reqwest::Client,
    pub base_url: String,
}

#[allow(dead_code)]
impl TestHelper {
    pub fn new(server: &TestServer) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: format!("http://127.0.0.1:{}", server.port),
        }
    }

    // Message operations - updated for new Record structure
    pub async fn post_message(
        &self,
        topic: &str,
        content: &str,
    ) -> reqwest::Result<reqwest::Response> {
        self.post_message_with_record(topic, None, content, None)
            .await
    }

    pub async fn post_message_with_record(
        &self,
        topic: &str,
        key: Option<String>,
        value: &str,
        headers: Option<std::collections::HashMap<String, String>>,
    ) -> reqwest::Result<reqwest::Response> {
        // Convert single record to batch format for new API
        let message_record = Record {
            key,
            value: value.to_string(),
            headers,
        };

        let produce_request = ProduceRequest {
            records: vec![message_record],
        };

        self.client
            .post(format!("{}/topics/{}/records", self.base_url, topic))
            .json(&produce_request)
            .send()
            .await
    }

    pub async fn post_batch_messages(
        &self,
        topic: &str,
        records: Vec<Record>,
    ) -> reqwest::Result<reqwest::Response> {
        let produce_request = ProduceRequest { records };

        self.client
            .post(format!("{}/topics/{}/records", self.base_url, topic))
            .json(&produce_request)
            .send()
            .await
    }

    // Note: Direct topic polling has been removed to align with OpenAPI spec.
    // Use consumer group endpoints for message consumption.

    // Basic polling for testing - creates temporary consumer group
    pub async fn poll_messages_for_testing(
        &self,
        topic: &str,
        count: Option<usize>,
    ) -> reqwest::Result<reqwest::Response> {
        // Create a unique temporary consumer group for this poll operation
        let temp_group_id = format!("test_poll_{}", std::process::id());

        // Create consumer group
        let _ = self.create_consumer_group(&temp_group_id).await;

        // Fetch messages
        let response = self
            .fetch_messages_for_consumer_group(&temp_group_id, topic, count)
            .await;

        // Clean up consumer group
        let _ = self.leave_consumer_group(&temp_group_id).await;

        response
    }

    // Consumer group operations
    pub async fn create_consumer_group(
        &self,
        group_id: &str,
    ) -> reqwest::Result<reqwest::Response> {
        self.client
            .post(format!("{}/consumer/{}", self.base_url, group_id))
            .send()
            .await
    }

    pub async fn update_consumer_group_offset(
        &self,
        group_id: &str,
        topic: &str,
        offset: u64,
    ) -> reqwest::Result<reqwest::Response> {
        self.client
            .post(format!(
                "{}/consumer/{}/topics/{}/offset",
                self.base_url, group_id, topic
            ))
            .json(&UpdateConsumerGroupOffsetRequest { offset })
            .send()
            .await
    }

    pub async fn get_consumer_group_offset(
        &self,
        group_id: &str,
        topic: &str,
    ) -> reqwest::Result<reqwest::Response> {
        self.client
            .get(format!(
                "{}/consumer/{}/topics/{}/offset",
                self.base_url, group_id, topic
            ))
            .send()
            .await
    }

    pub async fn leave_consumer_group(&self, group_id: &str) -> reqwest::Result<reqwest::Response> {
        self.client
            .delete(format!("{}/consumer/{}", self.base_url, group_id))
            .send()
            .await
    }

    pub async fn fetch_messages_for_consumer_group(
        &self,
        group_id: &str,
        topic: &str,
        count: Option<usize>,
    ) -> reqwest::Result<reqwest::Response> {
        let mut request = self.client.get(format!(
            "{}/consumer/{}/topics/{}",
            self.base_url, group_id, topic
        ));
        if let Some(c) = count {
            request = request.query(&[("count", c.to_string())]);
        }
        request.send().await
    }

    pub async fn fetch_messages_for_consumer_group_from_offset(
        &self,
        group_id: &str,
        topic: &str,
        from_offset: u64,
        count: Option<usize>,
    ) -> reqwest::Result<reqwest::Response> {
        let mut query = vec![("from_offset", from_offset.to_string())];
        if let Some(c) = count {
            query.push(("count", c.to_string()));
        }

        self.client
            .get(format!(
                "{}/consumer/{}/topics/{}",
                self.base_url, group_id, topic
            ))
            .query(&query)
            .send()
            .await
    }

    pub async fn assert_poll_response(
        &self,
        response: reqwest::Response,
        expected_count: usize,
        expected_values: Option<&[&str]>,
    ) -> FetchResponse {
        assert_eq!(response.status(), 200);
        let poll_data: FetchResponse = response.json().await.unwrap();
        assert_eq!(poll_data.records.len(), expected_count);

        // Verify high water mark is reasonable (should be >= next_offset)
        assert!(poll_data.high_water_mark >= poll_data.next_offset);

        // Verify lag calculation if present
        if let Some(lag) = poll_data.lag {
            assert_eq!(lag, poll_data.high_water_mark - poll_data.next_offset);
        }

        if let Some(expected) = expected_values {
            for (i, expected_value) in expected.iter().enumerate() {
                assert_eq!(poll_data.records[i].record.value, *expected_value);
                // Verify timestamp is in ISO 8601 format
                assert!(poll_data.records[i].timestamp.contains("T"));
                // Verify offset sequence
                assert_eq!(poll_data.records[i].offset, i as u64);
            }
        }

        poll_data
    }

    pub async fn health_check(&self) -> reqwest::Result<reqwest::Response> {
        self.client
            .get(format!("{}/health", self.base_url))
            .send()
            .await
    }

    pub async fn post_message_with_client(
        &self,
        topic: &str,
        message: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client_binary = ensure_client_binary()?;
        let port = self.base_url.split(':').last().unwrap();

        let output = TokioCommand::new(&client_binary)
            .args(["--port", port, "post", topic, message])
            .output()
            .await?;

        if !output.status.success() {
            return Err(format!(
                "Client post failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(())
    }

    pub async fn poll_messages_with_client(
        &self,
        topic: &str,
        count: Option<usize>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let client_binary = ensure_client_binary()?;
        let port = self.base_url.split(':').last().unwrap();

        let mut args = vec![
            "--port".to_string(),
            port.to_string(),
            "poll".to_string(),
            topic.to_string(),
        ];
        if let Some(c) = count {
            args.push("--count".to_string());
            args.push(c.to_string());
        }

        let output = TokioCommand::new(&client_binary)
            .args(&args)
            .output()
            .await?;

        if !output.status.success() {
            return Err(format!(
                "Client poll failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    pub async fn create_consumer_group_with_client(
        &self,
        group_id: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let client_binary = ensure_client_binary()?;
        let port = self.base_url.split(':').last().unwrap();

        let output = TokioCommand::new(&client_binary)
            .args(["--port", port, "consumer-group", "create", group_id])
            .output()
            .await?;

        if !output.status.success() {
            return Err(format!(
                "Client consumer group create failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    pub async fn leave_consumer_group_with_client(
        &self,
        group_id: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let client_binary = ensure_client_binary()?;
        let port = self.base_url.split(':').last().unwrap();

        let output = TokioCommand::new(&client_binary)
            .args(["--port", port, "consumer-group", "leave", group_id])
            .output()
            .await?;

        if !output.status.success() {
            return Err(format!(
                "Client consumer group leave failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    pub async fn poll_consumer_group_with_client(
        &self,
        group_id: &str,
        topic: &str,
        count: Option<usize>,
        from_offset: Option<u64>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let client_binary = ensure_client_binary()?;
        let port = self.base_url.split(':').last().unwrap();

        let mut args = vec![
            "--port".to_string(),
            port.to_string(),
            "consumer-group".to_string(),
            "poll".to_string(),
            group_id.to_string(),
            topic.to_string(),
        ];

        if let Some(c) = count {
            args.push("--count".to_string());
            args.push(c.to_string());
        }
        if let Some(offset) = from_offset {
            args.push("--from-offset".to_string());
            args.push(offset.to_string());
        }

        let output = TokioCommand::new(&client_binary)
            .args(&args)
            .output()
            .await?;

        if !output.status.success() {
            return Err(format!(
                "Client consumer group poll failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    pub async fn get_consumer_group_offset_with_client(
        &self,
        group_id: &str,
        topic: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let client_binary = ensure_client_binary()?;
        let port = self.base_url.split(':').last().unwrap();

        let output = TokioCommand::new(&client_binary)
            .args([
                "--port", port,
                "consumer-group", "offset",
                group_id, topic, "--get"
            ])
            .output()
            .await?;

        if !output.status.success() {
            return Err(format!(
                "Client get consumer group offset failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    pub async fn set_consumer_group_offset_with_client(
        &self,
        group_id: &str,
        topic: &str,
        offset: u64,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let client_binary = ensure_client_binary()?;
        let port = self.base_url.split(':').last().unwrap();

        let output = TokioCommand::new(&client_binary)
            .args([
                "--port", port,
                "consumer-group", "offset",
                group_id, topic,
                "--set", &offset.to_string()
            ])
            .output()
            .await?;

        if !output.status.success() {
            return Err(format!(
                "Client set consumer group offset failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    pub async fn health_check_with_client(&self) -> Result<String, Box<dyn std::error::Error>> {
        let client_binary = ensure_client_binary()?;
        let port = self.base_url.split(':').last().unwrap();

        let output = TokioCommand::new(&client_binary)
            .args(["--port", port, "health"])
            .output()
            .await?;

        if !output.status.success() {
            return Err(format!(
                "Client health check failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    pub async fn post_batch_messages_with_client(
        &self,
        topic: &str,
        batch_file_path: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let client_binary = ensure_client_binary()?;
        let port = self.base_url.split(':').last().unwrap();

        let output = TokioCommand::new(&client_binary)
            .args(["--port", port, "post", topic, "--batch", batch_file_path])
            .output()
            .await?;

        if !output.status.success() {
            return Err(format!(
                "Client batch post failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    pub async fn post_message_with_key_headers_client(
        &self,
        topic: &str,
        message: &str,
        key: Option<&str>,
        headers: Option<&[(&str, &str)]>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let client_binary = ensure_client_binary()?;
        let port = self.base_url.split(':').last().unwrap();

        let mut args = vec![
            "--port".to_string(),
            port.to_string(),
            "post".to_string(),
            topic.to_string(),
            message.to_string(),
        ];

        if let Some(k) = key {
            args.push("--key".to_string());
            args.push(k.to_string());
        }

        if let Some(header_pairs) = headers {
            for (k, v) in header_pairs {
                args.push("--header".to_string());
                args.push(format!("{}={}", k, v));
            }
        }

        let output = TokioCommand::new(&client_binary)
            .args(&args)
            .output()
            .await?;

        if !output.status.success() {
            return Err(format!(
                "Client post with key/headers failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    pub async fn poll_messages_from_offset_with_client(
        &self,
        topic: &str,
        from_offset: u64,
        count: Option<usize>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let client_binary = ensure_client_binary()?;
        let port = self.base_url.split(':').last().unwrap();

        let mut args = vec![
            "--port".to_string(),
            port.to_string(),
            "poll".to_string(),
            topic.to_string(),
            "--from-offset".to_string(),
            from_offset.to_string(),
        ];

        if let Some(c) = count {
            args.push("--count".to_string());
            args.push(c.to_string());
        }

        let output = TokioCommand::new(&client_binary)
            .args(&args)
            .output()
            .await?;

        if !output.status.success() {
            return Err(format!(
                "Client poll from offset failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}
