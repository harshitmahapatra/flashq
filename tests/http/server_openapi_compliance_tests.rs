use serde_json::Value;
use super::http_test_helpers::{TestClient, TestServer};

fn load_openapi_spec() -> Value {
    let content = std::fs::read_to_string("docs/openapi.yaml").unwrap();
    serde_yaml::from_str(&content).unwrap()
}

fn get_response_schema(spec: &Value, path: &str, method: &str, status: &str) -> Value {
    spec["paths"][path][method]["responses"][status]["content"]["application/json"]["schema"]
        .clone()
}

fn resolve_schema_ref(spec: &Value, schema: &Value) -> Value {
    if let Some(ref_path) = schema.get("$ref").and_then(|v| v.as_str()) {
        if let Some(component) = ref_path.strip_prefix("#/components/schemas/") {
            return spec["components"]["schemas"][component].clone();
        }
    }
    schema.clone()
}

fn validate_schema(spec: &Value, schema: &Value, data: &Value) -> Result<(), String> {
    let resolved_schema = resolve_schema_ref(spec, schema);

    if let Some(required) = resolved_schema.get("required").and_then(|v| v.as_array()) {
        for field in required {
            let field_name = field.as_str().unwrap();
            if !data.get(field_name).is_some() {
                return Err(format!("Missing required field: {}", field_name));
            }
        }
    }

    if let Some(properties) = resolved_schema
        .get("properties")
        .and_then(|v| v.as_object())
    {
        for (field_name, field_schema) in properties {
            if let Some(field_value) = data.get(field_name) {
                if let Some(field_type) = field_schema.get("type").and_then(|v| v.as_str()) {
                    match field_type {
                        "string" => {
                            if !field_value.is_string() {
                                return Err(format!("Field {} should be string", field_name));
                            }
                        }
                        "integer" => {
                            if !field_value.is_number() {
                                return Err(format!("Field {} should be number", field_name));
                            }
                        }
                        "array" => {
                            if !field_value.is_array() {
                                return Err(format!("Field {} should be array", field_name));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_health_endpoint_compliance() {
    let spec = load_openapi_spec();
    let server = TestServer::start().await.unwrap();
    let helper = TestClient::new(&server);

    let response = helper
        .client
        .get(&format!("{}/health", helper.base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let response_data: Value = response.json().await.unwrap();
    let schema = get_response_schema(&spec, "/health", "get", "200");

    validate_schema(&spec, &schema, &response_data)
        .expect("Health response does not comply with OpenAPI schema");
}

#[tokio::test]
async fn test_produce_records_endpoint_compliance() {
    let spec = load_openapi_spec();
    let server = TestServer::start().await.unwrap();
    let helper = TestClient::new(&server);

    let response = helper
        .post_record_with_record("test_topic", None, "Test record", None)
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let response_data: Value = response.json().await.unwrap();
    let schema = get_response_schema(&spec, "/topics/{topic}/records", "post", "200");

    validate_schema(&spec, &schema, &response_data)
        .expect("Produce response does not comply with OpenAPI schema");
}

#[tokio::test]
async fn test_consumer_group_create_compliance() {
    let spec = load_openapi_spec();
    let server = TestServer::start().await.unwrap();
    let helper = TestClient::new(&server);

    let response = helper.create_consumer_group("test_group").await.unwrap();

    assert_eq!(response.status(), 200);

    let response_data: Value = response.json().await.unwrap();
    let schema = get_response_schema(&spec, "/consumer/{group-id}", "post", "200");

    validate_schema(&spec, &schema, &response_data)
        .expect("Consumer group response does not comply with OpenAPI schema");
}

#[tokio::test]
async fn test_consumer_fetch_records_compliance() {
    let spec = load_openapi_spec();
    let server = TestServer::start().await.unwrap();
    let helper = TestClient::new(&server);

    let group_id = "fetch_compliance_group";
    let topic = "fetch_compliance_topic";

    helper.create_consumer_group(group_id).await.unwrap();
    helper.post_record(topic, "Test record").await.unwrap();

    let response = helper
        .fetch_records_for_consumer_group(group_id, topic, Some(10))
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let response_data: Value = response.json().await.unwrap();
    let schema = get_response_schema(&spec, "/consumer/{group-id}/topics/{topic}", "get", "200");

    validate_schema(&spec, &schema, &response_data)
        .expect("Consumer fetch response does not comply with OpenAPI schema");
}

#[tokio::test]
async fn test_error_response_compliance() {
    let _spec = load_openapi_spec();
    let server = TestServer::start().await.unwrap();
    let helper = TestClient::new(&server);

    let response = helper
        .fetch_records_for_consumer_group("nonexistent_group", "test_topic", None)
        .await
        .unwrap();

    assert!(response.status().is_client_error());

    let response_data: Value = response.json().await.unwrap();

    // Validate error response structure
    assert!(
        response_data.get("error").is_some(),
        "Missing 'error' field"
    );
    assert!(
        response_data.get("message").is_some(),
        "Missing 'message' field"
    );
}
