use anyhow::{Result, anyhow};
use serde_json::Value as JsonValue;
use reqwest::{Client, header};
use std::error::Error;

const TRUETABS_BASE_URL: &str = "https://true.tabs.sale/fusion/v1";

pub async fn update_records(
    api_token: &str,
    datasheet_id: &str,
    field_key: &str,
    updates: Vec<JsonValue>,
) -> Result<JsonValue, Box<dyn Error + Send + Sync>> {
    let client = Client::new();
    let url = format!("{}/datasheets/{}/records", TRUETABS_BASE_URL, datasheet_id);

    let mut body = JsonValue::from(serde_json::Map::new());
    body["records"] = JsonValue::from(updates);
    body["fieldKey"] = JsonValue::String(field_key.to_string());

    let response = client
        .patch(&url)
        .header(header::AUTHORIZATION, format!("Bearer {}", api_token))
        .json(&body)
        .send()
        .await?;

    let status = response.status();
    let response_text = response.text().await?;

    if status.is_success() {
        let json_response: JsonValue = serde_json::from_str(&response_text)
            .map_err(|e| -> Box<dyn Error + Send + Sync> { anyhow!("Failed to parse response JSON: {}. Response body: {}", e, response_text).into() })?;
        Ok(json_response)
    } else {
        Err(anyhow!("API request failed with status: {}. Response body: {}", status, response_text).into())
    }
}