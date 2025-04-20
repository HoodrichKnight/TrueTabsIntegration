use anyhow::{Result, anyhow};
use std::collections::HashMap;
use futures::TryStreamExt;

use mongodb::{Client as MongoClient, bson::{Document, Bson}, options::ClientOptions};
use std::error::Error;

use redis::{Client as RedisClient, AsyncCommands};

use elasticsearch::{Elasticsearch, http::transport::{Transport}};
use serde_json::{Value as JsonValue};

use crate::db::ExtractedData;

pub async fn extract_from_mongodb(uri: &str, db_name: &str, collection_name: &str, mut expected_headers: Option<Vec<String>>) -> Result<ExtractedData, Box<dyn Error + Send + Sync>> {
    println!("Подключение к MongoDB...");
    let client_options = ClientOptions::parse(uri).await?;
    let client = MongoClient::with_options(client_options)?;
    println!("Подключение к MongoDB успешно установлено.");

    let db = client.database(db_name);
    let collection = db.collection::<Document>(collection_name);

    println!("Извлечение из коллекции '{}' в БД '{}'...", collection_name, db_name);

    let mut cursor = collection.find(None, None).await?;

    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();
    let mut headers_extracted = false;
    let mut actual_headers: Vec<String> = Vec::new();

    while let Some(doc) = cursor.try_next().await? {
        if !headers_extracted {
            actual_headers = doc.keys().map(|key| key.to_string()).collect();
            actual_headers.sort();
            headers = actual_headers.clone();
            headers_extracted = true;

            if let Some(ref mut expected) = expected_headers {
                expected.sort();

                if actual_headers != *expected {
                    let expected_str = expected.join(", ");
                    let actual_str = actual_headers.join(", ");
                    let error_msg = format!("Column mismatch: Expected [{}], Got [{}]", expected_str, actual_str);
                    return Err(anyhow!(error_msg).into());
                }
            }
        }

        let mut current_row_data: Vec<String> = Vec::new();
        for header in &headers {
            let string_value = match doc.get(header) {
                Some(Bson::String(s)) => s.clone(),
                Some(Bson::Int32(i)) => i.to_string(),
                Some(Bson::Int64(i)) => i.to_string(),
                Some(Bson::Double(d)) => d.to_string(),
                Some(Bson::Boolean(b)) => b.to_string(),
                Some(Bson::DateTime(dt)) => dt.to_string(),
                Some(Bson::ObjectId(oid)) => oid.to_string(),
                Some(Bson::Decimal128(d)) => d.to_string(),
                Some(Bson::Null) => "".to_string(),
                Some(Bson::Array(arr)) => format!("{:?}", arr),
                Some(Bson::Document(doc_val)) => format!("{:?}", doc_val),
                _ => "".to_string(),
            };
            current_row_data.push(string_value);
        }
        data_rows.push(current_row_data);
    }

    println!("Извлечение из MongoDB успешно. Извлечено {} строк.", data_rows.len());
    Ok(ExtractedData { headers: actual_headers, rows: data_rows })
}

pub async fn extract_from_redis(url: &str, key_pattern: &str, mut expected_headers: Option<Vec<String>>) -> Result<ExtractedData, Box<dyn Error + Send + Sync>> {
    println!("Подключение к Redis...");
    let client = RedisClient::open(url)?;
    let mut con = client.get_async_connection().await.map_err(|e| -> Box<dyn Error + Send + Sync> { anyhow!("Ошибка получения асинхронного соединения Redis: {}", e).into() })?;
    println!("Подключение к Redis успешно установлено.");

    println!("Извлечение ключей по паттерну: '{}'...", key_pattern);

    let keys: Vec<String> = con.keys(key_pattern).await.map_err(|e| -> Box<dyn Error + Send + Sync> { anyhow!("Ошибка получения ключей Redis: {}", e).into() })?;

    if keys.is_empty() {
        println!("Не найдено ключей, соответствующих паттерну.");
        return Ok(ExtractedData { headers: vec![], rows: vec![] });
    }

    let headers = vec!["Key".to_string(), "Value".to_string()]; // Убран `mut`

    let mut data_rows: Vec<Vec<String>> = Vec::new();

    if let Some(_expected) = expected_headers { // Убран `mut`, переименована в `_expected`
        println!("Предупреждение: Проверка ожидаемых заголовков не реализована для Redis.");
    }

    for key in keys {
        let value: String = con.get(&key).await.map_err(|e| -> Box<dyn Error + Send + Sync> { anyhow!("Ошибка получения значения для ключа '{}': {}", key, e).into() })?;
        data_rows.push(vec![key, value]);
    }

    println!("Извлечение из Redis успешно. Извлечено {} строк.", data_rows.len());
    Ok(ExtractedData { headers, rows: data_rows })
}

pub async fn extract_from_elasticsearch(url: &str, index: &str, query: JsonValue, mut expected_headers: Option<Vec<String>>) -> Result<ExtractedData, Box<dyn Error + Send + Sync>> {
    println!("Подключение к Elasticsearch...");
    let transport = Transport::single_node(url)?;
    let client = Elasticsearch::new(transport);
    println!("Подключение к Elasticsearch успешно установлено.");

    println!("Извлечение из индекса '{}' с запросом: {}", index, query);

    let search_response = client
        .search(elasticsearch::SearchParts::Index(&[index]))
        .body(&query)
        .send()
        .await?
        .json::<JsonValue>()
        .await?;

    let search_response_clone = search_response.clone();

    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();
    let mut headers_extracted = false;
    let mut actual_headers: Vec<String> = Vec::new();

    if let Some(hits) = search_response_clone["hits"]["hits"].as_array() {
        if hits.is_empty() {
            println!("Elasticsearch запрос вернул 0 хитов.");
            return Ok(ExtractedData { headers: vec![], rows: vec![] });
        }
        for hit in hits {
            if let Some(source) = hit["_source"].as_object() {
                if !headers_extracted {
                    actual_headers = source.keys().map(|key| key.to_string()).collect();
                    actual_headers.sort();
                    headers = actual_headers.clone();
                    headers_extracted = true;

                    if let Some(ref mut expected) = expected_headers {
                        expected.sort();

                        let mut actual_sorted = actual_headers.clone();
                        actual_sorted.sort();

                        if actual_sorted != *expected {
                            let expected_str = expected.join(", ");
                            let actual_str = actual_headers.join(", ");
                            let error_msg = format!("Column mismatch: Expected [{}], Got [{}]", expected_str, actual_str);
                            return Err(anyhow!(error_msg).into());
                        }
                    }
                }

                let mut current_row_data: Vec<String> = Vec::new();
                for header in &headers {
                    let string_value = source.get(header)
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .or_else(|| source.get(header).map(|v| v.to_string()))
                        .unwrap_or_else(|| "".to_string());
                    current_row_data.push(string_value);
                }
                data_rows.push(current_row_data);
            }
        }
    } else {
        println!("Elasticsearch ответ не содержит ожидаемой структуры 'hits.hits'.");
        return Err(anyhow!("Elasticsearch response missing 'hits.hits' array").into());
    }

    println!("Извлечение из Elasticsearch успешно. Извлечено {} строк.", data_rows.len());
    Ok(ExtractedData { headers: actual_headers, rows: data_rows })
}