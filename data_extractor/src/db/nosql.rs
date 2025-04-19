// data_extractor/src/db/nosql.rs

use anyhow::{Result, anyhow};
use std::collections::HashMap; // Убедитесь, что HashMap используется, иначе удалите
use futures::{TryStreamExt, StreamExt};

// Импорты для MongoDB
use mongodb::{Client as MongoClient, bson::{Document, Bson}, options::ClientOptions};
use futures::stream::TryStream; // Импорт TryStream

// Импорты для Redis
use redis::{Client as RedisClient, AsyncCommands, RedisError}; // Убедитесь, что все импорты используются


// // Импорты для Cassandra (временно отключены)
// use cdrs::{cluster::TcpCluster, authenticators::NoneAuthenticator};
// use cdrs::query::QueryExecutor;
// use cdrs::types::rows::Row as CdrsRow;
// use cdrs::types::from_cdrs::FromCdrsBy;
// use uuid::Uuid;


// Импорты для ClickHouse
use clickhouse_rs::{Client as ClickhouseClient, types::{Value, ValueRef}}; // Убедитесь, что Value/ValueRef используются

// // Импорты для InfluxDB - временно отключены
// use influxdb_client::{Client as InfluxClient};


// Импорты для Elasticsearch
use elasticsearch::{Elasticsearch, http::transport::{Transport, SingleNodeConnectionPool}}; // Убедитесь, что все импорты используются
use serde_json::{json, Value as JsonValue}; // Убедитесь, что json! и Value используются

// // Импорты для Neo4j - временно отключены
// use neo4rs::{Graph, Config, Error as Neo4jError, query, types::Value as Neo4jValueType};
// use neo4rs::Row as Neo4jRow;


// // Импорты для Couchbase (временно отключены)
// use couchbase::{Cluster, QueryOptions};


use crate::db::ExtractedData; // Исправлен путь импорта

// --- Функции извлечения для NoSQL баз данных ---

// MongoDB
pub async fn extract_from_mongodb(uri: &str, db_name: &str, collection_name: &str) -> Result<ExtractedData> {
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

    while let Some(result) = cursor.next().await {
        let doc = result?;

        if !headers_extracted {
            headers = doc.keys().map(|key| key.to_string()).collect();
            headers.sort(); // Сортируем заголовки
            headers_extracted = true;
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
                Some(Bson::Document(doc)) => format!("{:?}", doc),
                _ => "".to_string(),
            };
            current_row_data.push(string_value);
        }
        data_rows.push(current_row_data);
    }


    Ok(ExtractedData { headers, rows: data_rows })
}

// Redis
pub async fn extract_from_redis(url: &str, key_pattern: &str) -> Result<ExtractedData> {
    println!("Подключение к Redis...");
    let client = RedisClient::open(url)?;
    let mut con = client.get_async_connection().await.map_err(|e| anyhow!("Ошибка получения асинхронного соединения Redis: {}", e))?;
    println!("Подключение к Redis успешно установлено.");

    println!("Извлечение ключей по паттерну: '{}'...", key_pattern);

    let keys: Vec<String> = con.keys(key_pattern).await.map_err(|e| anyhow!("Ошибка получения ключей Redis: {}", e))?;

    if keys.is_empty() {
        println!("Не найдено ключей, соответствующих паттерну.");
        return Ok(ExtractedData { headers: vec![], rows: vec![] });
    }

    let mut headers = vec!["Key".to_string(), "Value".to_string()];
    let mut data_rows: Vec<Vec<String>> = Vec::new();

    for key in keys {
        let value: String = con.get(&key).await.map_err(|e| anyhow!("Ошибка получения значения для ключа '{}': {}", key, e))?;
        data_rows.push(vec![key, value]);
    }

    Ok(ExtractedData { headers, rows: data_rows })
}


/*
// Закомментирован блок для Cassandra
pub async fn extract_from_cassandra(addresses: &str, keyspace: &str, query: &str) -> Result<ExtractedData> {
    println!("Подключение к Cassandra...");
    Err(anyhow!("Поддержка Cassandra временно отключена из-за проблем сборки."))
}
*/

/*
// Закомментирован блок для ClickHouse из-за problematic Client::new()
pub async fn extract_from_clickhouse(url: &str, query: &str) -> Result<ExtractedData> {
    println!("Подключение к ClickHouse...");
     println!("Предупреждение: Метод ClickhouseClient::new не найден. Поддержка ClickHouse временно отключена.");
     Ok(ExtractedData { headers: vec![], rows: vec![] })
}
*/

/*
// Закомментирован блок для InfluxDB из-за устаревшего API и проблем Client::new()
pub async fn extract_from_influxdb(
    url: &str,
    token: &str,
    org: &str,
    bucket: &str,
    query: &str, // Flux query string
) -> Result<ExtractedData> {
    println!("Подключение к InfluxDB...");
    println!("Предупреждение: Поддержка InfluxDB временно отключена из-за устаревшего API и проблем сборки.");
     Ok(ExtractedData { headers: vec![], rows: vec![] })
}
*/


// Elasticsearch
pub async fn extract_from_elasticsearch(url: &str, index: &str, query: JsonValue) -> Result<ExtractedData> {
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

    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();
    let mut headers_extracted = false;

    if let Some(hits) = search_response["hits"]["hits"].as_array() {
        for hit in hits {
            if let Some(source) = hit["_source"].as_object() {
                if !headers_extracted {
                    headers = source.keys().map(|key| key.to_string()).collect();
                    headers.sort();
                    headers_extracted = true;
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
    }


    Ok(ExtractedData { headers, rows: data_rows })
}

/*
// Закомментирован блок для Neo4j из-за проблем сборки
pub async fn extract_from_neo4j(uri: &str, user: &str, pass: &str, query: &str) -> Result<ExtractedData> {
    println!("Подключение к Neo4j...");
    println!("Предупреждение: Поддержка Neo4j временно отключена из-за проблем сборки.");
    Err(anyhow!("Поддержка Neo4j временно отключена из-за проблем сборки."))
    /*
    // Реальный код для Neo4j (закомментирован)
    let neo4j_config = Config::new(uri, user, pass)?;
    let graph = Graph::connect(neo4j_config).await?;
    println!("Подключение к Neo4j успешно установлено.");
    println!("Выполнение Cypher запроса: {}", query);
    let mut result_stream = graph.execute(query(query)).await?;
    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();
    let mut headers_extracted = false;
    while let Some(row_result) = result_stream.next().await {
        let row: Neo4jRow = row_result.map_err(|e| anyhow!("Ошибка получения строки из Neo4j результата: {}", e))?;
        if !headers_extracted {
             headers = row.keys().iter().map(|key| key.to_string()).collect();
             headers_extracted = true;
        }
        let mut current_row_data: Vec<String> = Vec::new();
        for header in &headers {
            let value_option: Option<Neo4jValueType> = row.get(header)
                 .map_err(|e| anyhow!("Ошибка получения значения для колонки '{}': {}", header, e))?;
            let string_value = match value_option {
                Some(Neo4jValueType::String(s)) => s,
                Some(Neo4jValueType::Integer(i)) => i.to_string(),
                Some(Neo4jValueType::Float(f)) => f.to_string(),
                Some(Neo4jValueType::Boolean(b)) => b.to_string(),
                Some(Neo4jValueType::Date(d)) => d.to_string(),
                Some(Neo4jValueType::DateTime(dt)) => dt.to_string(),
                Some(Neo4jValueType::Duration(dur)) => dur.to_string(),
                Some(Neo4jValueType::Point(p)) => format!("{:?}", p),
                Some(Neo4jValueType::Path(p)) => format!("{:?}", p),
                Some(Neo4jValueType::Map(m)) => format!("{:?}", m),
                Some(Neo4jValueType::List(l)) => format!("{:?}", l),
                Some(Neo4jValueType::Node(n)) => format!("{:?}", n),
                Some(Neo4jValueType::Relationship(r)) => format!("{:?}", r),
                Some(Neo4jValueType::Time(t)) => t.to_string(),
                None => "".to_string(),
            };
             current_row_data.push(string_value);
        }
        data_rows.push(current_row_data);
    }
    if !headers_extracted && data_rows.is_empty() {
        println!("Cypher запрос вернул 0 строк.");
        Ok(ExtractedData { headers: vec![], rows: vec![] })
    } else {
        println!("Cypher запрос успешно выполнен. Извлечено {} строк.", data_rows.len());
        Ok(ExtractedData { headers, rows: data_rows })
    }
    */
}
*/

/*
// Закомментирован блок для Couchbase
pub async fn extract_from_couchbase(
    cluster_url: &str,
    user: &str,
    pass: &str,
    bucket_name: &str,
    query: &str,
) -> Result<ExtractedData> {
    println!("Подключение к Couchbase...");
    Err(anyhow!("Поддержка Couchbase временно отключена из-за проблем сборки."))
}
*/