use anyhow::{Result, anyhow};
use std::{env, time::Duration};
use crate::db::ExtractedData;

use mongodb::{Client, options::ClientOptions, bson::Document};

use redis::{Client as RedisClient, AsyncCommands};

use cdrs::{frame::IntoBytes, query::QueryExecutor, types::IntoCdrsBy};
use cdrs_tokio::cluster::TcpCluster;
use cdrs_tokio::authenticators::NoneAuthenticator;
use cdrs::types::rows::Row;

use clickhouse_rs::{Client as ClickhouseClient, types::Row as ChRow};

use influxdb_client_rust::{Client as InfluxClient, models::{Query, Record}};

use elasticsearch::{Elasticsearch, SearchParts};
use serde_json::{json, Value as JsonValue};

fn get_env_var(key: &str) -> Result<String> {
    env::var(key).map_err(|e| anyhow!("Переменная окружения {} не найдена: {}", key, e))
}

pub async fn extract_from_mongodb(uri: &str, db_name: &str, collection_name: &str) -> Result<ExtractedData> {
    println!("Подключение к MongoDB: {}", uri);
    let client_options = ClientOptions::parse(uri).await?;
    let client = Client::with_options(client_options)?;
    let db = client.database(db_name);
    let collection = db.collection::<Document>(collection_name);

    println!("Извлечение данных из коллекции MongoDB: {}", collection_name);
    let mut cursor = collection.find(None, None).await?;

    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();
    let mut headers_collected = false;

    while let Some(doc) = cursor.next().await.transpose()? {
        if !headers_collected {
            headers = doc.keys().cloned().collect();
            headers.sort();
            headers_collected = true;
        }

        let mut row_values: Vec<String> = Vec::new();
        for header in &headers {
            let value = doc.get(header);
            let value_str = match value {
                Some(bson::Bson::String(s)) => s.clone(),
                Some(bson::Bson::Int32(i)) => i.to_string(),
                Some(bson::Bson::Int64(i)) => i.to_string(),
                Some(bson::Bson::Double(d)) => d.to_string(),
                Some(bson::Bson::Boolean(b)) => b.to_string(),
                Some(bson::Bson::DateTime(dt)) => dt.to_string(),
                Some(bson::Bson::ObjectId(oid)) => oid.to_string(),
                Some(bson::Bson::Decimal128(d)) => d.to_string(),
                Some(bson::Bson::Null) => "".to_string(),
                Some(bson::Bson::Array(arr)) => format!("{:?}", arr),
                Some(bson::Bson::Document(doc)) => format!("{:?}", doc),
                Some(_) => "[UNSUPPORTED BSON TYPE]".to_string(),
                None => "".to_string(),
            };
            row_values.push(value_str);
        }

        data_rows.push(row_values);
    }

    Ok(ExtractedData { headers, rows: data_rows })
}

pub async fn extract_from_redis(url: &str, key_pattern: &str) -> Result<ExtractedData> {
    println!("Подключение к Redis: {}", url);
    let client = RedisClient::open(url)?;
    let mut con = client.get_async_connection().await?;

    println!("Сканирование ключей в Redis с паттерном: {}", key_pattern);

    let mut iter: redis::AsyncIter<String> = con.scan(key_pattern).await?;

    let headers = vec!["Key".to_string(), "Type".to_string(), "Value".to_string(), "Score/Details".to_string()];
    let mut data_rows: Vec<Vec<String>> = Vec::new();

    while let Some(key) = iter.next_item().await? {
        let key_type: String = con.type_of(&key).await?;
        let mut row_values = vec![key.clone(), key_type.clone()];
        let mut value_str = "".to_string();
        let mut details_str = "".to_string();

        match key_type.as_str() {
            "string" => {
                value_str = con.get(&key).await?;
            },
            "hash" => {
                let hash_fields: Vec<(String, String)> = con.hgetall(&key).await?;
                value_str = format!("{{{}}}", hash_fields.into_iter().map(|(f, v)| format!("{}: {}", f, v)).collect::<Vec<_>>().join(", "));
            },
            "list" => {
                let list_values: Vec<String> = con.lrange(&key, 0, -1).await?;
                value_str = format!("[{}]", list_values.join(", "));
            },
            "set" => {
                let set_values: Vec<String> = con.smembers(&key).await?;
                value_str = format!("{{{}}}", set_values.join(", "));
            },
            "zset" => {
                let zset_values: Vec<(String, f64)> = con.zrange_withscores(&key, 0, -1).await?;
                value_str = format!("[{}]", zset_values.iter().map(|(m, _)| m.clone()).collect::<Vec<_>>().join(", "));
                details_str = format!("[{}]", zset_values.iter().map(|(_, s)| s.to_string()).collect::<Vec<_>>().join(", "));
            },
            "none" => {
                value_str = "[KEY NOT FOUND]".to_string();
            }
            _ => {
                value_str = "[UNSUPPORTED REDIS TYPE]".to_string();
            }
        }
        row_values.push(value_str);
        row_values.push(details_str);

        data_rows.push(row_values);
    }

    Ok(ExtractedData { headers, rows: data_rows })
}

pub async fn extract_from_cassandra(addresses: &str, keyspace: &str, query: &str) -> Result<ExtractedData> {
    println!("Подключение к Cassandra/ScyllaDB: {}", addresses);
    let authenticator = NoneAuthenticator {};
    let tcp_cluster = TcpCluster::new(addresses.split(',').collect::<Vec<_>>(), authenticator)?;
    let cluster_manager = tcp_cluster.connect().await?;
    let session = cluster_manager.user_session().await.expect("Session should be created");

    println!("Выполнение CQL запроса: {}", query);
    let query_obj = cdrs::query::Query::new(query);
    let result = session.query(query_obj).await?;

    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();

    if let Some(body) = result.into_body() {
        if let Some(rows_content) = body.rows_content {
            if let Some(ref metadata) = body.metadata {
                if let Some(ref col_specs) = metadata.col_specs {
                    headers = col_specs.iter().map(|cs| cs.name.to_string()).collect();
                }
            }

            for row in rows_content {
                let mut row_values: Vec<String> = Vec::new();
                for column in row.columns {
                    let value_str = match column.value {
                        Some(bytes) => {
                            String::from_utf8(bytes.into_bytes()).unwrap_or_else(|_| "[NON-UTF8 DATA]".to_string())
                        },
                        None => "".to_string(),
                    };
                    row_values.push(value_str);
                }
                data_rows.push(row_values);
            }
        }
    } else {
        println!("CQL запрос вернул пустой результат.");
    }

    Ok(ExtractedData { headers, rows: data_rows })
}

pub async fn extract_from_clickhouse(url: &str, query: &str) -> Result<ExtractedData> {
    println!("Подключение к ClickHouse: {}", url);
    let client = ClickhouseClient::connect(url).await?;

    println!("Выполнение ClickHouse запроса: {}", query);
    let rows = client.query(query).fetch_all().await?;

    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();

    if rows.is_empty() {
        println!("ClickHouse запрос вернул пустой результат.");
        return Ok(ExtractedData { headers, rows: data_rows });
    }

    headers = rows.columns().iter().map(|c| c.name().to_string()).collect();

    // Данные
    for row_index in 0..rows.len() {
        let mut row_values: Vec<String> = Vec::new();
        for col_index in 0..rows.columns().len() {
            let value_ref = rows[row_index].get::<clickhouse_rs::types::SqlType, _>(col_index)?;
            let value_str = format!("{}", value_ref);

            row_values.push(value_str);
        }
        data_rows.push(row_values);
    }

    Ok(ExtractedData { headers, rows: data_rows })
}

pub async fn extract_from_influxdb(url: &str, token: &str, org: &str, bucket: &str, query: &str) -> Result<ExtractedData> {
    println!("Подключение к InfluxDB: {}", url);
    let client = InfluxClient::new(url, token)?;

    println!("Выполнение Flux запроса: {}", query);
    let query_api = client.query_api();
    let mut tables = query_api.query(org, query).await?;

    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();
    let mut headers_collected = false;

    while let Some(table) = tables.next().await {
        let table = table?;

        for record in table.records {
            if !headers_collected {
                headers = record.columns.iter().map(|col| col.label.clone()).collect();
                headers_collected = true;
            }

            let mut row_values: Vec<String> = Vec::new();
            for header in &headers {
                let value = record.value(header.as_str());
                let value_str = match value {
                    Some(val) => format!("{}", val),
                    None => "".to_string(),
                };
                row_values.push(value_str);
            }
            data_rows.push(row_values);
        }
    }

    if !headers_collected {
        println!("Flux запрос вернул пустой результат.");
    }


    Ok(ExtractedData { headers, rows: data_rows })
}

pub async fn extract_from_elasticsearch(url: &str, index: &str, query_body: JsonValue) -> Result<ExtractedData> {
    println!("Подключение к Elasticsearch: {}", url);
    let client = Elasticsearch::new(elasticsearch::http::transport::TransportBuilder::new(elasticsearch::http::Url::parse(url)?).build()?);

    println!("Выполнение Elasticsearch запроса для индекса {}: {}", index, serde_json::to_string(&query_body)?);

    let search_response = client.search(SearchParts::Index(&[index]))
        .body(query_body)
        .send()
        .await?;

    let response_body = search_response.json::<JsonValue>().await?;

    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();
    let mut headers_collected = false;

    if let Some(hits) = response_body["hits"]["hits"].as_array() {
        for hit in hits {
            let mut current_headers = vec!["_id".to_string()];
            let mut current_row_values = vec![hit.get("_id").and_then(|id| id.as_str()).unwrap_or("").to_string()];

            if let Some(source) = hit.get("_source").and_then(|s| s.as_object()) {

                if !headers_collected {
                    let mut source_keys: Vec<String> = source.keys().cloned().collect();
                    source_keys.sort();
                    headers = current_headers.into_iter().chain(source_keys.into_iter()).collect();
                    headers_collected = true;
                }

                let mut row_values: Vec<String> = current_row_values;
                for header in headers.iter().skip(1) {
                    let value = source.get(header.as_str());
                    let value_str = match value {
                        Some(val) => val.to_string(),
                        None => "".to_string(),
                    };
                    row_values.push(value_str);
                }
                data_rows.push(row_values);
            } else {
                if !headers_collected {
                    headers = current_headers;
                    headers_collected = true;
                }
                data_rows.push(current_row_values);
            }
        }
    } else {
        println!("Elasticsearch запрос не вернул хиты.");
    }


    Ok(ExtractedData { headers, rows: data_rows })
}
