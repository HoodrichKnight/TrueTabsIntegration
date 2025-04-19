use anyhow::{Result, anyhow};
use std::collections::HashMap;
use futures::{TryStreamExt, StreamExt};

use mongodb::{Client as MongoClient, bson::{Document, Bson}, options::ClientOptions};
use futures::stream::TryStream;

use redis::{Client as RedisClient, AsyncCommands};

use cdrs::{cluster::TcpCluster, authenticators::NoneAuthenticator};
use cdrs::query::QueryExecutor;
use cdrs::types::rows::Row as CdrsRow;
use uuid::Uuid;

use clickhouse_rs::{Client as ClickhouseClient};

use influxdb_client::{Client as InfluxClient, models::{Query as InfluxQuery, Record as InfluxRecord}};

use elasticsearch::{Elasticsearch, http::transport::{Transport, SingleNodeConnectionPool}};
use serde_json::{json, Value as JsonValue};

use neo4rs::{Graph, config::Config as Neo4jConfig};

use neo4rs::Row as Neo4jRow;

use super::ExtractedData;

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
            headers.sort();
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
                Some(Bson::Document(doc)) => format!("{:?}", doc), // Просто строковое представление для вложенных документов
                // Добавьте другие типы Bson по мере необходимости
                _ => "".to_string(), // Значение отсутствует или другого типа
            };
            current_row_data.push(string_value);
        }
        data_rows.push(current_row_data);
    }


    Ok(ExtractedData { headers, rows: data_rows })
}

pub async fn extract_from_redis(url: &str, key_pattern: &str) -> Result<ExtractedData> {
    println!("Подключение к Redis...");
    let client = RedisClient::open(url)?;
    let mut con = client.get_async_connection().await?;
    println!("Подключение к Redis успешно установлено.");

    println!("Извлечение ключей по паттерну: '{}'...", key_pattern);

    let keys: Vec<String> = con.keys(key_pattern).await?;

    if keys.is_empty() {
        println!("Не найдено ключей, соответствующих паттерну.");
        return Ok(ExtractedData { headers: vec![], rows: vec![] });
    }

    let mut headers = vec!["Key".to_string(), "Value".to_string()];
    let mut data_rows: Vec<Vec<String>> = Vec::new();

    for key in keys {
        let value: String = con.get(&key).await?;
        data_rows.push(vec![key, value]);
    }

    Ok(ExtractedData { headers, rows: data_rows })
}

pub async fn extract_from_cassandra(addresses: &str, keyspace: &str, query: &str) -> Result<ExtractedData> {
    println!("Подключение к Cassandra...");
    let nodes: Vec<String> = addresses.split(',').map(|s| s.trim().to_string()).collect();

    let cluster = TcpCluster::new(nodes, NoneAuthenticator {}).await?;
    let mut session = cluster.connect(keyspace).await?;
    println!("Подключение к Cassandra успешно установлено.");

    println!("Выполнение CQL запроса: {}", query);

    let rows: Vec<CdrsRow> = session.query_async(query, ()).await?
        .response_body()?.ok_or_else(|| anyhow!("Нет тела ответа"))?.rows()?
        .into_iter().collect();


    if rows.is_empty() {
        println!("CQL запрос вернул 0 строк.");
        return Ok(ExtractedData { headers: vec![], rows: vec![] });
    }

    let headers: Vec<String> = rows[0].columns.iter().map(|col| col.name.clone()).collect();

    let data_rows: Vec<Vec<String>> = rows.into_iter().map(|row| {
        headers.iter().map(|header| {
            let value_option = row.get_value(header).and_then(|v| v.as_cow_str().map(|cow| cow.into_owned()));
            value_option.unwrap_or_else(|| "".to_string())
        }).collect()
    }).collect();


    Ok(ExtractedData { headers, rows: data_rows })
}

pub async fn extract_from_clickhouse(url: &str, query: &str) -> Result<ExtractedData> {
    println!("Подключение к ClickHouse...");
    let client = ClickhouseClient::new(url.parse()?); // parse url
    println!("Подключение к ClickHouse успешно установлено.");

    println!("Выполнение ClickHouse запроса: {}", query);

    let mut stream = client.query(query).stream();

    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();
    let mut headers_extracted = false;

    while let Some(block) = stream.next().await.transpose()? {
        if !headers_extracted {
            headers = block.columns().iter().map(|col| col.name().to_string()).collect();
            headers_extracted = true;
        }

        for row in block.rows() {
            let mut current_row_data: Vec<String> = Vec::new();
            for value in row.iter() {
                let string_value = value.as_sql()?;
                current_row_data.push(string_value);
            }
            data_rows.push(current_row_data);
        }
    }

    if !headers_extracted && data_rows.is_empty() {
        println!("ClickHouse запрос вернул 0 строк.");
        Ok(ExtractedData { headers: vec![], rows: vec![] })
    } else {
        println!("ClickHouse запрос успешно выполнен. Извлечено {} строк.", data_rows.len());
        Ok(ExtractedData { headers, rows: data_rows })
    }
}

pub async fn extract_from_influxdb(
    url: &str,
    token: &str,
    org: &str,
    bucket: &str,
    query: &str,
) -> Result<ExtractedData> {
    println!("Подключение к InfluxDB...");
    let client = InfluxClient::new(url, token).unwrap();

    println!("Подключение к InfluxDB успешно установлено.");
    println!("Выполнение Flux запроса для org '{}', bucket '{}'...", org, bucket);

    let influx_query = InfluxQuery::from(query);
    let records = client.query(&influx_query, bucket, org).await?;

    if records.is_empty() {
        println!("Flux запрос вернул 0 записей.");
        return Ok(ExtractedData { headers: vec![], rows: vec![] });
    }

    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();
    let mut headers_extracted = false;

    for record in records {
        let mut current_row_data: Vec<String> = Vec::new();
        let mut current_row_headers: Vec<String> = Vec::new();

        println!("Предупреждение: Парсинг результатов InfluxDB из версии 0.1.x API не реализован.");
        return Ok(ExtractedData { headers: vec![], rows: vec![] });
    }

}

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
                    headers.sort(); // Сортируем заголовки
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

pub async fn extract_from_neo4j(uri: &str, user: &str, pass: &str, query: &str) -> Result<ExtractedData> {
    println!("Подключение к Neo4j...");
    // Neo4jConfig::new требует user и pass
    let neo4j_config = Neo4jConfig::new(uri, user, pass);
    let graph = Graph::connect(neo4j_config).await?;
    println!("Подключение к Neo4j успешно установлено.");

    println!("Выполнение Cypher запроса: {}", query);

    let mut result_stream = graph.execute(neo4rs::query(query)).await?;


    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();
    let mut headers_extracted = false;

    while let Some(row_result) = result_stream.next().await {
        let row: Neo4jRow = row_result?;

        if !headers_extracted {
            headers = row.keys().iter().map(|key| key.to_string()).collect();
            headers_extracted = true;
        }

        let mut current_row_data: Vec<String> = Vec::new();
        for header in &headers {
            let string_value = row.get::<String>(header).unwrap_or_else(|| "".to_string());
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
}


// // Couchbase (временно отключен)
// pub async fn extract_from_couchbase(
//     cluster_url: &str,
//     user: &str,
//     pass: &str,
//     bucket_name: &str,
//     query: &str,
// ) -> Result<ExtractedData> {
//     println!("Подключение к Couchbase...");
//     Err(anyhow!("Поддержка Couchbase временно отключена из-за проблем сборки."))
// }