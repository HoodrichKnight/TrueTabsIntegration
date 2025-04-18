use anyhow::Result;
use std::{env, io::{self, Write}};
use data_extractor::db::{sql, nosql, ExtractedData};
use serde_json::Value as JsonValue;


#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    println!("Переменные окружения загружены.");

    loop {
        println!("\nВыберите базу данных для извлечения данных:");
        println!("1. PostgreSQL");
        println!("2. MySQL");
        println!("3. SQLite");
        println!("4. MSSQL");
        println!("5. MongoDB");
        println!("6. Redis");
        println!("7. Cassandra/ScyllaDB");
        println!("8. ClickHouse");
        println!("9. InfluxDB");
        println!("10. Elasticsearch");
        println!("0. Выход");

        print!("Введите номер: ");
        io::stdout().flush()?;

        let mut choice = String::new();
        io::stdin().read_line(&mut choice)?;

        let choice = choice.trim();

        if choice == "0" {
            println!("Выход из программы.");
            break;
        }

        let extraction_result: Result<ExtractedData> = match choice {
            "1" => {
                match sql::get_postgres_pool().await {
                    Ok(pool) => {
                        println!("Подключено к PostgreSQL.");
                        let query = env::var("POSTGRES_QUERY").unwrap_or_else(|_| "SELECT 1 as example_column".to_string());
                        sql::extract_from_sql(&pool, &query).await
                    },
                    Err(e) => Err(e.into()),
                }
            }
            "2" => {
                // MySQL
                match sql::get_mysql_pool().await {
                    Ok(pool) => {
                        println!("Подключено к MySQL.");
                        let query = env::var("MYSQL_QUERY").unwrap_or_else(|_| "SELECT 1 as example_column".to_string());
                        sql::extract_from_sql(&pool, &query).await
                    },
                    Err(e) => Err(e.into()),
                }
            }
            "3" => {
                // SQLite
                match sql::get_sqlite_pool().await {
                    Ok(pool) => {
                        println!("Подключено к SQLite.");
                        let query = env::var("SQLITE_QUERY").unwrap_or_else(|_| "SELECT 1 as example_column".to_string());
                        sql::extract_from_sql(&pool, &query).await
                    },
                    Err(e) => Err(e.into()),
                }
            }
            "4" => {
                // MSSQL
                match sql::get_mssql_pool().await {
                    Ok(pool) => {
                        println!("Подключено к MSSQL.");
                        let query = env::var("MSSQL_QUERY").unwrap_or_else(|_| "SELECT 1 as example_column".to_string());
                        sql::extract_from_sql(&pool, &query).await
                    },
                    Err(e) => Err(e.into()),
                }
            }
            "5" => {
                // MongoDB
                let mongo_uri = env::var("MONGODB_URI").unwrap_or_else(|_| "".to_string());
                let mongo_db = env::var("MONGODB_DATABASE").unwrap_or_else(|_| "".to_string());
                let mongo_collection = env::var("MONGODB_COLLECTION").unwrap_or_else(|_| "".to_string());
                if mongo_uri.is_empty() || mongo_db.is_empty() || mongo_collection.is_empty() {
                    Err(anyhow::anyhow!("Настройки MongoDB (MONGODB_URI, MONGODB_DATABASE, MONGODB_COLLECTION) не найдены в .env"))
                } else {
                    nosql::extract_from_mongodb(&mongo_uri, &mongo_db, &mongo_collection).await
                }
            }
            "6" => {
                // Redis
                let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "".to_string());
                let redis_pattern = env::var("REDIS_KEY_PATTERN").unwrap_or_else(|_| "*".to_string());
                if redis_url.is_empty() {
                    Err(anyhow::anyhow!("Настройка Redis (REDIS_URL) не найдена в .env"))
                } else {
                    nosql::extract_from_redis(&redis_url, &redis_pattern).await
                }
            }
            "7" => {
                // Cassandra/ScyllaDB
                let cassandra_addresses = env::var("CASSANDRA_ADDRESSES").unwrap_or_else(|_| "".to_string());
                let cassandra_keyspace = env::var("CASSANDRA_KEYSPACE").unwrap_or_else(|_| "".to_string());
                let cassandra_query = env::var("CASSANDRA_QUERY").unwrap_or_else(|_| "".to_string());
                if cassandra_addresses.is_empty() || cassandra_keyspace.is_empty() || cassandra_query.is_empty() {
                    Err(anyhow::anyhow!("Настройки Cassandra/ScyllaDB (CASSANDRA_ADDRESSES, CASSANDRA_KEYSPACE, CASSANDRA_QUERY) не найдены в .env"))
                } else {
                    nosql::extract_from_cassandra(&cassandra_addresses, &cassandra_keyspace, &cassandra_query).await
                }
            }
            "8" => {
                // ClickHouse
                let clickhouse_url = env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "".to_string());
                let clickhouse_query = env::var("CLICKHOUSE_QUERY").unwrap_or_else(|_| "".to_string());
                if clickhouse_url.is_empty() || clickhouse_query.is_empty() {
                    Err(anyhow::anyhow!("Настройки ClickHouse (CLICKHOUSE_URL, CLICKHOUSE_QUERY) не найдены в .env"))
                } else {
                    nosql::extract_from_clickhouse(&clickhouse_url, &clickhouse_query).await
                }
            }
            "9" => {
                // InfluxDB
                let influx_url = env::var("INFLUXDB_URL").unwrap_or_else(|_| "".to_string());
                let influx_token = env::var("INFLUXDB_TOKEN").unwrap_or_else(|_| "".to_string());
                let influx_org = env::var("INFLUXDB_ORG").unwrap_or_else(|_| "".to_string());
                let influx_bucket = env::var("INFLUXDB_BUCKET").unwrap_or_else(|_| "".to_string());
                let influx_query = env::var("INFLUXDB_QUERY").unwrap_or_else(|_| "".to_string());
                if influx_url.is_empty() || influx_token.is_empty() || influx_org.is_empty() || influx_bucket.is_empty() || influx_query.is_empty() {
                    Err(anyhow::anyhow!("Настройки InfluxDB (INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET, INFLUXDB_QUERY) не найдены в .env"))
                } else {
                    nosql::extract_from_influxdb(&influx_url, &influx_token, &influx_org, &influx_bucket, &influx_query).await
                }
            }
            "10" => {
                // Elasticsearch
                let es_url = env::var("ELASTICSEARCH_URL").unwrap_or_else(|_| "".to_string());
                let es_index = env::var("ELASTICSEARCH_INDEX").unwrap_or_else(|_| "".to_string());
                let es_query_str = env::var("ELASTICSEARCH_QUERY").unwrap_or_else(|_| "{}".to_string());
                if es_url.is_empty() || es_index.is_empty() {
                    Err(anyhow::anyhow!("Настройки Elasticsearch (ELASTICSEARCH_URL, ELASTICSEARCH_INDEX) не найдены в .env"))
                } else {
                    let es_query: Result<JsonValue, _> = serde_json::from_str(&es_query_str);
                    match es_query {
                        Ok(query_body) => {
                            nosql::extract_from_elasticsearch(&es_url, &es_index, query_body).await
                        },
                        Err(e) => Err(anyhow::anyhow!("Ошибка парсинга JSON запроса Elasticsearch из .env: {}", e)),
                    }
                }
            }
            _ => {
                eprintln!("Неверный ввод. Пожалуйста, выберите номер из списка.");
                continue;
            }
        };

        match extraction_result {
            Ok(data) => {
                println!("Успешно извлечено {} строк.", data.rows.len());
                if !data.headers.is_empty() {
                    println!("Заголовки: {:?}", data.headers);
                } else {
                    println!("Заголовки не получены или отсутствуют.");
                }
            },
            Err(e) => {
                eprintln!("Произошла ошибка при извлечении данных: {}", e);
            }
        }
    }

    Ok(())
}