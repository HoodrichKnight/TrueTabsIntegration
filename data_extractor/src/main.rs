use anyhow::{Result, anyhow};
use std::io::{self, Write};
use data_extractor::db::{sql, nosql, ExtractedData};
use serde_json::Value as JsonValue;

fn read_required_input(prompt: &str) -> Result<String> {
    loop {
        print!("{}", prompt);
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let trimmed = input.trim().to_string();
        if trimmed.is_empty() {
            eprintln!("Это поле обязательно. Пожалуйста, введите значение.");
        } else {
            return Ok(trimmed);
        }
    }
}

fn read_optional_input(prompt: &str) -> Result<String> {
    print!("{}", prompt);
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}

fn read_required_json(prompt: &str) -> Result<JsonValue> {
    loop {
        print!("{}", prompt);
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let trimmed = input.trim();
        if trimmed.is_empty() {
            eprintln!("Это поле обязательно. Пожалуйста, введите JSON строку.");
            continue;
        }
        match serde_json::from_str(trimmed) {
            Ok(json) => return Ok(json),
            Err(e) => eprintln!("Ошибка парсинга JSON: {}. Попробуйте снова.", e),
        }
    }
}


#[tokio::main]
async fn main() -> Result<()> {
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

        let mut extraction_result: Result<ExtractedData> = Err(anyhow!("Выбор не обработан"));

        match choice {
            "1" => {
                println!("\n--- Настройки PostgreSQL ---");
                let db_url = read_required_input("Введите DATABASE_URL_POSTGRES (например, postgres://user:pass@host:port/db): ")?;
                let query = read_required_input("Введите SQL запрос (например, SELECT * FROM table LIMIT 100): ")?;
                match sql::get_postgres_pool(&db_url).await {
                    Ok(pool) => {
                        println!("Подключено к PostgreSQL.");
                        extraction_result = sql::extract_from_sql(&pool, &query).await;
                    },
                    Err(e) => extraction_result = Err(e),
                }
            }
            "2" => {
                println!("\n--- Настройки MySQL ---");
                let db_url = read_required_input("Введите DATABASE_URL_MYSQL (например, mysql://user:pass@host:port/db): ")?;
                let query = read_required_input("Введите SQL запрос: ")?;
                match sql::get_mysql_pool(&db_url).await {
                    Ok(pool) => {
                        println!("Подключено к MySQL.");
                        extraction_result = sql::extract_from_sql(&pool, &query).await;
                    },
                    Err(e) => extraction_result = Err(e),
                }
            }
            "3" => {
                println!("\n--- Настройки SQLite ---");
                let db_url = read_required_input("Введите DATABASE_URL_SQLITE (например, sqlite://./mydatabase.db): ")?;
                let query = read_required_input("Введите SQL запрос: ")?;
                match sql::get_sqlite_pool(&db_url).await {
                    Ok(pool) => {
                        println!("Подключено к SQLite.");
                        extraction_result = sql::extract_from_sql(&pool, &query).await;
                    },
                    Err(e) => extraction_result = Err(e),
                }
            }
            "4" => {
                println!("\n--- Настройки MSSQL ---");
                let db_url = read_required_input("Введите DATABASE_URL_MSSQL (например, mssql://user:pass@host:port/db): ")?;
                let query = read_required_input("Введите SQL запрос: ")?;
                match sql::get_mssql_pool(&db_url).await {
                    Ok(pool) => {
                        println!("Подключено к MSSQL.");
                        extraction_result = sql::extract_from_sql(&pool, &query).await;
                    },
                    Err(e) => extraction_result = Err(e),
                }
            }
            "5" => {
                println!("\n--- Настройки MongoDB ---");
                let uri = read_required_input("Введите MONGODB_URI (например, mongodb://host:port): ")?;
                let db_name = read_required_input("Введите имя базы данных: ")?;
                let collection_name = read_required_input("Введите имя коллекции: ")?;
                extraction_result = nosql::extract_from_mongodb(&uri, &db_name, &collection_name).await;
            }
            "6" => {
                println!("\n--- Настройки Redis ---");
                let url = read_required_input("Введите REDIS_URL (например, redis://host:port/db): ")?;
                let key_pattern = read_optional_input("Введите паттерн ключей (оставьте пустым для '*', например, user:*): ")?;
                let pattern = if key_pattern.is_empty() { "*".to_string() } else { key_pattern };
                extraction_result = nosql::extract_from_redis(&url, &pattern).await;
            }
            "7" => {
                println!("\n--- Настройки Cassandra/ScyllaDB ---");
                let addresses = read_required_input("Введите адреса узлов через запятую (например, host1:port,host2:port): ")?;
                let keyspace = read_required_input("Введите имя keyspace: ")?;
                let query = read_required_input("Введите CQL запрос: ")?;
                extraction_result = nosql::extract_from_cassandra(&addresses, &keyspace, &query).await;
            }
            "8" => {
                println!("\n--- Настройки ClickHouse ---");
                let url = read_required_input("Введите CLICKHOUSE_URL (например, clickhouse://host:port/db): ")?;
                let query = read_required_input("Введите SQL запрос: ")?;
                extraction_result = nosql::extract_from_clickhouse(&url, &query).await;
            }
            "9" => {
                println!("\n--- Настройки InfluxDB ---");
                let url = read_required_input("Введите INFLUXDB_URL (например, http://host:port): ")?;
                let token = read_required_input("Введите INFLUXDB_TOKEN: ")?;
                let org = read_required_input("Введите INFLUXDB_ORG: ")?;
                let bucket = read_required_input("Введите INFLUXDB_BUCKET: ")?;
                let query = read_required_input("Введите Flux запрос (в одну строку): ")?;
                extraction_result = nosql::extract_from_influxdb(&url, &token, &org, &bucket, &query).await;
            }
            "10" => {
                println!("\n--- Настройки Elasticsearch ---");
                let url = read_required_input("Введите ELASTICSEARCH_URL (например, http://host:port): ")?;
                let index = read_required_input("Введите имя индекса: ")?;
                let query_str = read_optional_input("Введите JSON тело запроса (оставьте пустым для match_all='{}'): ")?;
                let query_str = if query_str.is_empty() { "{}".to_string() } else { query_str };

                let es_query: Result<JsonValue, _> = serde_json::from_str(&query_str);
                match es_query {
                    Ok(query_body) => {
                        extraction_result = nosql::extract_from_elasticsearch(&url, &index, query_body).await;
                    },
                    Err(e) => extraction_result = Err(anyhow!("Ошибка парсинга JSON запроса: {}", e)),
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