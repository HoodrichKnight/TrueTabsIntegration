use anyhow::{Result, anyhow};
use std::io::{self, Write};
use data_extractor::{db::{sql, nosql, ExtractedData}, file_loader};
use serde_json::{Value as JsonValue, json};
use reqwest::{Client, header};
use tokio::time::{sleep, Duration};
use std::collections::HashMap;

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

async fn upload_to_true_tabs(
    data: ExtractedData,
    api_token: &str,
    datasheet_id: &str,
    field_map: &HashMap<String, String>,
) -> Result<()> {
    let base_url = "https://true.tabs.sale/fusion/v1/datasheets/";
    let upload_url = format!("{}{}/records?fieldKey=id", base_url, datasheet_id);
    let client = Client::new();

    let batch_size = 1000;
    let total_records = data.rows.len();
    let mut uploaded_count = 0;

    if total_records == 0 {
        println!("Нет данных для загрузки.");
        return Ok(());
    }

    println!("Начата загрузка {} записей в True Tabs Datasheet: {}", total_records, datasheet_id);
    println!("Используются Field ID для сопоставления колонок.");

    for (batch_index, chunk) in data.rows.chunks(batch_size).enumerate() {
        let mut records_json: Vec<JsonValue> = Vec::new();

        for row in chunk {
            let mut record_object = serde_json::Map::new();
            for (i, header_name) in data.headers.iter().enumerate() {
                if let Some(field_id) = field_map.get(header_name) {
                    let value_str = row.get(i).unwrap_or(&"".to_string());
                    record_object.insert(field_id.clone(), JsonValue::String(value_str.clone()));
                } else {
                    eprintln!("Предупреждение: Не найден Field ID для колонки '{}'. Пропускаем поле.", header_name);
                }
            }
            records_json.push(JsonValue::Object(record_object));
        }

        let request_body = json!({
            "records": records_json,
            "fieldKey": "id"
        });

        println!("Отправка батча {} ({} записей)...", batch_index + 1, chunk.len());

        let response = client.post(&upload_url)
            .header(header::AUTHORIZATION, format!("Bearer {}", api_token))
            .header(header::CONTENT_TYPE, "application/json")
            .json(&request_body)
            .send()
            .await?;

        let status = response.status();
        let response_text = response.text().await?;

        if status.is_success() {
            let api_response: Result<JsonValue, _> = serde_json::from_str(&response_text);
            match api_response {
                Ok(api_json) => {
                    if api_json["success"].as_bool() == Some(true) && api_json["code"].as_u64() == Some(200) {
                        let records_in_response = api_json["data"]["records"].as_u64().unwrap_or(0);
                        uploaded_count += records_in_response;
                        println!("Батч {} успешно загружен. Обработано записей по ответу: {}", batch_index + 1, records_in_response);
                    } else {
                        eprintln!("Предупреждение: Батч {} отправлен, но API вернуло success: false или нестандартный код: {}", batch_index + 1, response_text);
                    }
                }
                Err(_) => {
                    eprintln!("Предупреждение: Батч {} отправлен, получен статус {}, но не удалось распарсить JSON ответ: {}", batch_index + 1, status, response_text);
                }
            }

        } else {
            eprintln!("Ошибка при отправке батча {}: Статус {}, Ответ: {}", batch_index + 1, status, response_text);
            return Err(anyhow!("Ошибка HTTP статуса при загрузке батча {}: {} - {}", batch_index + 1, status, response_text));
        }

        if (batch_index + 1) * batch_size < total_records {
            sleep(Duration::from_millis(200)).await;
        }
    }

    println!("Загрузка завершена. Всего отправлено батчей: {}. Предположительно загружено записей: {}", data.rows.chunks(batch_size).count(), uploaded_count);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {

    loop {
        println!("\nВыберите источник данных:");
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
        println!("11. Загрузить из Excel файла");
        println!("12. Загрузить из CSV файла");
        println!("13. Neo4j");
        println!("14. Couchbase");
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
            "11" => {
                println!("\n--- Загрузка из Excel файла ---");
                let file_path = read_required_input("Введите путь к Excel файлу (.xlsx, .xls): ")?;
                extraction_result = file_loader::read_excel(&file_path);
            }
            "12" => {
                println!("\n--- Загрузка из CSV файла ---");
                let file_path = read_required_input("Введите путь к CSV файлу: ")?;
                extraction_result = file_loader::read_csv(&file_path);
            }
            "13" => {
                println!("\n--- Настройки Neo4j ---");
                let uri = read_required_input("Введите URI Neo4j (например, neo4j://host:port): ")?;
                let user = read_required_input("Введите имя пользователя: ")?;
                let password = read_required_input("Введите пароль: ")?;
                let query = read_required_input("Введите Cypher запрос (например, MATCH (n:Label) RETURN n.name, n.age LIMIT 100): ")?;
                extraction_result = nosql::extract_from_neo4j(&uri, &user, &password, &query).await;
            }
            "14" => {
                println!("\n--- Настройки Couchbase ---");
                let cluster_url = read_required_input("Введите URL кластера Couchbase (например, couchbase://host): ")?;
                let user = read_required_input("Введите имя пользователя: ")?;
                let password = read_required_input("Введите пароль: ")?;
                let bucket_name = read_required_input("Введите имя бакета: ")?;
                let query = read_required_input("Введите N1QL запрос (например, SELECT d.* FROM `bucket_name` d LIMIT 100): ")?;
                extraction_result = nosql::extract_from_couchbase(&cluster_url, &user, &password, &bucket_name, &query).await;
            }
            "0" => {
                println!("Выход из программы.");
                break;
            }
            _ => {
                eprintln!("Неверный ввод. Пожалуйста, выберите номер из списка.");
                continue;
            }
        }

        match extraction_result {
            Ok(data) => {
                println!("\n--- Результат извлечения ---");
                println!("Успешно извлечено {} строк.", data.rows.len());
                if !data.headers.is_empty() {
                    println!("Заголовки: {:?}", data.headers);
                } else {
                    println!("Заголовки не получены или отсутствуют.");
                }

                if data.headers.is_empty() && !data.rows.is_empty() {
                    eprintln!("Предупреждение: Извлечены строки данных, но отсутствуют заголовки. Невозможно сопоставить с Field ID.");
                    continue;
                }
                if data.rows.is_empty() {
                    println!("Нет строк данных для загрузки.");
                    continue;
                }

                println!("\n--- Сопоставление колонок с Field ID True Tabs ---");
                println!("Для каждого извлеченного заголовка (колонки) введите соответствующий Field ID из вашей таблицы True Tabs.");
                println!("Если для колонки нет соответствующего поля в True Tabs, оставьте поле ввода пустым.");

                let mut field_map: HashMap<String, String> = HashMap::new();
                for header_name in &data.headers {
                    let prompt = format!("Введите Field ID для колонки '{}': ", header_name);
                    let field_id = read_optional_input(&prompt)?;
                    if !field_id.is_empty() {
                        field_map.insert(header_name.clone(), field_id);
                    }
                }

                if field_map.is_empty() {
                    eprintln!("Ошибка: Не было предоставлено ни одного Field ID. Невозможно загрузить данные.");
                    continue;
                }

                println!("Сопоставление готово: {:?}", field_map);

                println!("\n--- Загрузка в True Tabs ---");
                let api_token = read_required_input("Введите токен авторизации для True Tabs API: ")?;
                let datasheet_id = read_required_input("Введите Datasheet ID для загрузки (например, dst...): ")?;

                match upload_to_true_tabs(data, &api_token, &datasheet_id, &field_map).await {
                    Ok(_) => println!("Данные успешно загружены в True Tabs."),
                    Err(e) => eprintln!("Ошибка при загрузке данных в True Tabs: {}", e),
                }

            },
            Err(e) => {
                eprintln!("Произошла ошибка при извлечении данных: {}", e);
            }
        }
    }

    Ok(())
}