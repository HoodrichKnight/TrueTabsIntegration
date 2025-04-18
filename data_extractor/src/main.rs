use anyhow::{Result, anyhow};
use clap::Parser;
use std::{collections::HashMap, path::PathBuf};
use data_extractor::{db::{sql, nosql, ExtractedData}, file_loader};
use serde_json::Value as JsonValue;
use reqwest::{Client, header};
use tokio::time::{sleep, Duration};
use std::io::Write;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    source_type: String,

    #[arg(long)]
    source_url: Option<String>,

    #[arg(long)]
    source_user: Option<String>,

    #[arg(long)]
    source_pass: Option<String>,

    #[arg(long)]
    source_query: Option<String>,

    #[arg(long)]
    mongo_db: Option<String>,
    #[arg(long)]
    mongo_collection: Option<String>,
    #[arg(long)]
    redis_pattern: Option<String>,
    #[arg(long)]
    cassandra_keyspace: Option<String>,
    #[arg(long)]
    cassandra_query: Option<String>,
    #[arg(long)]
    influx_token: Option<String>,
    #[arg(long)]
    influx_org: Option<String>,
    #[arg(long)]
    influx_bucket: Option<String>,
    #[arg(long)]
    influx_query: Option<String>,
    #[arg(long)]
    es_index: Option<String>,
    #[arg(long)]
    es_query: Option<String>,
    #[arg(long)]
    neo4j_user: Option<String>,
    #[arg(long)]
    neo4j_pass: Option<String>,
    #[arg(long)]
    couchbase_bucket: Option<String>,
    #[arg(long)]
    couchbase_query: Option<String>,

    #[arg(long)]
    upload_api_token: String,

    #[arg(long)]
    upload_datasheet_id: String,

    #[arg(long)]
    upload_field_map_json: String,

    #[arg(long)]
    output_xlsx_path: PathBuf,
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
    let args = Args::parse();

    println!("Инициализация...");

    let extraction_result: Result<ExtractedData> = match args.source_type.as_str() {
        "postgres" => {
            let db_url = args.source_url.ok_or_else(|| anyhow!("Не указан --source-url для postgres"))?;
            let query = args.source_query.ok_or_else(|| anyhow!("Не указан --source-query для postgres"))?;
            sql::get_postgres_pool(&db_url).await.and_then(|pool| sql::extract_from_sql(&pool, &query).await)
        }
        "mysql" => {
            let db_url = args.source_url.ok_or_else(|| anyhow!("Не указан --source-url для mysql"))?;
            let query = args.source_query.ok_or_else(|| anyhow!("Не указан --source-query для mysql"))?;
            sql::get_mysql_pool(&db_url).await.and_then(|pool| sql::extract_from_sql(&pool, &query).await)
        }
        "sqlite" => {
            let db_url = args.source_url.ok_or_else(|| anyhow!("Не указан --source-url для sqlite"))?;
            let query = args.source_query.ok_or_else(|| anyhow!("Не указан --source-query для sqlite"))?;
            sql::get_sqlite_pool(&db_url).await.and_then(|pool| sql::extract_from_sql(&pool, &query).await)
        }
        "mssql" => {
            let db_url = args.source_url.ok_or_else(|| anyhow!("Не указан --source-url для mssql"))?;
            let query = args.source_query.ok_or_else(|| anyhow!("Не указан --source-query для mssql"))?;
            sql::get_mssql_pool(&db_url).await.and_then(|pool| sql::extract_from_sql(&pool, &query).await)
        }
        "mongodb" => {
            let uri = args.source_url.ok_or_else(|| anyhow!("Не указан --source-url (URI) для mongodb"))?;
            let db_name = args.mongo_db.ok_or_else(|| anyhow!("Не указан --mongo-db для mongodb"))?;
            let collection_name = args.mongo_collection.ok_or_else(|| anyhow!("Не указана --mongo-collection для mongodb"))?;
            nosql::extract_from_mongodb(&uri, &db_name, &collection_name).await
        }
        "redis" => {
            let url = args.source_url.ok_or_else(|| anyhow!("Не указан --source-url (URL) для redis"))?;
            let key_pattern = args.redis_pattern.unwrap_or("*".to_string());
            nosql::extract_from_redis(&url, &key_pattern).await
        }
        "cassandra" => {
            let addresses = args.source_url.ok_or_else(|| anyhow!("Не указан --source-url (адреса через запятую) для cassandra"))?;
            let keyspace = args.cassandra_keyspace.ok_or_else(|| anyhow!("Не указан --cassandra-keyspace для cassandra"))?;
            let query = args.cassandra_query.ok_or_else(|| anyhow!("Не указан --cassandra-query для cassandra"))?;
            nosql::extract_from_cassandra(&addresses, &keyspace, &query).await
        }
        "clickhouse" => {
            let url = args.source_url.ok_or_else(|| anyhow!("Не указан --source-url (URL) для clickhouse"))?;
            let query = args.source_query.ok_or_else(|| anyhow!("Не указан --source-query для clickhouse"))?;
            nosql::extract_from_clickhouse(&url, &query).await
        }
        "influxdb" => {
            let url = args.source_url.ok_or_else(|| anyhow!("Не указан --source-url (URL) для influxdb"))?;
            let token = args.influx_token.ok_or_else(|| anyhow!("Не указан --influx-token для influxdb"))?;
            let org = args.influx_org.ok_or_else(|| anyhow!("Не указан --influx-org для influxdb"))?;
            let bucket = args.influx_bucket.ok_or_else(|| anyhow!("Не указан --influx-bucket для influxdb"))?;
            let query = args.influx_query.ok_or_else(|| anyhow!("Не указан --influx-query (Flux запрос) для influxdb"))?;
            nosql::extract_from_influxdb(&url, &token, &org, &bucket, &query).await
        }
        "elasticsearch" => {
            let url = args.source_url.ok_or_else(|| anyhow!("Не указан --source-url (URL) для elasticsearch"))?;
            let index = args.es_index.ok_or_else(|| anyhow!("Не указан --es-index для elasticsearch"))?;
            let query_str = args.es_query.unwrap_or("{}".to_string());
            let es_query: Result<JsonValue, _> = serde_json::from_str(&query_str).map_err(|e| anyhow!("Ошибка парсинга --es-query JSON: {}", e));
            es_query.and_then(|query_body| async move {
                nosql::extract_from_elasticsearch(&url, &index, query_body).await
            }.await)
        }
        "excel" => {
            let file_path = args.source_url.ok_or_else(|| anyhow!("Не указан --source-url (путь к файлу) для excel"))?;
            file_loader::read_excel(&file_path)
        }
        "csv" => {
            let file_path = args.source_url.ok_or_else(|| anyhow!("Не указан --source-url (путь к файлу) для csv"))?;
            file_loader::read_csv(&file_path)
        }
        "neo4j" => {
            let uri = args.source_url.ok_or_else(|| anyhow!("Не указан --source-url (URI) для neo4j"))?;
            let user = args.neo4j_user.ok_or_else(|| anyhow!("Не указан --neo4j-user для neo4j"))?;
            let pass = args.neo4j_pass.ok_or_else(|| anyhow!("Не указан --neo4j-pass для neo4j"))?;
            let query = args.source_query.ok_or_else(|| anyhow!("Не указан --source-query (Cypher запрос) для neo4j"))?;
            nosql::extract_from_neo4j(&uri, &user, &pass, &query).await
        }
        "couchbase" => {
            let cluster_url = args.source_url.ok_or_else(|| anyhow!("Не указан --source-url (URL кластера) для couchbase"))?;
            let user = args.source_user.ok_or_else(|| anyhow!("Не указан --source-user для couchbase"))?;
            let pass = args.source_pass.ok_or_else(|| anyhow!("Не указан --source-pass для couchbase"))?;
            let bucket_name = args.couchbase_bucket.ok_or_else(|| anyhow!("Не указан --couchbase-bucket для couchbase"))?;
            let query = args.couchbase_query.ok_or_else(|| anyhow!("Не указан --couchbase-query (N1QL запрос) для couchbase"))?;
            nosql::extract_from_couchbase(&cluster_url, &user, &pass, &bucket_name, &query).await
        }
        _ => Err(anyhow!("Неизвестный тип источника данных: {}", args.source_type)),
    };

    match extraction_result {
        Ok(data) => {
            println!("Извлечение успешно. Извлечено {} строк.", data.rows.len());
            if data.headers.is_empty() && !data.rows.is_empty() {
                eprintln!("Предупреждение: Извлечены строки данных, но отсутствуют заголовки. Невозможно сопоставить с Field ID.");
            }
            if data.rows.is_empty() {
                println!("Нет строк данных для загрузки.");
                println!("STATUS: SUCCESS");
                println!("FILE_PATH: {}", args.output_xlsx_path.display());
                return Ok(());
            }
            if data.headers.is_empty() {
                eprintln!("STATUS: ERROR");
                eprintln!("MESSAGE: Отсутствуют заголовки, невозможно сопоставить с Field ID.");
                return Err(anyhow!("Отсутствуют заголовки, невозможно сопоставить с Field ID."));
            }


            let field_map: HashMap<String, String> = serde_json::from_str(&args.upload_field_map_json)
                .map_err(|e| anyhow!("Ошибка парсинга --upload-field-map-json: {}", e))?;

            if field_map.is_empty() {
                eprintln!("STATUS: ERROR");
                eprintln!("MESSAGE: Не была предоставлена непустая мапа Field ID (--upload-field-map-json).");
                return Err(anyhow!("Не была предоставлена непустая мапа Field ID (--upload-field-map-json)."));
            }

            for header in &data.headers {
                if !field_map.contains_key(header) {
                    eprintln!("Предупреждение: Для извлеченного заголовка '{}' не найден Field ID в предоставленной мапе.", header);
                }
            }

            file_loader::write_excel(&data, args.output_xlsx_path.to_str().ok_or_else(|| anyhow!("Неверный путь для сохранения XLSX"))?)?;

            upload_to_true_tabs(data, &args.upload_api_token, &args.upload_datasheet_id, &field_map).await?;

            println!("STATUS: SUCCESS");
            println!("FILE_PATH: {}", args.output_xlsx_path.display());

            Ok(())
        }
        Err(e) => {
            eprintln!("STATUS: ERROR");
            eprintln!("MESSAGE: {}", e);
            Err(e)
        }
    }
}