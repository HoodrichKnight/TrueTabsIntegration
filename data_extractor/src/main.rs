use anyhow::{Result, anyhow};
use clap::Parser;
use std::{collections::HashMap, path::PathBuf, time::Instant, io::Write};
use data_extractor::{db::{sql, nosql, ExtractedData}, file_loader};
use serde_json::{json, Value as JsonValue};
use reqwest::{Client, header};
use tokio::time::{sleep, Duration};

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

struct ExecutionResult {
    status: String,
    message: String,
    file_path: Option<PathBuf>,
    duration_seconds: f64,
    extracted_rows: Option<usize>,
    uploaded_records: Option<usize>,
    datasheet_id: Option<String>,
}

fn print_json_result(result: ExecutionResult) {
    let json_output = json!({
        "status": result.status,
        "message": result.message,
        "file_path": result.file_path.and_then(|p| p.to_str().map(|s| s.to_string())),
        "duration_seconds": result.duration_seconds,
        "extracted_rows": result.extracted_rows,
        "uploaded_records": result.uploaded_records,
        "datasheet_id": result.datasheet_id,
    });
    // Выводим JSON в stdout
    let mut stdout = std::io::stdout().lock();
    let _ = writeln!(stdout, "{}", json_output.to_string());
}


#[tokio::main]
async fn main() {
    let args = Args::parse();
    let start_time = Instant::now();

    let datasheet_id_for_result = Some(args.upload_datasheet_id.clone());

    let result = async move {
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
                let es_query: JsonValue = serde_json::from_str(&query_str).map_err(|e| anyhow!("Ошибка парсинга --es-query JSON: {}", e))?;
                nosql::extract_from_elasticsearch(&url, &index, es_query).await
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

        let data = extraction_result?;
        println!("Извлечение успешно. Извлечено {} строк.", data.rows.len());

        let extracted_rows_count = data.rows.len();

        if extracted_rows_count == 0 {
            return Ok(ExecutionResult {
                status: "SUCCESS".to_string(),
                message: "Извлечено 0 строк данных. Загрузка не требуется.".to_string(),
                file_path: None, // Нет данных, нет файла
                duration_seconds: start_time.elapsed().as_secs_f64(),
                extracted_rows: Some(0),
                uploaded_records: Some(0),
                datasheet_id: datasheet_id_for_result,
            });
        }

        if data.headers.is_empty() {
            return Err(anyhow!("Отсутствуют заголовки. Невозможно сопоставить с Field ID для загрузки."));
        }


        let field_map: HashMap<String, String> = serde_json::from_str(&args.upload_field_map_json)
            .map_err(|e| anyhow!("Ошибка парсинга --upload-field-map-json: {}", e))?;

        if field_map.is_empty() {
            return Err(anyhow!("Не была предоставлена непустая мапа Field ID (--upload-field-map-json)."));
        }

        for header in &data.headers {
            if !field_map.contains_key(header) {
                eprintln!("Предупреждение: Для извлеченного заголовка '{}' не найден Field ID в предоставленной мапе.", header);
            }
        }

        let file_save_result = file_loader::write_excel(&data, args.output_xlsx_path.to_str().ok_or_else(|| anyhow!("Неверный путь для сохранения XLSX"))?);

        if let Err(e) = file_save_result {
            return Err(anyhow!("Ошибка сохранения XLSX файла: {}", e));
        }
        println!("Данные успешно сохранены в {}.", args.output_xlsx_path.display());

        let base_url = "https://true.tabs.sale/fusion/v1/datasheets/";
        let upload_url = format!("{}{}/records?fieldKey=id", base_url, args.upload_datasheet_id);
        let client = Client::new();

        let batch_size = 1000;
        let total_records = data.rows.len();
        let mut successfully_uploaded_count = 0;

        println!("Начата загрузка {} записей в True Tabs Datasheet: {}", total_records, args.upload_datasheet_id);

        for chunk in data.rows.chunks(batch_size) {
            let mut records_json: Vec<JsonValue> = Vec::new();

            for row in chunk {
                let mut record_object = serde_json::Map::new();
                for (i, header_name) in data.headers.iter().enumerate() {
                    if let Some(field_id) = field_map.get(header_name) {
                        let value_str = row.get(i).unwrap_or(&"".to_string());
                        record_object.insert(field_id.clone(), JsonValue::String(value_str.clone()));
                    }
                }
                records_json.push(JsonValue::Object(record_object));
            }

            let request_body = json!({
                "records": records_json,
                "fieldKey": "id"
            });

            println!("Отправка батча ({} записей)...", chunk.len());

            let response = client.post(&upload_url)
                .header(header::AUTHORIZATION, format!("Bearer {}", args.upload_api_token))
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
                            println!("Батч успешно отправлен.");
                            successfully_uploaded_count += chunk.len();
                        } else {
                            eprintln!("Предупреждение: Батч отправлен, но API вернуло success: false или нестандартный код: {}", response_text);
                        }
                    }
                    Err(_) => {
                        eprintln!("Предупреждение: Батч отправлен, получен статус {}, но не удалось распарсить JSON ответ: {}", status, response_text);
                    }
                }

            } else {
                return Err(anyhow!("Ошибка HTTP статуса при загрузке батча: {} - {}", status, response_text));
            }

            // Небольшая задержка между батчами
            sleep(Duration::from_millis(200)).await;
        }

        Ok(ExecutionResult {
            status: "SUCCESS".to_string(),
            message: format!("Извлечение и загрузка завершены. Извлечено {} записей. Отправлено {} записей в {} батча.",
                             extracted_rows_count, successfully_uploaded_count, (total_records + batch_size - 1) / batch_size),
            file_path: Some(args.output_xlsx_path),
            duration_seconds: start_time.elapsed().as_secs_f64(),
            extracted_rows: Some(extracted_rows_count),
            uploaded_records: Some(successfully_uploaded_count),
            datasheet_id: datasheet_id_for_result,
        })

    }.await;

    let final_result = match result {
        Ok(exec_result) => exec_result,
        Err(e) => {
            // Ошибка произошла
            ExecutionResult {
                status: "ERROR".to_string(),
                message: format!("Ошибка выполнения: {}", e),
                file_path: None,
                duration_seconds: start_time.elapsed().as_secs_f64(),
                extracted_rows: None,
                uploaded_records: None,
                datasheet_id: datasheet_id_for_result,
            }
        }
    };
    print_json_result(final_result);

}