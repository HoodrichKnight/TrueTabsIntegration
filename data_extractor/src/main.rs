// data_extractor/src/main.rs

use anyhow::{Result, anyhow};
use clap::Parser;
use dotenv::dotenv;
use std::env;
mod db;
mod file_loader;
use sqlx::{Executor, Row, Column};
use sqlx::types::{JsonValue, chrono::NaiveDateTime, BigDecimal};
use serde_json::json;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    source: String,

    #[arg(short, long)]
    connection: String,

    #[arg(long)]
    db_name: Option<String>,

    #[arg(long)]
    collection: Option<String>,

    #[arg(short, long)]
    query: Option<String>,

    #[arg(long)]
    key_pattern: Option<String>,

    #[arg(short, long)]
    output: String,

    #[arg(short, long)]
    user: Option<String>,

    #[arg(short, long)]
    pass: Option<String>,

    #[arg(long)]
    org: Option<String>,

    #[arg(long)]
    bucket: Option<String>,

    #[arg(long)]
    index: Option<String>,

    #[arg(long)]
    action: String,

    #[arg(long)]
    record_id: Option<String>,

    #[arg(long)]
    field_updates: Option<String>,

    #[arg(long, value_parser = parse_json_string)]
    pub expected_headers: Option<Vec<String>>,
}

fn parse_json_string(arg: &str) -> Result<Vec<String>, String> {
    serde_json::from_str(arg).map_err(|e| format!("Invalid JSON string: {}", e))
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let args = Args::parse();
    let action = args.action.to_lowercase();
    let source_type = args.source.to_lowercase();
    let db_url = args.connection;
    let query = args.query;
    let output_path = args.output;
    let db_name = args.db_name;
    let collection = args.collection;
    let key_pattern = args.key_pattern;
    let user = args.user;
    let pass = args.pass;
    let org = args.org;
    let bucket = args.bucket;
    let index = args.index;


    match action.as_str() {
        "extract" => {
            let extracted_data = match source_type.as_str() {
                "postgres" => {
                    let pool = db::sql::get_postgres_pool(&db_url).await?;
                    let query_str = query.ok_or_else(|| anyhow!("Query is required for PostgreSQL"))?;
                    println!("Выполнение SQL запроса: {}", query_str);
                    let rows = sqlx::query(&query_str)
                        .fetch_all(&pool)
                        .await?;

                    if rows.is_empty() {
                        println!("PostgreSQL запрос вернул 0 строк.");
                        db::ExtractedData { headers: vec![], rows: vec![] }
                    } else {
                        let actual_headers: Vec<String> = rows[0].columns().iter().map(|col| col.name().to_string()).collect();

                        if let Some(expected) = &args.expected_headers {
                            if actual_headers != *expected {
                                let expected_str = expected.join(", ");
                                let actual_str = actual_headers.join(", ");
                                let error_msg = format!("Column mismatch: Expected [{}], Got [{}]", expected_str, actual_str);
                                return Err(anyhow!(error_msg));
                            }
                        }

                        let data_rows: Vec<Vec<String>> = rows.into_iter().map(|row| {
                            actual_headers.iter().enumerate().map(|(i, _)| {
                                let string_value = if let Ok(Some(s)) = row.try_get::<Option<String>, usize>(i) {
                                    s
                                } else if let Ok(Some(i_val)) = row.try_get::<Option<i64>, usize>(i) {
                                    i_val.to_string()
                                } else if let Ok(Some(f_val)) = row.try_get::<Option<f64>, usize>(i) {
                                    f_val.to_string()
                                } else if let Ok(Some(b_val)) = row.try_get::<Option<bool>, usize>(i) {
                                    b_val.to_string()
                                } else if let Ok(Some(json_val)) = row.try_get::<Option<JsonValue>, usize>(i) {
                                    json_val.to_string()
                                } else if let Ok(Some(dt_val)) = row.try_get::<Option<NaiveDateTime>, usize>(i) {
                                    dt_val.to_string()
                                } else if let Ok(Some(bd_val)) = row.try_get::<Option<BigDecimal>, usize>(i) {
                                    bd_val.to_string()
                                }
                                else {
                                    "".to_string()
                                };
                                string_value
                            }).collect()
                        }).collect();

                        println!("PostgreSQL запрос успешно выполнен. Извлечено {} строк.", data_rows.len());
                        db::ExtractedData { headers: actual_headers, rows: data_rows }
                    }
                }
                "mysql" => {
                    let pool = db::sql::get_mysql_pool(&db_url).await?;
                    let query_str = query.ok_or_else(|| anyhow!("Query is required for MySQL"))?;
                    println!("Выполнение SQL запроса: {}", query_str);
                    let rows = sqlx::query(&query_str)
                        .fetch_all(&pool)
                        .await?;

                    if rows.is_empty() {
                        println!("MySQL запрос вернул 0 строк.");
                        db::ExtractedData { headers: vec![], rows: vec![] }
                    } else {
                        let actual_headers: Vec<String> = rows[0].columns().iter().map(|col| col.name().to_string()).collect();

                        if let Some(expected) = &args.expected_headers {
                            if actual_headers != *expected {
                                let expected_str = expected.join(", ");
                                let actual_str = actual_headers.join(", ");
                                let error_msg = format!("Column mismatch: Expected [{}], Got [{}]", expected_str, actual_str);
                                return Err(anyhow!(error_msg));
                            }
                        }

                        let data_rows: Vec<Vec<String>> = rows.into_iter().map(|row| {
                            actual_headers.iter().enumerate().map(|(i, _)| {
                                let string_value = if let Ok(Some(s)) = row.try_get::<Option<String>, usize>(i) {
                                    s
                                } else if let Ok(Some(i_val)) = row.try_get::<Option<i64>, usize>(i) {
                                    i_val.to_string()
                                } else if let Ok(Some(f_val)) = row.try_get::<Option<f64>, usize>(i) {
                                    f_val.to_string()
                                } else if let Ok(Some(b_val)) = row.try_get::<Option<bool>, usize>(i) {
                                    b_val.to_string()
                                } else if let Ok(Some(json_val)) = row.try_get::<Option<JsonValue>, usize>(i) {
                                    json_val.to_string()
                                } else if let Ok(Some(dt_val)) = row.try_get::<Option<NaiveDateTime>, usize>(i) {
                                    dt_val.to_string()
                                } else if let Ok(Some(bd_val)) = row.try_get::<Option<BigDecimal>, usize>(i) {
                                    bd_val.to_string()
                                }
                                else {
                                    "".to_string()
                                };
                                string_value
                            }).collect()
                        }).collect();

                        println!("MySQL запрос успешно выполнен. Извлечено {} строк.", data_rows.len());
                        db::ExtractedData { headers: actual_headers, rows: data_rows }
                    }
                }
                "sqlite" => {
                    let pool = db::sql::get_sqlite_pool(&db_url).await?;
                    let query_str = query.ok_or_else(|| anyhow!("Query is required for SQLite"))?;
                    println!("Выполнение SQL запроса: {}", query_str);
                    let rows = sqlx::query(&query_str)
                        .fetch_all(&pool)
                        .await?;

                    if rows.is_empty() {
                        println!("SQLite запрос вернул 0 строк.");
                        db::ExtractedData { headers: vec![], rows: vec![] }
                    } else {
                        let actual_headers: Vec<String> = rows[0].columns().iter().map(|col| col.name().to_string()).collect();

                        if let Some(expected) = &args.expected_headers {
                            if actual_headers != *expected {
                                let expected_str = expected.join(", ");
                                let actual_str = actual_headers.join(", ");
                                let error_msg = format!("Column mismatch: Expected [{}], Got [{}]", expected_str, actual_str);
                                return Err(anyhow!(error_msg));
                            }
                        }

                        let data_rows: Vec<Vec<String>> = rows.into_iter().map(|row| {
                            actual_headers.iter().enumerate().map(|(i, _)| {
                                let string_value = if let Ok(Some(s)) = row.try_get::<Option<String>, usize>(i) {
                                    s
                                } else if let Ok(Some(i_val)) = row.try_get::<Option<i64>, usize>(i) {
                                    i_val.to_string()
                                } else if let Ok(Some(f_val)) = row.try_get::<Option<f64>, usize>(i) {
                                    f_val.to_string()
                                } else if let Ok(Some(b_val)) = row.try_get::<Option<bool>, usize>(i) {
                                    b_val.to_string()
                                } else if let Ok(Some(json_val)) = row.try_get::<Option<JsonValue>, usize>(i) {
                                    json_val.to_string()
                                } else if let Ok(Some(dt_val)) = row.try_get::<Option<NaiveDateTime>, usize>(i) {
                                    dt_val.to_string()
                                }
                                else {
                                    "".to_string()
                                };
                                string_value
                            }).collect()
                        }).collect();

                        println!("SQLite запрос успешно выполнен. Извлечено {} строк.", data_rows.len());
                        db::ExtractedData { headers: actual_headers, rows: data_rows }
                    }
                }
                "mongodb" => {
                    let db_name_str = db_name.ok_or_else(|| anyhow!("Database name is required for MongoDB"))?;
                    let collection_str = collection.ok_or_else(|| anyhow!("Collection name is required for MongoDB"))?;
                    db::nosql::extract_from_mongodb(&db_url, &db_name_str, &collection_str, args.expected_headers.clone()).await?
                }
                "redis" => {
                    let key_pattern_str = key_pattern.ok_or_else(|| anyhow!("Key pattern is required for Redis"))?;
                    db::nosql::extract_from_redis(&db_url, &key_pattern_str, args.expected_headers.clone()).await?
                }
                "elasticsearch" => {
                    let index_str = index.ok_or_else(|| anyhow!("Index is required for Elasticsearch"))?;
                    let query_str = query.ok_or_else(|| anyhow!("Query (JSON) is required for Elasticsearch"))?;
                    let query_json: JsonValue = serde_json::from_str(&query_str)?;
                    db::nosql::extract_from_elasticsearch(&db_url, &index_str, query_json, args.expected_headers.clone()).await?
                }
                "csv" => {
                    file_loader::read_csv(&db_url, args.expected_headers.clone())?
                }
                _ => return Err(anyhow!("Unsupported source type for extract action: {}", source_type)),
            };

            if output_path.to_lowercase().ends_with(".xlsx") {
                file_loader::write_excel(&extracted_data, &output_path)
                    .map_err(|e| anyhow!("Failed to write to XLSX file {}: {}", output_path, e))?;
            } else {
                return Err(anyhow!("Unsupported output file format. Only .xlsx is supported for extract action."));
            }

            println!("Data extraction and saving complete.");
        }
        "update" => {
            match source_type.as_str() {
                "truetabs" => {
                    let api_token = env::var("TRUETABS_API_TOKEN").map_err(|_| anyhow!("TRUETABS_API_TOKEN not set in .env"))?;
                    let datasheet_id = collection.ok_or_else(|| anyhow!("Datasheet ID (use --collection) is required for TrueTabs update"))?;
                    let record_id_str = args.record_id.ok_or_else(|| anyhow!("--record-id is required for TrueTabs update"))?;
                    let field_updates_str = args.field_updates.ok_or_else(|| anyhow!("--field-updates (JSON string) is required for TrueTabs update"))?;
                    let field_key = "name"; // Assuming "name" for simplicity

                    let field_updates_json: JsonValue = serde_json::from_str(&field_updates_str)
                        .map_err(|e| anyhow!("Invalid JSON for --field-updates: {}", e))?;

                    let update_payload = json!({
                        "recordId": record_id_str,
                        "fields": field_updates_json
                    });
                    let updates_vec = vec![update_payload];

                    println!("Calling TrueTabs update_records...");
                    db::truetabs::update_records(&api_token, &datasheet_id, field_key, updates_vec).await?;
                    println!("TrueTabs update response: Data updated successfully."); // Simplified success message
                    println!("Data update complete.");
                }
                _ => return Err(anyhow!("Unsupported source type for update action: {}. Only 'truetabs' is supported.", source_type)),
            }
        }
        _ => return Err(anyhow!("Unsupported action: {}. Use 'extract' or 'update'.", action)),
    }


    Ok(())
}