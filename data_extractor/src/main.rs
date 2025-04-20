use anyhow::{Result, anyhow};
use clap::Parser;
use dotenv::dotenv;
use std::env;

// Импортируем модули базы данных и загрузчика файлов
mod db;
mod file_loader;

// Импортируем конкретные типы sqlx, необходимые для прямого выполнения SQL
// Это понадобится, т.к. универсальная функция extract_from_sql отключена
use sqlx::{PgPool, MySqlPool, SqlitePool, Executor, Row, Column}; // Добавлен Column
use sqlx::types::{JsonValue, chrono::NaiveDateTime, BigDecimal}; // Импортируем типы, необходимые для получения данных


// Определяем структуру аргументов командной строки с помощью clap
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Data source type (e.g., postgres, mysql, sqlite, mongodb, redis, clickhouse, influxdb, elasticsearch, neo4j, couchbase, excel, csv, mssql)
    #[arg(short, long)]
    source: String,

    /// Database URL or file path
    #[arg(short, long)]
    connection: String,

    /// Database name (for sources like MongoDB)
    #[arg(long)]
    db_name: Option<String>,

    /// Collection name or keyspace (for sources like MongoDB, Cassandra)
    #[arg(long)]
    collection: Option<String>,

    /// Query string (for database sources)
    #[arg(short, long)]
    query: Option<String>,

    /// Key pattern (for Redis)
    #[arg(long)]
    key_pattern: Option<String>,

    /// Output file path (e.g., output.xlsx)
    #[arg(short, long)]
    output: String,

    /// User for database authentication (e.g., for Neo4j, Couchbase)
    #[arg(short, long)]
    user: Option<String>,

    /// Password for database authentication (e.g., for Neo4j, Couchbase)
    #[arg(short, long)]
    pass: Option<String>,

    /// Organization (for InfluxDB)
    #[arg(long)]
    org: Option<String>,

    /// Bucket (for InfluxDB)
    #[arg(long)]
    bucket: Option<String>,

    /// Index (for Elasticsearch)
    #[arg(long)]
    index: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok(); // Загружаем переменные окружения из файла .env

    let args = Args::parse();

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


    let extracted_data = match source_type.as_str() {
        // --- Включенные SQL Источники (используем конкретные типы пулов и выполняем запрос здесь) ---
        "postgres" => {
            let pool = db::sql::get_postgres_pool(&db_url).await?;
            let query_str = query.ok_or_else(|| anyhow!("Query is required for PostgreSQL"))?;
            println!("Выполнение SQL запроса: {}", query_str);
            // Выполняем запрос напрямую с конкретным пулом
            let rows = sqlx::query(&query_str)
                .fetch_all(&pool) // Используем конкретный PgPool
                .await?;

            // Конвертируем sqlx строки в ExtractedData (логика из закомментированной универсальной функции)
            if rows.is_empty() {
                println!("PostgreSQL запрос вернул 0 строк.");
                db::ExtractedData { headers: vec![], rows: vec![] }
            } else {
                let headers: Vec<String> = rows[0].columns().iter().map(|col| col.name().to_string()).collect();
                let data_rows: Vec<Vec<String>> = rows.into_iter().map(|row| {
                    headers.iter().enumerate().map(|(i, _)| { // Итерируемся по индексу
                        // Используем try_get с конкретными типами (совместимы с типами Postgres)
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
                db::ExtractedData { headers, rows: data_rows }
            }
        }
        "mysql" => {
            let pool = db::sql::get_mysql_pool(&db_url).await?;
            let query_str = query.ok_or_else(|| anyhow!("Query is required for MySQL"))?;
            println!("Выполнение SQL запроса: {}", query_str);
            // Выполняем запрос напрямую с конкретным пулом
            let rows = sqlx::query(&query_str)
                .fetch_all(&pool) // Используем конкретный MySqlPool
                .await?;

            // Конвертируем sqlx строки в ExtractedData (логика как для Postgres)
            if rows.is_empty() {
                println!("MySQL запрос вернул 0 строк.");
                db::ExtractedData { headers: vec![], rows: vec![] }
            } else {
                let headers: Vec<String> = rows[0].columns().iter().map(|col| col.name().to_string()).collect();
                let data_rows: Vec<Vec<String>> = rows.into_iter().map(|row| {
                    headers.iter().enumerate().map(|(i, _)| {
                        // Используем try_get с конкретными типами (совместимы с типами MySQL)
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
                db::ExtractedData { headers, rows: data_rows }
            }
        }
        "sqlite" => {
            let pool = db::sql::get_sqlite_pool(&db_url).await?;
            let query_str = query.ok_or_else(|| anyhow!("Query is required for SQLite"))?;
            println!("Выполнение SQL запроса: {}", query_str);
            // Выполняем запрос напрямую с конкретным пулом
            let rows = sqlx::query(&query_str)
                .fetch_all(&pool) // Используем конкретный SqlitePool
                .await?;

            // Конвертируем sqlx строки в ExtractedData
            if rows.is_empty() {
                println!("SQLite запрос вернул 0 строк.");
                db::ExtractedData { headers: vec![], rows: vec![] }
            } else {
                let headers: Vec<String> = rows[0].columns().iter().map(|col| col.name().to_string()).collect();
                let data_rows: Vec<Vec<String>> = rows.into_iter().map(|row| {
                    headers.iter().enumerate().map(|(i, _)| {
                        // Используем try_get с конкретными типами.
                        // Убираем попытку читать BigDecimal, т.к. он не поддерживается для SQLite.
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
                        //  else if let Ok(Some(bd_val)) = row.try_get::<Option<BigDecimal>, usize>(i) { // <--- Закомментируйте или удалите эту строку
                        //       bd_val.to_string() // <--- Закомментируйте или удалите эту строку
                        //  } // <--- Закомментируйте или удалите эту строку
                        else {
                            "".to_string() // Любые другие типы или NULL будут преобразованы в пустую строку
                        };
                        string_value
                    }).collect()
                }).collect();
                println!("SQLite запрос успешно выполнен. Извлечено {} строк.", data_rows.len());
                db::ExtractedData { headers, rows: data_rows }
            }
        }
        // --- Включенные NoSQL Источники ---
        "mongodb" => {
            let db_name_str = db_name.ok_or_else(|| anyhow!("Database name is required for MongoDB"))?;
            let collection_str = collection.ok_or_else(|| anyhow!("Collection name is required for MongoDB"))?;
            db::nosql::extract_from_mongodb(&db_url, &db_name_str, &collection_str).await?
        }
        "redis" => {
            let key_pattern_str = key_pattern.ok_or_else(|| anyhow!("Key pattern is required for Redis"))?;
            db::nosql::extract_from_redis(&db_url, &key_pattern_str).await?
        }
        "elasticsearch" => {
            let index_str = index.ok_or_else(|| anyhow!("Index is required for Elasticsearch"))?;
            let query_str = query.ok_or_else(|| anyhow!("Query (JSON) is required for Elasticsearch"))?;
            let query_json: JsonValue = serde_json::from_str(&query_str)?;
            db::nosql::extract_from_elasticsearch(&db_url, &index_str, query_json).await?
        }

        // --- Включенные Файловые Источники ---
        "csv" => {
            file_loader::read_csv(&db_url)? // connection is treated as file path for files
        }

        _ => return Err(anyhow!("Unsupported data source type: {}", source_type)),
    };

    // Записываем извлеченные данные в выходной файл (предполагаем, что write_excel обрабатывает .xlsx)
    if output_path.to_lowercase().ends_with(".xlsx") {
        file_loader::write_excel(&extracted_data, &output_path)
            .map_err(|e| anyhow!("Failed to write to XLSX file {}: {}", output_path, e))?;
    } else {
        // Обрабатываем другие форматы вывода, если необходимо, или возвращаем ошибку
        return Err(anyhow!("Unsupported output file format. Only .xlsx is supported."));
    }


    println!("Data extraction and saving complete.");

    Ok(())
}