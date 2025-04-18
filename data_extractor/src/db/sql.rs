use anyhow::{Result, anyhow};
use sqlx::{Executor, FromRow, postgres::PgPool, mysql::MySqlPool, sqlite::SqlitePool, mssql::MssqlPool, Row, Column};
use crate::db::ExtractedData;

pub async fn extract_from_sql<DB>(pool: &sqlx::Pool<DB>, query: &str) -> Result<ExtractedData>
where
    DB: sqlx::Database,
    for<'a> (usize, sqlx::database::Row<'a, DB>): sqlx::ColumnIndex<sqlx::database::Row<'a, DB>>,
    for<'a> String: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> Option<String>: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> i32: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> Option<i32>: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> i64: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> Option<i64>: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> f32: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> Option<f32>: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> f64: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> Option<f64>: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> bool: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> Option<bool>: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> sqlx::types::chrono::NaiveDateTime: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> Option<sqlx::types::chrono::NaiveDateTime>: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> sqlx::types::uuid::Uuid: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> Option<sqlx::types::uuid::Uuid>: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> sqlx::types::BigDecimal: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> Option<sqlx::types::BigDecimal>: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> sqlx::types::JsonValue: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
    for<'a> Option<sqlx::types::JsonValue>: sqlx::types::Type<DB> + sqlx::decode::Decode<'a, DB>,
{
    println!("Выполнение SQL запроса: {}", query);
    let rows = sqlx::query(query)
        .fetch_all(pool)
        .await?;

    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();

    if rows.is_empty() {
        println!("Запрос вернул пустой результат.");
        return Ok(ExtractedData { headers, rows: data_rows });
    }

    if let Some(first_row) = rows.first() {
        for column in first_row.columns() {
            headers.push(column.name().to_string());
        }
    }

    for row in rows {
        let mut row_values: Vec<String> = Vec::new();
        for i in 0..row.columns().len() {
            let value_str = match row.try_get_unchecked::<Option<String>>(i) {
                Ok(Some(s)) => s,
                Ok(None) => "".to_string(),
                Err(_) => {
                    match row.try_get_unchecked::<Option<i64>>(i) {
                        Ok(Some(n)) => n.to_string(),
                        Ok(None) => "".to_string(),
                        Err(_) => {
                            match row.try_get_unchecked::<Option<f64>>(i) {
                                Ok(Some(f)) => f.to_string(),
                                Ok(None) => "".to_string(),
                                Err(_) => {
                                    match row.try_get_unchecked::<Option<bool>>(i) {
                                        Ok(Some(b)) => b.to_string(),
                                        Ok(None) => "".to_string(),
                                        Err(_) => {
                                            match row.try_get_unchecked::<Option<sqlx::types::chrono::NaiveDateTime>>(i) {
                                                Ok(Some(d)) => d.to_string(),
                                                Ok(None) => "".to_string(),
                                                Err(_) => {
                                                    match row.try_get_unchecked::<Option<sqlx::types::uuid::Uuid>>(i) {
                                                        Ok(Some(u)) => u.to_string(),
                                                        Ok(None) => "".to_string(),
                                                        Err(_) => {
                                                            match row.try_get_unchecked::<Option<sqlx::types::BigDecimal>>(i) {
                                                                Ok(Some(bd)) => bd.to_string(),
                                                                Ok(None) => "".to_string(),
                                                                Err(_) => {
                                                                    match row.try_get_unchecked::<Option<sqlx::types::JsonValue>>(i) {
                                                                        Ok(Some(json_val)) => json_val.to_string(),
                                                                        Ok(None) => "".to_string(),
                                                                        Err(_) => {
                                                                            eprintln!("Предупреждение: Не удалось извлечь значение колонки {} как известный тип. Попытка использовать Debug.", i);
                                                                            match row.try_get_unchecked::<sqlx::ValueRef>(i) {
                                                                                Ok(val_ref) => format!("{:?}", val_ref),
                                                                                Err(_) => "[UNSUPPORTED TYPE]".to_string(),
                                                                            }

                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            };
            row_values.push(value_str);
        }
        data_rows.push(row_values);
    }

    Ok(ExtractedData { headers, rows: data_rows })
}

pub async fn get_postgres_pool(db_url: &str) -> Result<PgPool> {
    println!("Подключение к PostgreSQL...");
    PgPool::connect(db_url).await.map_err(|e| anyhow!("Ошибка подключения к PostgreSQL: {}", e))
}

pub async fn get_mysql_pool(db_url: &str) -> Result<MySqlPool> {
    println!("Подключение к MySQL...");
    MySqlPool::connect(db_url).await.map_err(|e| anyhow!("Ошибка подключения к MySQL: {}", e))
}

pub async fn get_sqlite_pool(db_url: &str) -> Result<SqlitePool> {
    println!("Подключение к SQLite...");
    SqlitePool::connect(db_url).await.map_err(|e| anyhow!("Ошибка подключения к SQLite: {}", e))
}

pub async fn get_mssql_pool(db_url: &str) -> Result<MssqlPool> {
    println!("Подключение к MSSQL...");
    MssqlPool::connect(db_url).await.map_err(|e| anyhow!("Ошибка подключения к MSSQL: {}", e))
}