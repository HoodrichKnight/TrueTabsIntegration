use anyhow::{Result, anyhow};
use std::{collections::HashMap, time::Duration};
use sqlx::{
    Executor, Pool, FromRow, Row, Column,
    postgres::PgPool,
    mysql::MySqlPool,
    sqlite::SqlitePool,
    PgPoolOptions, MySqlPoolOptions, SqlitePoolOptions,
    Arguments, Database, Type, Decode, ColumnIndex
};
use sqlx::types::JsonValue;
use sqlx::types::chrono::{NaiveDateTime};
use sqlx::types::BigDecimal;
use sqlx::Error as SqlxError;


use tiberius::{Client, Config, Row as TiberiusRow, error::Error as TiberiusError};
use tokio::net::TcpStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use futures::{TryStreamExt, StreamExt};

use super::ExtractedData;

pub async fn get_postgres_pool(database_url: &str) -> Result<PgPool> {
    println!("Подключение к PostgreSQL...");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;
    println!("Подключение к PostgreSQL успешно установлено.");
    Ok(pool)
}

pub async fn get_mysql_pool(database_url: &str) -> Result<MySqlPool> {
    println!("Подключение к MySQL...");
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;
    println!("Подключение к MySQL успешно установлено.");
    Ok(pool)
}

pub async fn get_sqlite_pool(database_url: &str) -> Result<SqlitePool> {
    println!("Подключение к SQLite...");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;
    println!("Подключение к SQLite успешно установлено.");
    Ok(pool)
}

pub async fn extract_from_sql<DB>(pool: &Pool<DB>, query: &str) -> Result<ExtractedData>
where
    DB: Database,
    for<'a> <DB as Database>::Row: FromRow<'a, DB>,
    for<'a> &'a str: ColumnIndex<<DB as Database>::Row>,
    usize: ColumnIndex<<DB as Database>::Row>,
    Option<String>: for<'a> sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    Option<i64>: for<'a> sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    Option<f64>: for<'a> sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    Option<bool>: for<'a> sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    Option<JsonValue>: for<'a> sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    Option<NaiveDateTime>: for<'a> sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    Option<BigDecimal>: for<'a> sqlx::Type<DB> + sqlx::Decode<'a, DB>,
{
    println!("Выполнение SQL запроса: {}", query);
    let rows: Vec<DB::Row> = sqlx::query(query)
        .fetch_all(pool)
        .await?;

    if rows.is_empty() {
        println!("Запрос вернул 0 строк.");
        return Ok(ExtractedData { headers: vec![], rows: vec![] });
    }

    let headers: Vec<String> = rows[0].columns().iter().map(|col| col.name().to_string()).collect();

    let data_rows: Vec<Vec<String>> = rows.into_iter().map(|row| {
        headers.iter().enumerate().map(|(i, header)| {
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

    Ok(ExtractedData { headers, rows: data_rows })
}

fn parse_tiberius_config(database_url: &str) -> Result<Config, TiberiusError> {
    Config::from_ado_string(database_url)
}


pub async fn extract_from_mssql(database_url: &str, query: &str) -> Result<ExtractedData> {
    println!("Подключение к MSSQL...");

    let config = parse_tiberius_config(database_url)
        .map_err(|e| anyhow!("Ошибка парсинга строки подключения MSSQL: {}", e))?;

    let tcp = TcpStream::connect(config.get_addr()).await
        .map_err(|e| anyhow!("Ошибка TCP подключения к MSSQL {}: {}", config.get_addr(), e))?;
    tcp.set_nodelay(true).map_err(|e| anyhow!("Ошибка set_nodelay для MSSQL TCP: {}", e))?;

    let tcp = tcp.compat();

    let mut client = Client::connect(config, tcp).await
        .map_err(|e| anyhow!("Ошибка подключения к MSSQL: {}", e))?;

    println!("Подключение к MSSQL успешно установлено.");
    println!("Выполнение MSSQL запроса: {}", query);

    let mut stream = client.simple_query(query).await
        .map_err(|e| anyhow!("Ошибка выполнения MSSQL запроса: {}", e))?
        .into_stream();

    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();
    let mut headers_extracted = false;

    while let Some(item) = stream.next().await {
        let row: TiberiusRow = item.map_err(|e| anyhow!("Ошибка получения строки из MSSQL результата: {}", e))?;

        if !headers_extracted {
            headers = row.columns().iter().map(|col| col.name().to_string()).collect();
            headers_extracted = true;
        }

        let mut current_row_data: Vec<String> = Vec::new();
        for col_value in row.iter() {
            let string_value = match col_value.to_owned() {
                Some(tiberius::Value::Sn(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::TinyInt(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::SmallInt(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::Int(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::BigInt(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::Float(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::Real(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::F64(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::Numeric(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::Bit(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::String(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::Raw(val)) => val.map_or("".to_string(), |v| format!("{:?}", v)),
                Some(tiberius::Value::Binary(val)) => val.map_or("".to_string(), |v| format!("{:?}", v)),
                Some(tiberius::Value::Guid(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::Time(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::Date(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::DateTime(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::DateTime2(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::DateTimeOffset(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::SmallDateTime(val)) => val.map_or("".to_string(), |v| v.to_string()),
                Some(tiberius::Value::UnalignedDateTime(val)) => val.map_or("".to_string(), |v| format!("{:?}", v)),
                Some(tiberius::Value::Xml(val)) => val.map_or("".to_string(), |v| v.to_string()),
                None => "".to_string(),
            };
            current_row_data.push(string_value);
        }
        data_rows.push(current_row_data);
    }


    if !headers_extracted && data_rows.is_empty() {
        println!("MSSQL запрос вернул 0 строк.");
        Ok(ExtractedData { headers: vec![], rows: vec![] })
    } else {
        println!("MSSQL запрос успешно выполнен. Извлечено {} строк.", data_rows.len());
        Ok(ExtractedData { headers, rows: data_rows })
    }
}