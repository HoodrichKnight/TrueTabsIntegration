// data_extractor/src/db/sql.rs
use anyhow::{Result, anyhow};
use std::{collections::HashMap, time::Duration}; // Убедитесь, что HashMap и Duration используются, иначе удалите

// Используем sqlx с явными импортами. Импортируем PoolOptions явно из sqlx::pool.
use sqlx::{
    Executor, Pool, Row, Column, Database, Arguments, Type, Decode, ColumnIndex,
    postgres::{PgPool, Postgres}, // Импортируем типы баз данных
    mysql::{MySqlPool, MySql},   // Импортируем типы баз данных
    sqlite::{SqlitePool, Sqlite}, // Импортируем типы баз данных
    pool::PoolOptions, // Импортируем общий тип PoolOptions из sqlx::pool
    Error as SqlxError,
    types::{JsonValue, chrono::NaiveDateTime, BigDecimal},
};

// // Импорты для Tiberius (MSSQL) - временно отключены
// use tiberius::{
//     Client, Config, Row as TiberiusRow, error::Error as TiberiusError,
//     Value, QueryItem // Импортируем Value и QueryItem напрямую из tiberius
// };
// use tokio::net::TcpStream; // Временно отключен для MSSQL
// use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt}; // Временно отключен для MSSQL


use crate::db::ExtractedData; // Исправлен путь импорта


// --- Функции get_pool для разных SQL баз на sqlx ---

pub async fn get_postgres_pool(database_url: &str) -> Result<PgPool> {
    println!("Подключение к PostgreSQL...");
    let pool = PoolOptions::<Postgres>::new()
        .max_connections(5)
        .connect(database_url)
        .await?;
    println!("Подключение к PostgreSQL успешно установлено.");
    Ok(pool)
}

pub async fn get_mysql_pool(database_url: &str) -> Result<MySqlPool> {
    println!("Подключение к MySQL...");
    let pool = PoolOptions::<MySql>::new()
        .max_connections(5)
        .connect(database_url)
        .await?;
    println!("Подключение к MySQL успешно установлено.");
    Ok(pool)
}

pub async fn get_sqlite_pool(database_url: &str) -> Result<SqlitePool> {
    println!("Подключение к SQLite...");
    let pool = PoolOptions::<Sqlite>::new()
        .max_connections(1) // SQLite обычно однопоточное
        .connect(database_url)
        .await?;
    println!("Подключение к SQLite успешно установлено.");
    Ok(pool)
}


// --- Универсальная функция извлечения из SQL пула (для sqlx) ---

// Уточняем трейт-границы
/*
pub async fn extract_from_sql<DB>(pool: &Pool<DB>, query: &str) -> Result<ExtractedData>
where
    DB: Database, // DB должна быть базой данных sqlx
// Требуем, чтобы Row для этой базы данных реализовал нужные трейты
    <DB as Database>::Row: sqlx::Row<Database = DB> + sqlx::ColumnIndex<usize> + for<'a> sqlx::ColumnIndex<&'a str>,
// Требуем, чтобы типы могли быть декодированы и типизированы для этой базы данных для ЛЮБОГО времени жизни 'a
    for<'a> Option<String>: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    for<'a> Option<i64>: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    for<'a> Option<f64>: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    for<'a> Option<bool>: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    for<'a> Option<JsonValue>: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    for<'a> Option<NaiveDateTime>: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    for<'a> Option<BigDecimal>: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
// Добавьте другие for<'a> Option<Тип>: sqlx::Type<DB> + sqlx::Decode<'a, DB>, по мере необходимости
// Требуем, чтобы тип аргументов для этой базы данных реализовал IntoArguments для ЛЮБОГО времени жизни 'a
    for<'a> <DB as sqlx::Database>::Arguments<'a>: sqlx::IntoArguments<'a, DB>,
// Требуем, чтобы Pool реализовывал Executor для выполнения запросов
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
// Требуем, чтобы тип аргументов запроса (пустой ()) реализовал Execute
    for<'q> sqlx::query::Query<'q, DB, ()>: sqlx::Execute<'q, DB>,
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
        headers.iter().enumerate().map(|(i, _)| {
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
} */


// // --- Реализация для MSSQL с использованием Tiberius - временно отключена ---
//
// fn parse_tiberius_config(database_url: &str) -> Result<Config, TiberiusError> {
//     Config::from_ado_string(database_url)
// }
//
//
// pub async fn extract_from_mssql(database_url: &str, query: &str) -> Result<ExtractedData> {
//     println!("Подключение к MSSQL...");
//
//     // Заглушка вместо реальной реализации
//     println!("Предупреждение: Поддержка MSSQL временно отключена из-за проблем сборки.");
//     Err(anyhow!("Поддержка MSSQL временно отключена из-за проблем сборки."))
//
//     /*
//     // Реальный код для MSSQL (закомментирован)
//     let config = parse_tiberius_config(database_url)
//         .map_err(|e| anyhow!("Ошибка парсинга строки подключения MSSQL: {}", e))?;
//
//     let tcp = TcpStream::connect(config.get_addr()).await
//         .map_err(|e| anyhow!("Ошибка TCP подключения к MSSQL {}: {}", config.get_addr(), e))?;
//     tcp.set_nodelay(true).map_err(|e| anyhow!("Ошибка set_nodelay для MSSQL TCP: {}", e))?;
//
//     let tcp = tcp.compat(); // Используем .compat() из tokio_util::compat
//
//     let mut client = Client::connect(config, tcp).await
//          .map_err(|e| anyhow!("Ошибка подключения к MSSQL: {}", e))?;
//
//     println!("Подключение к MSSQL успешно установлено.");
//     println!("Выполнение MSSQL запроса: {}", query);
//
//     let mut stream = client.simple_query(query).await
//         .map_err(|e| anyhow!("Ошибка выполнения MSSQL запроса: {}", e))?
//         .into_stream();
//
//     let mut headers: Vec<String> = Vec::new();
//     let mut data_rows: Vec<Vec<String>> = Vec::new();
//     let mut headers_extracted = false;
//
//     while let Some(item) = stream.next().await {
//         let query_item = item.map_err(|e| anyhow!("Ошибка получения элемента из MSSQL результата: {}", e))?;
//
//         match query_item {
//             QueryItem::Row(row) => {
//                 if !headers_extracted {
//                     headers = row.columns().iter().map(|col| col.name().to_string()).collect();
//                     headers_extracted = true;
//                 }
//
//                 let mut current_row_data: Vec<String> = Vec::new();
//                 for i in 0..row.len() {
//                     let col_value = row.get(i).to_owned();
//                      let string_value = match col_value {
//                           Some(Value::Sn(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::TinyInt(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::SmallInt(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::Int(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::BigInt(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::Float(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::Real(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::F64(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::Numeric(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::Bit(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::String(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::Raw(val)) => val.map_or("".to_string(), |v| format!("{:?}", v)),
//                           Some(Value::Binary(val)) => val.map_or("".to_string(), |v| format!("{:?}", v)),
//                           Some(Value::Guid(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::Time(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::Date(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::DateTime(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::DateTime2(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::DateTimeOffset(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::SmallDateTime(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           Some(Value::UnalignedDateTime(val)) => val.map_or("".to_string(), |v| format!("{:?}", v)),
//                           Some(Value::Xml(val)) => val.map_or("".to_string(), |v| v.to_string()),
//                           None => "".to_string(),
//                       };
//                      current_row_data.push(string_value);
//                 }
//                 data_rows.push(current_row_data);
//             },
//             QueryItem::Metadata(_) => { /* Игнорируем метаданные */ },
//             QueryItem::DoneProc(_) | QueryItem::DoneProcMap(_) | QueryItem::DoneInProc(_) | QueryItem::DoneInProcMap(_) => { /* Конец набора результатов */ },
//             QueryItem::Error(e) => return Err(anyhow!("MSSQL QueryItem Error: {:?}", e)),
//             _ => { println!("Предупреждение: Получен необработанный тип QueryItem из MSSQL: {:?}", query_item); }
//         }
//     }
//
//     if !headers_extracted && data_rows.is_empty() {
//         println!("MSSQL запрос вернул 0 строк.");
//         Ok(ExtractedData { headers: vec![], rows: vec![] })
//     } else {
//          println!("MSSQL запрос успешно выполнен. Извлечено {} строк.", data_rows.len());
//          Ok(ExtractedData { headers, rows: data_rows })
//     }
//     */
// }