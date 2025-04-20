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
