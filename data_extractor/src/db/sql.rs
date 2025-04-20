use anyhow::{Result};
use sqlx::{
    Executor, Row, Column, Database, Arguments,
    postgres::{PgPool, Postgres},
    mysql::{MySqlPool, MySql},
    sqlite::{SqlitePool, Sqlite},
    pool::PoolOptions,

};

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
