from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

def main_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="⚙️ Выбрать источник данных", callback_data="select_source")
    )
    builder.row(
        InlineKeyboardButton(text="📊 История загрузок", callback_data="view_history:0")
    )
    builder.row(
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")
    )
    return builder.as_markup()

def source_selection_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    sources = [
        ("PostgreSQL", "postgres"), ("MySQL", "mysql"), ("SQLite", "sqlite"), ("MSSQL", "mssql"),
        ("MongoDB", "mongodb"), ("Redis", "redis"), ("Cassandra/ScyllaDB", "cassandra"),
        ("ClickHouse", "clickhouse"), ("InfluxDB", "influxdb"), ("Elasticsearch", "elasticsearch"),
        ("Excel файл", "excel"), ("CSV файл", "csv"),
        ("Neo4j", "neo4j"), ("Couchbase", "couchbase"),
    ]
    for text, source_type in sources:
        builder.button(text=text, callback_data=f"select_source:{source_type}")

    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="main_menu"))
    return builder.as_markup()

def history_pagination_keyboard(current_offset: int, total_records: int, limit: int = 10) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    prev_offset = max(0, current_offset - limit)
    next_offset = current_offset + limit
    has_prev = current_offset > 0
    has_next = next_offset < total_records

    if has_prev:
        builder.button(text="⬅️", callback_data=f"view_history:{prev_offset}")
    current_page_end = min(current_offset + limit, total_records)
    builder.button(text=f"{current_offset + 1}-{current_page_end} из {total_records}", callback_data="ignore")

    if has_next:
        builder.button(text="➡️", callback_data=f"view_history:{next_offset}")

    builder.row(InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="main_menu"))

    return builder.as_markup()