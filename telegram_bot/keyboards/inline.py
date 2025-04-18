from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from typing import List, Dict, Any

def main_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="⚙️ Выбрать источник данных", callback_data="select_source")
    )
    builder.row(
        InlineKeyboardButton(text="📊 История загрузок", callback_data="view_history:0")
    )
    builder.row(
        InlineKeyboardButton(text="💾 Сохраненные конфигурации", callback_data="manage_configs")
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

def upload_confirm_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🚀 Загрузить", callback_data="confirm_upload"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")
    )
    return builder.as_markup()

def manage_configs_menu_keyboard() -> InlineKeyboardMarkup:
    """Меню управления сохраненными конфигурациями."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🔗 Источники данных", callback_data="manage_source_configs")
    )
    builder.row(
        InlineKeyboardButton(text="✅ True Tabs", callback_data="manage_tt_configs")
    )
    builder.row(
        InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="main_menu")
    )
    return builder.as_markup()

def manage_source_configs_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="➕ Добавить новую", callback_data="add_source_config")
    )
    builder.row(
        InlineKeyboardButton(text="📋 Список сохраненных", callback_data="list_source_configs")
    )
    builder.row(
        InlineKeyboardButton(text="⬅️ Назад", callback_data="manage_configs") # Возврат в меню управления конфигами
    )
    return builder.as_markup()

def manage_tt_configs_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="➕ Добавить новую", callback_data="add_tt_config")
    )
    builder.row(
        InlineKeyboardButton(text="📋 Список сохраненных", callback_data="list_tt_configs")
    )
    builder.row(
        InlineKeyboardButton(text="⬅️ Назад", callback_data="manage_configs") # Возврат в меню управления конфигами
    )
    return builder.as_markup()

def list_configs_keyboard(configs: List[Dict[str, Any]], config_type: str, offset: int = 0, limit: int = 10) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if not configs:
        builder.row(InlineKeyboardButton(text="Список пуст", callback_data="ignore"))
    else:
        for config in configs:
            if config_type == 'source':
                 text = f"{config['name']} ({config['source_type']})"
                 callback_prefix = "source_config"
            elif config_type == 'tt':
                 text = f"{config['name']} (Datasheet ID: {config['upload_datasheet_id']})"
                 callback_prefix = "tt_config"
            else:
                 text = config['name']
                 callback_prefix = "config"

            builder.row(InlineKeyboardButton(text=text, callback_data=f"{callback_prefix}_actions:{config['name']}"))

    # TODO: Добавить пагинацию, если списки станут большими

    builder.row(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"manage_{config_type}_configs"))

    return builder.as_markup()

def config_actions_keyboard(config_name: str, config_type: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    # TODO: Кнопка "Использовать" (будет добавлена позже в сценарий загрузки)
    # builder.row(InlineKeyboardButton(text="🚀 Использовать для загрузки", callback_data=f"use_{config_type}_config:{config_name}"))
    # TODO: Кнопка "Редактировать" (сложная реализация)
    # builder.row(InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_{config_type}_config:{config_name}"))
    builder.row(InlineKeyboardButton(text="🗑️ Удалить", callback_data=f"delete_{config_type}_config_confirm:{config_name}")) # Шаг подтверждения удаления
    builder.row(InlineKeyboardButton(text="⬅️ Назад к списку", callback_data=f"list_{config_type}_configs")) # Возврат к списку
    return builder.as_markup()


def delete_confirm_keyboard(config_name: str, config_type: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Подтвердить удаление", callback_data=f"delete_{config_type}_config:{config_name}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"{config_type}_actions:{config_name}") # Возврат к меню действий
    )
    return builder.as_markup()