from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from typing import List, Dict, Any, Optional

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
    # Кнопка Отмена теперь на главном меню не нужна, она есть в других флоу
    # builder.row(
    #     InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")
    # )
    return builder.as_markup()

def source_selection_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    # Убраны Labguru, Cassandra, Neo4j, Couchbase
    sources = [
        ("PostgreSQL", "postgres"), ("MySQL", "mysql"), ("SQLite", "sqlite"),
        ("MongoDB", "mongodb"), ("Redis", "redis"), ("Elasticsearch", "elasticsearch"),
        ("CSV файл", "csv"), ("Excel файл", "excel"),
        # Удаленные источники:
        # ("Labguru", "labguru"),
        # ("Cassandra", "cassandra"),
        # ("Neo4j", "neo4j"),
        # ("Couchbase", "couchbase"),
    ]
    for text, source_type in sources:
        builder.button(text=text, callback_data=f"start_upload_process:{source_type}")

    builder.adjust(2) # По 2 кнопки в ряд
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
    current_page_start = current_offset + 1
    current_page_end = min(current_offset + limit, total_records)
    builder.button(text=f"{current_page_start}-{current_page_end} из {total_records}", callback_data="ignore")

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
        InlineKeyboardButton(text="⬅️ Назад", callback_data="manage_configs")
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
        InlineKeyboardButton(text="⬅️ Назад", callback_data="manage_configs")
    )
    return builder.as_markup()

def select_input_method_keyboard(config_type: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    # callback_data теперь включает config_type для хэндлера
    builder.row(
        InlineKeyboardButton(text="✏️ Ввести вручную", callback_data=f"select_input_method:manual:{config_type}")
    )
    builder.row(
        InlineKeyboardButton(text="💾 Использовать сохраненную", callback_data=f"select_input_method:saved:{config_type}")
    )
    builder.row(
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")
    )
    return builder.as_markup()


def select_config_keyboard(configs: List[Dict[str, Any]], callback_prefix: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if not configs:
        builder.row(InlineKeyboardButton(text="Список пуст", callback_data="ignore"))
    else:
        for config in configs:
            text = config['name']
            if config.get('source_type'):
                 text += f" ({config['source_type']})"
            elif config.get('upload_datasheet_id'):
                 text += f" (Datasheet ID: {config['upload_datasheet_id']})"

            # Добавляем индикатор дефолтной конфигурации
            if config.get('is_default'):
                 text += " ⭐"

            builder.row(InlineKeyboardButton(text=text, callback_data=f"select_config:{callback_prefix}:{config['name']}"))

    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"))
    return builder.as_markup()

def config_actions_keyboard(config: Dict[str, Any], config_type: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_{config_type}_config:{config['name']}"))
    builder.row(InlineKeyboardButton(text="🗑️ Удалить", callback_data=f"delete_{config_type}_config_confirm:{config['name']}"))

    # Кнопка "Сделать по умолчанию" только если не дефолтная
    if not config.get('is_default'):
        builder.row(InlineKeyboardButton(text="⭐ Сделать по умолчанию", callback_data=f"set_default_{config_type}_config:{config['name']}"))

    builder.row(InlineKeyboardButton(text="⬅️ Назад к списку", callback_data=f"list_{config_type}_configs"))
    return builder.as_markup()

def delete_confirm_keyboard(config_name: str, config_type: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Подтвердить удаление", callback_data=f"delete_{config_type}_config:{config_name}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"{config_type}_actions:{config_name}")
    )
    return builder.as_markup()

def operation_in_progress_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="❌ Отменить операцию", callback_data="cancel_operation")
    )
    return builder.as_markup()