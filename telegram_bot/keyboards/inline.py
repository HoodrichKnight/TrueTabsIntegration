from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from typing import List, Dict, Any, Optional

# Copy of inline.py with added Export and Update buttons in main menu

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
        InlineKeyboardButton(text="📅 Запланированные задания", callback_data="manage_schedules")
    )
    builder.row(
        InlineKeyboardButton(text="📤 Выгрузить данные", callback_data="export_data")
    )
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить данные", callback_data="update_data")
    )
    builder.row(
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")
    )
    return builder.as_markup()

# The rest of the keyboards are copied as is from inline.py

def manage_schedules_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="➕ Добавить новое задание", callback_data="add_schedule")
    )
    builder.row(
        InlineKeyboardButton(text="📋 Список заданий", callback_data="list_schedules")
    )
    builder.row(
        InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="main_menu")
    )
    return builder.as_markup()

def source_selection_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    sources = [
        ("PostgreSQL", "postgres"), ("MySQL", "mysql"), ("SQLite", "sqlite"),
        ("MongoDB", "mongodb"), ("Redis", "redis"), ("Elasticsearch", "elasticsearch"),
        ("CSV файл", "csv"),
    ]
    for text, source_type in sources:
        builder.button(text=text, callback_data=f"start_upload_process:{source_type}")

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
                 # Remove Excel file from display
                 if config['source_type'].lower() == 'excel':
                     continue
                 text += f" ({config['source_type']})"
            elif config.get('upload_datasheet_id'):
                 text += f" (Datasheet ID: {config['upload_datasheet_id']})"

            if config.get('is_default'):
                 text += " ⭐"

            builder.row(InlineKeyboardButton(text=text, callback_data=f"select_config:{callback_prefix}:{config['name']}"))


    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"))
    return builder.as_markup()


def config_actions_keyboard(config: Dict[str, Any], config_type: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_{config_type}_config:{config['name']}"))
    builder.row(InlineKeyboardButton(text="🗑️ Удалить", callback_data=f"delete_{config_type}_config_confirm:{config['name']}"))

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

def select_schedule_action_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="Извлечь данные (extract)", callback_data="select_schedule_action:extract"))

    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"))
    return builder.as_markup()

def select_schedule_trigger_type_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="По интервалу (Interval)", callback_data="select_trigger_type:interval"))
    builder.row(InlineKeyboardButton(text="По расписанию (Cron)", callback_data="select_trigger_type:cron"))
    builder.row(InlineKeyboardButton(text="Один раз в дату/время (Date)", callback_data="select_trigger_type:date"))

    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"))
    return builder.as_markup()

def confirm_schedule_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Подтвердить и создать", callback_data="confirm_create_schedule"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")
    )
    return builder.as_markup()

def weather_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🏙️ Погода в городе", callback_data="weather_by_city")
    )
    builder.row(
        InlineKeyboardButton(text="🗺️ Погода по координатам", callback_data="weather_by_coords")
    )
    builder.row(
        InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="main_menu")
    )
    return builder.as_markup()

def select_forecast_period_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    # Periods supported by OpenWeatherMap Free API (3-hour intervals, up to 5 days)
    # Mapping user-friendly periods to API intervals/summaries
    builder.row(
        InlineKeyboardButton(text="Сейчас", callback_data="weather_period:now"),
        InlineKeyboardButton(text="Ближайший час", callback_data="weather_period:1h"),
        InlineKeyboardButton(text="Ближайшие 3 часа", callback_data="weather_period:3h"),
    )
    builder.row(
        InlineKeyboardButton(text="На сегодня (до конца дня)", callback_data="weather_period:today"), # Will require summarizing 3h intervals
    )
    builder.row(
        InlineKeyboardButton(text="На 1 день", callback_data="weather_period:1d"), # Will require summarizing from forecast
        InlineKeyboardButton(text="На 3 дня", callback_data="weather_period:3d"), # Will require summarizing from forecast
    )
    # Consider if Week/Month are feasible with paid API or need external logic
    builder.row(
        InlineKeyboardButton(text="На неделю", callback_data="weather_period:7d"),
        InlineKeyboardButton(text="На месяц", callback_data="weather_period:30d"),
    )
    builder.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="weather_menu")) # Back to weather menu
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")) # Added Cancel button

    # Note: Week and Month forecast require paid API or external logic to summarize data

    return builder.as_markup()
