import os
import asyncio
import json
import sys
from datetime import datetime
from typing import Dict
from pathlib import Path
import validators
import re

from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import StateFilter
from ..keyboards import (
    main_menu_keyboard,
    upload_confirm_keyboard,
    select_input_method_keyboard,
    select_config_keyboard,
)
from ..utils.rust_executor import execute_rust_command
from ..database import sqlite_db
from .. import config

PARAM_NAMES_FRIENDLY = {
    'source_url': "адрес базы данных (URL) или путь к файлу",
    'source_user': "имя пользователя",
    'source_pass': "пароль",
    'source_query': "текст SQL запроса",
    'db_name': "имя базы данных",
    'collection_name': "имя коллекции",
    'key_pattern': "паттерн ключей Redis",
    'es_index': "имя индекса Elasticsearch",
    'es_query': "текст JSON запроса Elasticsearch",
    'redis_pattern': "паттерн ключей Redis",
    'mongo_db': "имя базы данных MongoDB",
    'mongo_collection': "имя коллекции MongoDB",
    'csv': "CSV файл",
    'upload_api_token': "токен API True Tabs",
    'upload_datasheet_id': "ID таблицы True Tabs (начинается на 'dst')",
    'upload_field_map_json': "сопоставление заголовков и Field ID в формате JSON (опционально, оставьте пустым для пропуска)",
}


def get_friendly_param_name(param_key: str) -> str:
    return PARAM_NAMES_FRIENDLY.get(param_key, param_key.replace('_', ' ').capitalize())

router = Router()

class UploadProcess(StatesGroup):
    select_source_input_method = State()
    select_saved_source_config = State()

    waiting_pg_url = State()
    waiting_pg_query = State()

    waiting_mysql_url = State()
    waiting_mysql_query = State()

    waiting_sqlite_url = State()
    waiting_sqlite_query = State()

    waiting_redis_url = State()
    waiting_redis_pattern = State()

    waiting_mongodb_uri = State()
    waiting_mongo_db = State()
    waiting_mongo_collection = State()

    waiting_elasticsearch_url = State()
    waiting_elasticsearch_index = State()
    waiting_elasticsearch_query = State()

    waiting_csv_filepath = State()

    # TODO: Добавить состояния для Labguru

    select_tt_input_method = State()
    select_saved_tt_config = State()

    waiting_upload_token = State()
    waiting_datasheet_id = State()
    waiting_field_map_json = State()

    confirm_parameters = State()

SOURCE_PARAMS_ORDER = {
    "postgres": ["source_url", "source_query"],
    "mysql": ["source_url", "source_query"],
    "sqlite": ["source_url", "source_query"],
    "redis": ["source_url", "redis_pattern"],
    "mongodb": ["source_url", "mongo_db", "mongo_collection"],
    "elasticsearch": ["source_url", "es_index", "es_query"],
    "csv": ["source_url"],
    # TODO: Добавить сюда порядок параметров для Labguru
}

DISABLED_SOURCES = ['mssql', 'cassandra', 'couchbase', 'clickhouse', 'influxdb', 'neo4j', 'excel']

def is_valid_url(url_string: str) -> bool:
    """Checks if a string is a valid URL."""
    # A basic regex for URL validation - can be made more complex if needed
    url_regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https:// or ftp:// or ftps://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' # domain...
        r'localhost|' # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(url_regex, url_string) is not None

def is_valid_json(json_string: str) -> bool:
    """Checks if a string is a valid JSON string."""
    try:
        json.loads(json_string)
        return True
    except json.JSONDecodeError:
        return False

@router.message(F.text.lower() == "/cancel", StateFilter(UploadProcess))
@router.callback_query(F.data == "cancel", StateFilter(UploadProcess))
async def cancel_upload_process(callback_query: CallbackQuery, state: FSMContext):
    await state.clear()
    chat_id = callback_query.message.chat.id if isinstance(callback_query, CallbackQuery) else callback_query.chat.id
    bot = callback_query.message.bot if isinstance(callback_query, CallbackQuery) else callback_query.bot
    message_id = callback_query.message.message_id if isinstance(callback_query, CallbackQuery) else None

    text = "Операция отменена."
    keyboard = main_menu_keyboard()

    if isinstance(callback_query, CallbackQuery):
         await callback_query.message.edit_text(text, reply_markup=keyboard)
         await callback_query.answer()
    else:
         await callback_query.answer(text, reply_markup=keyboard)

@router.callback_query(F.data.startswith("start_upload_process:"))
async def start_upload_process_fsm(callback: CallbackQuery, state: FSMContext):
     source_type = callback.data.split(":")[1]
     await state.update_data(selected_source_type=source_type, source_params={})
     await state.set_state(UploadProcess.select_source_input_method)

     # ... остальная логика хендлера start_upload_process_fsm ...
     if source_type in SOURCE_PARAMS_ORDER:
          await callback.message.edit_text(
             f"Выбран источник: <b>{source_type}</b>.\nВыберите способ ввода параметров для источника:",
             reply_markup=select_input_method_keyboard('source'),
             parse_mode='HTML'
          )
     else:
          # Это обрабатывает источники, которых нет в SOURCE_PARAMS_ORDER
          # и, возможно, отключенные источники, если они не были пойманы выше
          await callback.message.edit_text(
              f"Выбран источник: <b>{source_type}</b>.\nПошаговый ввод параметров для этого источника пока не реализован или он отключен.\n"
              f"Пожалуйста, выберите другой источник или вернитесь в главное меню.",
              reply_markup=main_menu_keyboard(),
              parse_mode='HTML'
          )
          await state.clear()

     await callback.answer()


@router.callback_query(F.data.startswith("select_input_method:"), UploadProcess.select_source_input_method)
async def process_source_input_method(callback: CallbackQuery, state: FSMContext):
    method = callback.data.split(":")[1]
    state_data = await state.get_data()
    source_type = state_data['selected_source_type']
    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]])

    # TODO: Добавьте здесь проверку на отключенные источники и Алерт!

    # --- Логика для файловых источников ---
    if source_type in ['csv', 'excel']: # Проверяем, является ли источник файлом
         await state.set_state(UploadProcess.waiting_file_upload) # Переходим в состояние ожидания файла
         friendly_name = get_friendly_param_name(source_type) # Получаем "CSV файл" или "Excel файл"
         await callback.message.edit_text(f"Пожалуйста, отправьте мне файл {friendly_name.upper()}.") # <-- Изменено
         await callback.answer()
         return # Важно: выходим из хендлера

    # --- Логика для ручного ввода параметров (если не файловый источник) ---
    if method == 'manual':
        initial_param_state = None
        param_keys_order = SOURCE_PARAMS_ORDER.get(source_type, [])

        if not param_keys_order:
            # TODO: Обработка источников без параметров
            await callback.message.edit_text(f"Для источника '{source_type}' параметры не требуются или собираются другим способом.", reply_markup=main_menu_keyboard())
            await state.clear()
            await callback.answer()
            return

        first_param_key = param_keys_order[0]

        # Определение начального состояния для первого параметра
        # Это можно упростить, если структура SOURCE_PARAMS_ORDER и StatesGroup жестко связаны
        if source_type == "postgres":
            initial_param_state = UploadProcess.waiting_pg_url
        elif source_type == "mysql":
            initial_param_state = UploadProcess.waiting_mysql_url
        elif source_type == "sqlite":
            initial_param_state = UploadProcess.waiting_sqlite_url
        elif source_type == "redis":
             initial_param_state = UploadProcess.waiting_redis_url
        elif source_type == "mongodb":
            initial_param_state = UploadProcess.waiting_mongodb_uri
        elif source_type == "elasticsearch":
             initial_param_state = UploadProcess.waiting_elasticsearch_url
        elif source_type == "csv":
            initial_param_state = UploadProcess.waiting_csv_filepath
        # TODO: Добавить сюда условия для Labguru и других

        if initial_param_state:
             await state.set_state(initial_param_state)
             await state.update_data(
                  param_keys_order=param_keys_order,
                  current_param_index=0, # Индекс для текущего (уже введенного) параметра, а не для следующего!
                                         # Возможно, стоит пересмотреть логику индексации.
                                         # Или удалять ключи из param_keys_order по мере ввода.
             )
             # Используем функцию get_friendly_param_name для запроса ПЕРВОГО параметра
             friendly_name = get_friendly_param_name(first_param_key)
             await callback.message.edit_text(f"Введите {friendly_name}:", reply_markup=cancel_kb) # <--- Изменено здесь
             await callback.answer()
        else:
            # ... (обработка нереализованных источников) ...
            await callback.message.edit_text(f"Ошибка: Ручной ввод параметров для источника '{source_type}' не реализован.", reply_markup=main_menu_keyboard())
            await state.clear()
            await callback.answer()

    elif method == 'saved':
        await state.set_state(UploadProcess.select_saved_source_config)
        source_configs = await sqlite_db.list_source_configs()
        filtered_configs = [cfg for cfg in source_configs if cfg.get('source_type') == source_type]

        if not filtered_configs:
            await callback.message.edit_text(f"Сохраненных конфигураций источника типа '{source_type}' не найдено. Пожалуйста, выберите ручной ввод.", reply_markup=select_input_method_keyboard('source'))
            await state.set_state(UploadProcess.select_source_input_method)
        else:
            text = f"Выберите сохраненную конфигурацию источника типа '{source_type}':"
            await callback.message.edit_text(text, reply_markup=select_config_keyboard(filtered_configs, 'source_select'))

        await callback.answer()

    else:
        await callback.message.edit_text("Неверный выбор метода ввода.", reply_markup=main_menu_keyboard())
        await state.clear()
        await callback.answer()

@router.callback_query(F.data.startswith("select_config:source_select:"), UploadProcess.select_saved_source_config)
async def process_saved_source_config_selection(callback: CallbackQuery, state: FSMContext):
    config_name = callback.data.split(":")[2]
    saved_config = await sqlite_db.get_source_config(config_name)

    if saved_config:
        await state.update_data(source_params=saved_config)
        await callback.message.edit_text(f"Использована конфигурация источника: <b>{config_name}</b>.\n"
                                         f"Выберите способ ввода параметров для True Tabs:",
                                         reply_markup=select_input_method_keyboard('tt'),
                                         parse_mode='HTML')
        await state.set_state(UploadProcess.select_tt_input_method)
    else:
        await callback.message.edit_text(f"Ошибка: Конфигурация источника '{config_name}' не найдена.", reply_markup=main_menu_keyboard())
        await state.clear()

    await callback.answer()

@router.message(UploadProcess.waiting_pg_url)
async def process_pg_url_manual(message: Message, state: FSMContext):
    url = message.text.strip()
    # TODO: Возможно, стоит добавить более строгую проверку URL для Postgres
    if not validators.url(url):
        # Используем понятное имя для повторного запроса
        friendly_name = get_friendly_param_name('source_url')
        await message.answer(f"Неверный формат URL. Пожалуйста, введите валидный {friendly_name} для PostgreSQL:") # <-- Изменено
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = url
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_pg_query)
    # Используем понятное имя для запроса СЛЕДУЮЩЕГО параметра
    friendly_name = get_friendly_param_name('source_query')
    await message.answer(f"Введите {friendly_name}:")

@router.message(UploadProcess.waiting_pg_query)
async def process_pg_query_manual(message: Message, state: FSMContext):
    query = message.text.strip()
    if not query:
        # Используем понятное имя для повторного запроса
        friendly_name = get_friendly_param_name('source_query')
        await message.answer(f"{friendly_name.capitalize()} не может быть пустым. Введите {friendly_name}:") # <-- Изменено
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_query'] = query
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_mysql_url)
async def process_mysql_url_manual(message: Message, state: FSMContext):
    url = message.text.strip()
    if not validators.url(url):
        # Используем понятное имя для повторного запроса
        friendly_name = get_friendly_param_name('source_url')
        await message.answer(f"Неверный формат URL. Пожалуйста, введите валидный {friendly_name} для MySQL:") # <-- Изменено
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = url
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_mysql_query)
    # Используем понятное имя для запроса СЛЕДУЮЩЕГО параметра
    friendly_name = get_friendly_param_name('source_query')
    await message.answer(f"Введите {friendly_name}:")

@router.message(UploadProcess.waiting_mysql_query)
async def process_mysql_query_manual(message: Message, state: FSMContext):
    query = message.text.strip()
    if not query:
        # Используем понятное имя для повторного запроса
        friendly_name = get_friendly_param_name('source_query')
        await message.answer(f"{friendly_name.capitalize()} не может быть пустым. Введите {friendly_name}:") # <-- Изменено
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_query'] = query
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_sqlite_url)
async def process_sqlite_url_manual(message: Message, state: FSMContext):
    url = message.text.strip() # Здесь url - это путь к файлу БД
    if not url: # Проверяем на пустоту
        # Используем понятное имя для повторного запроса
        friendly_name = get_friendly_param_name('source_url')
        await message.answer(f"{friendly_name.capitalize()} не может быть пустым. Введите {friendly_name} для SQLite:") # <-- Изменено
        return
    # TODO: Возможно, стоит добавить проверку существования файла здесь: if not Path(url).is_file(): ...
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = url
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_sqlite_query)
    # Используем понятное имя для запроса СЛЕДУЮЩЕГО параметра
    friendly_name = get_friendly_param_name('source_query')
    await message.answer(f"Введите {friendly_name}:") # <-- Изменено


@router.message(UploadProcess.waiting_sqlite_query)
async def process_sqlite_query_manual(message: Message, state: FSMContext):
    query = message.text.strip()
    if not query:
        # Используем понятное имя для повторного запроса
        friendly_name = get_friendly_param_name('source_query')
        await message.answer(f"{friendly_name.capitalize()} не может быть пустым. Введите {friendly_name}:") # <-- Изменено
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_query'] = query
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_redis_url)
async def process_redis_url_manual(message: Message, state: FSMContext):
    url = message.text.strip()
    if not validators.url(url, require_tld=False): # TODO: Проверьте валидацию URL для Redis
        # Используем понятное имя для повторного запроса
        friendly_name = get_friendly_param_name('source_url')
        await message.answer(f"Неверный формат URL. Пожалуйста, введите валидный {friendly_name} для Redis:") # <-- Изменено
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = url
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_redis_pattern)
    # Используем понятное имя для запроса СЛЕДУЮЩЕГО параметра
    friendly_name = get_friendly_param_name('redis_pattern')
    await message.answer(f"Введите {friendly_name} (например, *, user:*, опционально):") # <-- Изменено

@router.message(UploadProcess.waiting_redis_pattern)
async def process_redis_pattern_manual(message: Message, state: FSMContext):
    pattern = message.text.strip()
    # У вас нет явной валидации на формат паттерна, только проверка на пустоту в параметрах Rust
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['redis_pattern'] = pattern if pattern else "*" # Если пусто, используем "*" по умолчанию
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_mongodb_uri)
async def process_mongo_uri_manual(message: Message, state: FSMContext):
    uri = message.text.strip()
    if not uri: # TODO: Возможно, стоит добавить валидацию URI для MongoDB
        # Используем понятное имя для повторного запроса
        friendly_name = get_friendly_param_name('source_url')
        await message.answer(f"{friendly_name.capitalize()} не может быть пустым. Введите {friendly_name} для MongoDB:") # <-- Изменено
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = uri # TODO: Ключ параметра в Rust - source_url, убедитесь, что он совпадает
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_mongo_db)
    # Используем понятное имя для запроса СЛЕДУЮЩЕГО параметра
    friendly_name = get_friendly_param_name('mongo_db')
    await message.answer(f"Введите {friendly_name}:") # <-- Изменено


@router.message(UploadProcess.waiting_mongo_db)
async def process_mongo_db_manual(message: Message, state: FSMContext):
    db_name = message.text.strip()
    if not db_name:
         # Используем понятное имя для повторного запроса
         friendly_name = get_friendly_param_name('mongo_db')
         await message.answer(f"{friendly_name.capitalize()} не может быть пустым. Введите {friendly_name}:") # <-- Изменено
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['mongo_db'] = db_name
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_mongo_collection)
    # Используем понятное имя для запроса СЛЕДУЮЩЕЙ коллекции
    friendly_name = get_friendly_param_name('mongo_collection')
    await message.answer(f"Введите {friendly_name}:") # <-- Изменено

@router.message(UploadProcess.waiting_mongo_collection)
async def process_mongo_collection_manual(message: Message, state: FSMContext):
    collection_name = message.text.strip()
    if not collection_name:
         # Используем понятное имя для повторного запроса
         friendly_name = get_friendly_param_name('mongo_collection')
         await message.answer(f"{friendly_name.capitalize()} не может быть пустым. Введите {friendly_name}:") # <-- Изменено
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['mongo_collection'] = collection_name
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_elasticsearch_url)
async def process_elasticsearch_url_manual(message: Message, state: FSMContext):
    url = message.text.strip()
    if not validators.url(url, require_tld=False): # TODO: Проверьте валидацию URL для Elasticsearch
        # Используем понятное имя для повторного запроса
        friendly_name = get_friendly_param_name('source_url')
        await message.answer(f"Неверный формат URL. Пожалуйста, введите валидный {friendly_name} для Elasticsearch:") # <-- Изменено
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = url # TODO: Ключ параметра в Rust - source_url, убедитесь, что он совпадает
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_elasticsearch_index)
    # Используем понятное имя для запроса СЛЕДУЮЩЕГО параметра
    friendly_name = get_friendly_param_name('es_index')
    await message.answer(f"Введите {friendly_name}:") # <-- Изменено

@router.message(UploadProcess.waiting_elasticsearch_index)
async def process_elasticsearch_index_manual(message: Message, state: FSMContext):
    index = message.text.strip()
    if not index:
         # Используем понятное имя для повторного запроса
         friendly_name = get_friendly_param_name('es_index')
         await message.answer(f"{friendly_name.capitalize()} не может быть пустым. Введите {friendly_name}:") # <-- Изменено
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['es_index'] = index # TODO: Ключ параметра в Rust - es_index, убедитесь, что он совпадает
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_elasticsearch_query)
    # Используем понятное имя для запроса СЛЕДУЮЩЕГО параметра
    friendly_name = get_friendly_param_name('es_query')
    await message.answer(f"Введите {friendly_name} (опционально, {{}} для всех):") # <-- Изменено

@router.message(UploadProcess.waiting_elasticsearch_query)
async def process_elasticsearch_query_manual(message: Message, state: FSMContext):
    query_str = message.text.strip()
    if not query_str:
        query_str = "{}"

    try:
        json.loads(query_str)
    except json.JSONDecodeError:
        # Используем понятное имя для повторного запроса
        friendly_name = get_friendly_param_name('es_query')
        await message.answer(f"Неверный формат JSON запроса ({friendly_name}). Пожалуйста, проверьте формат и попробуйте снова:") # <-- Изменено
        return

    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['es_query'] = query_str # TODO: Ключ параметра в Rust - es_query, убедитесь, что он совпадает
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_csv_filepath)
async def process_csv_filepath_manual(message: Message, state: FSMContext):
    filepath = message.text.strip()
    # Валидация на существование файла и расширение остается
    if not Path(filepath).is_file():
         # Используем понятное имя для повторного запроса
         friendly_name = get_friendly_param_name('csv')
         await message.answer(f"Файл не найден или это не файл. Убедитесь, что путь указан верно и файл доступен на сервере бота. Введите путь к {friendly_name}:") # <-- Изменено
         return
    if not filepath.lower().endswith('.csv'):
         # Используем понятное имя для повторного запроса
         friendly_name = get_friendly_param_name('csv')
         await message.answer(f"Файл должен быть в формате .csv. Введите путь к {friendly_name}:") # <-- Изменено
         return

    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = filepath # TODO: Ключ параметра в Rust - source_url, убедитесь, что он совпадает
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))


# TODO: Адаптировать обработчики ручного ввода для Labguru

@router.callback_query(F.data.startswith("select_input_method:"), UploadProcess.select_tt_input_method)
async def process_tt_input_method(callback: CallbackQuery, state: FSMContext):
    method = callback.data.split(":")[1]
    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]])

    if method == 'manual':
        await state.set_state(UploadProcess.waiting_upload_token)
        await state.update_data(
             param_keys_order=["upload_api_token", "upload_datasheet_id", "upload_field_map_json"],
             current_param_index=0, # Оставляем, если используется для чего-то еще
             tt_params={}
        )
        # Используем понятное имя для запроса ПЕРВОГО параметра TT
        first_param_key = "upload_api_token"
        friendly_name = get_friendly_param_name(first_param_key)
        await callback.message.edit_text(f"Введите {friendly_name}:", reply_markup=cancel_kb) # <-- Изменено
        await callback.answer()

    elif method == 'saved':
        await state.set_state(UploadProcess.select_saved_tt_config)
        tt_configs = await sqlite_db.list_tt_configs()

        if not tt_configs:
            await callback.message.edit_text("Сохраненных конфигураций True Tabs не найдено. Пожалуйста, выберите ручной ввод.", reply_markup=select_input_method_keyboard('tt'))
            await state.set_state(UploadProcess.select_tt_input_method)
        else:
            text = "Выберите сохраненную конфигурацию True Tabs:"
            await callback.message.edit_text(text, reply_markup=select_config_keyboard(tt_configs, 'tt_select'))

        await callback.answer()

    else:
        await callback.message.edit_text("Неверный выбор метода ввода.", reply_markup=main_menu_keyboard())
        await state.clear()
        await callback.answer()

@router.callback_query(F.data.startswith("select_config:tt_select:"), UploadProcess.select_saved_tt_config)
async def process_saved_tt_config_selection(callback: CallbackQuery, state: FSMContext):
    config_name = callback.data.split(":")[2]
    saved_config = await sqlite_db.get_tt_config(config_name)

    if saved_config:
        await state.update_data(tt_params={
            "upload_api_token": saved_config.get("upload_api_token"),
            "upload_datasheet_id": saved_config.get("upload_datasheet_id"),
            "upload_field_map_json": saved_config.get("upload_field_map_json"),
        })
        await callback.message.edit_text(f"Использована конфигурация True Tabs: <b>{config_name}</b>.\n"
                                         f"Все параметры собраны. Нажмите 'Загрузить' для подтверждения.",
                                         reply_markup=upload_confirm_keyboard(),
                                         parse_mode='HTML')
        await state.set_state(UploadProcess.confirm_parameters)
    else:
        await callback.message.edit_text(f"Ошибка: Конфигурация True Tabs '{config_name}' не найдена.", reply_markup=main_menu_keyboard())
        await state.clear()

    await callback.answer()

@router.message(UploadProcess.waiting_upload_token)
async def process_upload_token_manual(message: Message, state: FSMContext):
    token = message.text.strip()
    if not token:
        # Используем понятное имя для повторного запроса
        friendly_name = get_friendly_param_name('upload_api_token')
        await message.answer(f"{friendly_name.capitalize()} не может быть пустым. Введите {friendly_name}:") # <-- Изменено
        return
    state_data = await state.get_data()
    current_params = state_data.get('tt_params', {})
    current_params['upload_api_token'] = token # TODO: Ключ параметра в Rust - upload_api_token, убедитесь, что он совпадает
    await state.update_data(tt_params=current_params)
    await state.set_state(UploadProcess.waiting_datasheet_id)
    # Используем понятное имя для запроса СЛЕДУЮЩЕГО параметра TT
    friendly_name = get_friendly_param_name('upload_datasheet_id')
    await message.answer(f"Введите {friendly_name}:") # <-- Изменено

@router.message(UploadProcess.waiting_datasheet_id)
async def process_datasheet_id_manual(message: Message, state: FSMContext):
    datasheet_id = message.text.strip()
    if not datasheet_id or not datasheet_id.startswith("dst"):
         # Используем понятное имя для повторного запроса
         friendly_name = get_friendly_param_name('upload_datasheet_id')
         await message.answer(f"Неверный формат {friendly_name}. Он должен начинаться с 'dst'. Введите {friendly_name}:") # <-- Изменено
         return
    state_data = await state.get_data()
    current_params = state_data.get('tt_params', {})
    current_params['upload_datasheet_id'] = datasheet_id # TODO: Ключ параметра в Rust - upload_datasheet_id, убедитесь, что он совпадает
    await state.update_data(tt_params=current_params)
    await state.set_state(UploadProcess.waiting_field_map_json)
    # Используем понятное имя для запроса СЛЕДУЮЩЕГО параметра TT
    friendly_name = get_friendly_param_name('upload_field_map_json')
    await message.answer(f"Введите {friendly_name}:") # <-- Изменено

@router.message(UploadProcess.waiting_field_map_json)
async def process_field_map_manual(message: Message, state: FSMContext):
    json_str = message.text.strip()
    # Обработка пустого ввода для опционального поля
    if not json_str:
        json_str = "{}" # Или None, если Rust утилита может обработать None

    try:
        field_map: Dict[str, str] = json.loads(json_str)
        # Валидация структуры JSON (остается)
        if not isinstance(field_map, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in field_map.items()):
             # Используем понятное имя для повторного запроса
             friendly_name = get_friendly_param_name('upload_field_map_json')
             await message.answer(f"Неверная структура JSON для {friendly_name}. Ожидается объект {{ \"header\": \"field_id\" }}. Попробуйте снова.") # <-- Изменено
             return
        # Проверка на пустой объект, если поле НЕ опциональное в Rust (у вас оно опционально по описанию)
        # if not field_map:
        #      friendly_name = get_friendly_param_name('upload_field_map_json')
        #      await message.answer(f"{friendly_name.capitalize()} не может быть пустым. Введите {friendly_name}:")
        #      return

        state_data = await state.get_data()
        current_params = state_data.get('tt_params', {})
        current_params['upload_field_map_json'] = json_str # TODO: Ключ параметра в Rust - upload_field_map_json, убедитесь, что он совпадает
        await state.update_data(tt_params=current_params)

        await state.set_state(UploadProcess.confirm_parameters)
        source_params = state_data.get('source_params', {})
        tt_params = state_data.get('tt_params', {})

        # --- Формирование текста подтверждения ---
        # Здесь вы уже используете .replace('_', ' ').title(). Можно заменить на get_friendly_param_name
        confirm_text = f"<b>Собранные параметры:</b>\n\n"
        confirm_text += f"Источник: <b>{state_data.get('selected_source_type', 'Неизвестно')}</b>\n"
        for key, value in source_params.items():
             # Используем get_friendly_param_name для ключей
             friendly_key = get_friendly_param_name(key)
             if key in ['source_pass', 'neo4j_pass', 'couchbase_pass', 'upload_api_token']: # Пароли/токены скрываем
                 confirm_text += f"  {friendly_key.capitalize()}: <code>***</code>\n"
             # Специальная обработка для URL/URI, чтобы не скрывать их, если это не пароль
             elif key in ['source_url', 'cassandra_addresses', 'neo4j_uri', 'couchbase_cluster_url'] and state_data.get('selected_source_type') not in ['excel', 'csv']:
                  confirm_text += f"  {friendly_key.capitalize()}: <code>{value}</code>\n"
             # Специальная обработка для пути к файлу (если источник excel/csv)
             elif key == 'source_url' and state_data.get('selected_source_type') in ['excel', 'csv']:
                 confirm_text += f"  {get_friendly_param_name('source_url_file').capitalize()}: <code>{value}</code>\n" # Можно добавить отдельное понятное имя для пути к файлу
             # Специальная обработка для JSON запроса Elasticsearch
             elif key == 'es_query':
                 try:
                      query_display = json.dumps(json.loads(value), indent=2, ensure_ascii=False)
                      confirm_text += f"  {friendly_key.capitalize()}:\n<pre><code class=\"language-json\">{query_display}</code></pre>\n"
                 except:
                      confirm_text += f"  {friendly_key.capitalize()}: <code>Некорректный JSON</code>\n"
             # Пропускаем специфические параметры, если они не отображаются здесь
             elif key == 'specific_params':
                  pass # Пропускаем
             else:
                confirm_text += f"  {friendly_key.capitalize()}: <code>{value}</code>\n" # <-- Используем понятное имя

        confirm_text += f"\n<b>Параметры True Tabs:</b>\n"
        for key, value in tt_params.items():
             # Используем get_friendly_param_name для ключей
             friendly_key = get_friendly_param_name(key)
             if key == 'upload_api_token':
                 confirm_text += f"  {friendly_key.capitalize()}: <code>***</code>\n"
             # Специальная обработка для JSON сопоставления полей
             elif key == 'upload_field_map_json':
                 try:
                     field_map_display = json.dumps(json.loads(value), indent=2, ensure_ascii=False)
                     confirm_text += f"  {get_friendly_param_name('upload_field_map_json_display').capitalize()}:\n<pre><code class=\"language-json\">{field_map_display}</code></pre>\n" # Можно добавить отдельное понятное имя для отображения
                 except:
                     confirm_text += f"  {friendly_key.capitalize()}: <code>Некорректный JSON</code>\n"
             else:
                confirm_text += f"  {friendly_key.capitalize()}: <code>{value}</code>\n" # <-- Используем понятное имя


        confirm_text += f"\nВсе верно? Нажмите 'Загрузить' для старта операции."

        await message.answer(confirm_text, reply_markup=upload_confirm_keyboard(), parse_mode='HTML')

    except json.JSONDecodeError as e:
        # Используем понятное имя для сообщения об ошибке парсинга JSON
        friendly_name = get_friendly_param_name('upload_field_map_json')
        await message.answer(f"Ошибка парсинга JSON для {friendly_name}: {e}. Пожалуйста, проверьте формат и попробуйте снова.") # <-- Изменено

@router.callback_query(F.data == "confirm_upload", UploadProcess.confirm_parameters)
async def handle_confirm_upload(callback: CallbackQuery, state: FSMContext, bot: Bot):
    state_data = await state.get_data()
    source_type = state_data.get("selected_source_type", "unknown")
    source_params = state_data.get("source_params", {})
    tt_params = state_data.get("tt_params", {})

    output_filepath = Path(config.TEMP_FILES_DIR) / f"upload_{callback.from_user.id}_{source_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    rust_args = []

    rust_args.append("--source-type")
    rust_args.append(source_type)

    for key, value in source_params.items():
        if value is not None:
            rust_args.append(f"--{key.replace('_', '-')}")
            rust_args.append(str(value))

    for key, value in tt_params.items():
        if value is not None:
            rust_args.append(f"--{key.replace('_', '-')}")
            if key == "upload_field_map_json":
                 rust_args.append(json.dumps(value))
            else:
                rust_args.append(str(value))

    rust_args.append("--output-xlsx-path")
    rust_args.append(str(output_filepath))

    await state.clear()

    await callback.message.edit_text("Запуск процесса извлечения и загрузки. Это может занять время...", reply_markup=main_menu_keyboard())
    await callback.answer()

    asyncio.create_task(process_upload_task(
        bot,
        callback.message.chat.id,
        rust_args,
        source_type,
        tt_params.get("upload_datasheet_id", "N/A"),
        str(output_filepath)
    ))

async def process_upload_task(bot: Bot, chat_id: int, rust_args: list, source_type: str, datasheet_id: str, output_filepath: str):
    start_time = datetime.now()
    result = await execute_rust_command(rust_args)
    end_time = datetime.now()
    duration = result.get("duration_seconds", (end_time - start_time).total_seconds())


    status = result.get("status", "ERROR")
    file_path_from_rust = result.get("file_path")
    error_message = result.get("message", "Неизвестная ошибка выполнения.")

    extracted_rows = result.get("extracted_rows")
    uploaded_records = result.get("uploaded_records")
    datasheet_id_from_result = result.get("datasheet_id", datasheet_id)

    final_file_path = output_filepath if status == "SUCCESS" and file_path_from_rust else None

    await sqlite_db.add_upload_record(
        source_type=source_type,
        status=status,
        file_path=final_file_path,
        error_message=error_message,
        true_tabs_datasheet_id=datasheet_id_from_result,
        duration_seconds=duration
    )

    if status == "SUCCESS":
        final_message_text = f"✅ <b>Загрузка успешно завершена!</b>\n"
        final_message_text += f"Источник: <code>{source_type}</code>\n"
        if datasheet_id_from_result and datasheet_id_from_result != 'N/A':
            final_message_text += f"Datasheet ID: <code>{datasheet_id_from_result}</code>\n"

        if extracted_rows is not None:
             final_message_text += f"Извлечено строк: {extracted_rows}\n"
        if uploaded_records is not None:
             final_message_text += f"Загружено записей: {uploaded_records}\n"

        final_message_text += f"Время выполнения: {duration:.2f} секунд\n"
        if final_file_path:
             final_message_text += f"Файл сохранен на сервере бота: <code>{final_file_path}</code>"
        if result.get('message') and result.get('message') != "Request successful":
             final_message_text += f"\n<i>Сообщение от утилиты:</i> {result['message']}"


        await bot.send_message(chat_id, final_message_text, parse_mode='HTML')

        if final_file_path and os.path.exists(final_file_path):
            try:
                await bot.send_document(chat_id, document= FSInputFile(final_file_path, filename=os.path.basename(final_file_path)))
            except Exception as e:
                 print(f"Ошибка при отправке файла в Telegram: {e}", file=sys.stderr)
                 await bot.send_message(chat_id, f"❌ Ошибка при отправке файла: {e}")
        else:
             await bot.send_message(chat_id, "⚠️ Не удалось найти или отправить файл XLSX.")

    else:
        final_message_text = f"❌ <b>Ошибка при извлечении или загрузке данных!</b>\n"
        final_message_text += f"Источник: <code>{source_type}</code>\n"
        if datasheet_id_from_result and datasheet_id_from_result != 'N/A':
            final_message_text += f"Datasheet ID: <code>{datasheet_id_from_result}</code>\n"

        if extracted_rows is not None:
             final_message_text += f"Извлечено строк (до ошибки): {extracted_rows}\n"
        if uploaded_records is not None:
             final_message_text += f"Загружено записей (до ошибки): {uploaded_records}\n"

        final_message_text += f"Время выполнения: {duration:.2f} секунд\n"
        final_message_text += f"Сообщение об ошибке:\n<pre><code>{error_message}</code></pre>"

        await bot.send_message(chat_id, final_message_text, parse_mode='HTML')