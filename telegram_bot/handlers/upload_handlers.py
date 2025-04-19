import os
import asyncio
import json
import sys
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path
import validators

from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, FSInputFile, ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from keyboards.inline import (
    main_menu_keyboard, upload_confirm_keyboard,
    select_input_method_keyboard, select_config_keyboard
)
from utils.rust_executor import execute_rust_command
from database import sqlite_db
import config

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

    waiting_mssql_url = State()
    waiting_mssql_query = State()

    waiting_redis_url = State()
    waiting_redis_pattern = State()

    waiting_mongodb_uri = State()
    waiting_mongo_db = State()
    waiting_mongo_collection = State()

    waiting_cassandra_addresses = State()
    waiting_cassandra_keyspace = State()
    waiting_cassandra_query = State()

    waiting_clickhouse_url = State()
    waiting_clickhouse_query = State()

    waiting_influxdb_url = State()
    waiting_influxdb_token = State()
    waiting_influxdb_org = State()
    waiting_influxdb_bucket = State()
    waiting_influxdb_query = State()

    waiting_elasticsearch_url = State()
    waiting_elasticsearch_index = State()
    waiting_elasticsearch_query = State()

    waiting_neo4j_uri = State()
    waiting_neo4j_user = State()
    waiting_neo4j_pass = State()
    waiting_neo4j_query = State()

    waiting_couchbase_cluster_url = State()
    waiting_couchbase_user = State()
    waiting_couchbase_pass = State()
    waiting_couchbase_bucket = State()
    waiting_couchbase_query = State()


    waiting_excel_filepath = State()
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
    "mssql": ["source_url", "source_query"],
    "redis": ["source_url", "redis_pattern"],
    "mongodb": ["source_url", "mongo_db", "mongo_collection"],
    "cassandra": ["source_url", "cassandra_keyspace", "cassandra_query"],
    "clickhouse": ["source_url", "source_query"],
    "influxdb": ["source_url", "influx_token", "influx_org", "influx_bucket", "influx_query"],
    "elasticsearch": ["source_url", "es_index", "es_query"],
    "neo4j": ["source_url", "neo4j_user", "neo4j_pass", "source_query"],
    "couchbase": ["source_url", "source_user", "source_pass", "couchbase_bucket", "couchbase_query"],
    "excel": ["source_url"],
    "csv": ["source_url"],
    # TODO: Добавить сюда порядок параметров для Labguru
}

@router.message(F.text.lower() == "/cancel", State!S(UploadProcess))
@router.callback_query(F.data == "cancel", State!S(UploadProcess))
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

     if source_type in SOURCE_PARAMS_ORDER:
          await callback.message.edit_text(
             f"Выбран источник: <b>{source_type}</b>.\nВыберите способ ввода параметров для источника:",
             reply_markup=select_input_method_keyboard('source'),
             parse_mode='HTML'
          )
     else:
          await callback.message.edit_text(
              f"Выбран источник: <b>{source_type}</b>.\nПошаговый ввод параметров для этого источника пока не реализован.\n"
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


    if method == 'manual':
        initial_param_state = None

        if source_type == "postgres":
            initial_param_state = UploadProcess.waiting_pg_url
        elif source_type == "mysql":
            initial_param_state = UploadProcess.waiting_mysql_url
        elif source_type == "sqlite":
            initial_param_state = UploadProcess.waiting_sqlite_url
        elif source_type == "mssql":
            initial_param_state = UploadProcess.waiting_mssql_url
        elif source_type == "redis":
             initial_param_state = UploadProcess.waiting_redis_url
        elif source_type == "mongodb":
            initial_param_state = UploadProcess.waiting_mongodb_uri
        elif source_type == "cassandra":
             initial_param_state = UploadProcess.waiting_cassandra_addresses
        elif source_type == "clickhouse":
             initial_param_state = UploadProcess.waiting_clickhouse_url
        elif source_type == "influxdb":
             initial_param_state = UploadProcess.waiting_influxdb_url
        elif source_type == "elasticsearch":
             initial_param_state = UploadProcess.waiting_elasticsearch_url
        elif source_type == "neo4j":
             initial_param_state = UploadProcess.waiting_neo4j_uri
        elif source_type == "couchbase":
             initial_param_state = UploadProcess.waiting_couchbase_cluster_url
        elif source_type == "excel":
            initial_param_state = UploadProcess.waiting_excel_filepath
        elif source_type == "csv":
            initial_param_state = UploadProcess.waiting_csv_filepath

        # TODO: Добавить сюда условия для Labguru

        if initial_param_state:
             param_keys_order = SOURCE_PARAMS_ORDER.get(source_type, [])
             first_param_key = param_keys_order[0] if param_keys_order else "параметр"

             await state.set_state(initial_param_state)
             await state.update_data(
                  param_keys_order=param_keys_order,
                  current_param_index=0,
             )
             await callback.message.edit_text(f"Введите параметр '{first_param_key}':", reply_markup=cancel_kb)
             await callback.answer()
        else:
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
    if not validators.url(url):
        await message.answer("Неверный формат URL. Пожалуйста, введите валидный URL подключения к PostgreSQL:")
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = url
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_pg_query)
    await message.answer("Введите SQL запрос:")

@router.message(UploadProcess.waiting_pg_query)
async def process_pg_query_manual(message: Message, state: FSMContext):
    query = message.text.strip()
    if not query:
        await message.answer("SQL запрос не может быть пустым. Введите SQL запрос:")
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
        await message.answer("Неверный формат URL. Пожалуйста, введите валидный URL подключения к MySQL:")
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = url
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_mysql_query)
    await message.answer("Введите SQL запрос:")

@router.message(UploadProcess.waiting_mysql_query)
async def process_mysql_query_manual(message: Message, state: FSMContext):
    query = message.text.strip()
    if not query:
        await message.answer("SQL запрос не может быть пустым. Введите SQL запрос:")
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_query'] = query
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_sqlite_url)
async def process_sqlite_url_manual(message: Message, state: FSMContext):
    url = message.text.strip()
    if not url:
        await message.answer("URL (путь к файлу БД) не может быть пустым. Введите путь к файлу SQLite БД:")
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = url
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_sqlite_query)
    await message.answer("Введите SQL запрос:")

@router.message(UploadProcess.waiting_sqlite_query)
async def process_sqlite_query_manual(message: Message, state: FSMContext):
    query = message.text.strip()
    if not query:
        await message.answer("SQL запрос не может быть пустым. Введите SQL запрос:")
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_query'] = query
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_mssql_url)
async def process_mssql_url_manual(message: Message, state: FSMContext):
    url = message.text.strip()
    if not validators.url(url):
        await message.answer("Неверный формат URL. Пожалуйста, введите валидный URL подключения к MSSQL:")
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = url
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_mssql_query)
    await message.answer("Введите SQL запрос:")

@router.message(UploadProcess.waiting_mssql_query)
async def process_mssql_query_manual(message: Message, state: FSMContext):
    query = message.text.strip()
    if not query:
        await message.answer("SQL запрос не может быть пустым. Введите SQL запрос:")
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
    if not validators.url(url, require_tld=False):
        await message.answer("Неверный формат URL. Пожалуйста, введите валидный URL подключения к Redis:")
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = url
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_redis_pattern)
    await message.answer("Введите паттерн ключей Redis (например, *, user:*, опционально):")

@router.message(UploadProcess.waiting_redis_pattern)
async def process_redis_pattern_manual(message: Message, state: FSMContext):
    pattern = message.text.strip()
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['redis_pattern'] = pattern if pattern else "*"
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_mongodb_uri)
async def process_mongo_uri_manual(message: Message, state: FSMContext):
    uri = message.text.strip()
    if not uri:
        await message.answer("URI подключения не может быть пустым. Введите URI подключения к MongoDB:")
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = uri
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_mongo_db)
    await message.answer("Введите имя базы данных MongoDB:")

@router.message(UploadProcess.waiting_mongo_db)
async def process_mongo_db_manual(message: Message, state: FSMContext):
    db_name = message.text.strip()
    if not db_name:
         await message.answer("Имя базы данных не может быть пустым. Введите имя базы данных:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['mongo_db'] = db_name
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_mongo_collection)
    await message.answer("Введите имя коллекции MongoDB:")

@router.message(UploadProcess.waiting_mongo_collection)
async def process_mongo_collection_manual(message: Message, state: FSMContext):
    collection_name = message.text.strip()
    if not collection_name:
         await message.answer("Имя коллекции не может быть пустым. Введите имя коллекции:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['mongo_collection'] = collection_name
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_cassandra_addresses)
async def process_cassandra_addresses_manual(message: Message, state: FSMContext):
    addresses = message.text.strip()
    if not addresses or not any(addr.strip() for addr in addresses.split(',')):
         await message.answer("Адреса узлов не могут быть пустыми. Введите адреса Cassandra/ScyllaDB через запятую:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = addresses
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_cassandra_keyspace)
    await message.answer("Введите keyspace Cassandra/ScyllaDB:")

@router.message(UploadProcess.waiting_cassandra_keyspace)
async def process_cassandra_keyspace_manual(message: Message, state: FSMContext):
    keyspace = message.text.strip()
    if not keyspace:
         await message.answer("Keyspace не может быть пустым. Введите keyspace Cassandra/ScyllaDB:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['cassandra_keyspace'] = keyspace
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_cassandra_query)
    await message.answer("Введите CQL запрос:")

@router.message(UploadProcess.waiting_cassandra_query)
async def process_cassandra_query_manual(message: Message, state: FSMContext):
    query = message.text.strip()
    if not query:
         await message.answer("CQL запрос не может быть пустым. Введите CQL запрос:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['cassandra_query'] = query
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_clickhouse_url)
async def process_clickhouse_url_manual(message: Message, state: FSMContext):
    url = message.text.strip()
    if not validators.url(url, require_tld=False):
        await message.answer("Неверный формат URL. Пожалуйста, введите валидный URL подключения к ClickHouse:")
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = url
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_clickhouse_query)
    await message.answer("Введите запрос ClickHouse:")

@router.message(UploadProcess.waiting_clickhouse_query)
async def process_clickhouse_query_manual(message: Message, state: FSMContext):
    query = message.text.strip()
    if not query:
        await message.answer("Запрос ClickHouse не может быть пустым. Введите запрос ClickHouse:")
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_query'] = query
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_influxdb_url)
async def process_influxdb_url_manual(message: Message, state: FSMContext):
    url = message.text.strip()
    if not url:
         await message.answer("URL не может быть пустым. Введите URL InfluxDB:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = url
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_influxdb_token)
    await message.answer("Введите токен InfluxDB:")

@router.message(UploadProcess.waiting_influxdb_token)
async def process_influxdb_token_manual(message: Message, state: FSMContext):
    token = message.text.strip()
    if not token:
         await message.answer("Токен не может быть пустым. Введите токен InfluxDB:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['influx_token'] = token
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_influxdb_org)
    await message.answer("Введите организацию (org) InfluxDB:")

@router.message(UploadProcess.waiting_influxdb_org)
async def process_influxdb_org_manual(message: Message, state: FSMContext):
    org = message.text.strip()
    if not org:
         await message.answer("Организация не может быть пустой. Введите организацию (org) InfluxDB:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['influx_org'] = org
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_influxdb_bucket)
    await message.answer("Введите bucket InfluxDB:")

@router.message(UploadProcess.waiting_influxdb_bucket)
async def process_influxdb_bucket_manual(message: Message, state: FSMContext):
    bucket = message.text.strip()
    if not bucket:
         await message.answer("Bucket не может быть пустым. Введите bucket InfluxDB:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['influx_bucket'] = bucket
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_influxdb_query)
    await message.answer("Введите Flux запрос InfluxDB:")

@router.message(UploadProcess.waiting_influxdb_query)
async def process_influxdb_query_manual(message: Message, state: FSMContext):
    query = message.text.strip()
    if not query:
         await message.answer("Flux запрос не может быть пустым. Введите Flux запрос InfluxDB:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['influx_query'] = query
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_elasticsearch_url)
async def process_elasticsearch_url_manual(message: Message, state: FSMContext):
    url = message.text.strip()
    if not validators.url(url, require_tld=False):
        await message.answer("Неверный формат URL. Пожалуйста, введите валидный URL подключения к Elasticsearch:")
        return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = url
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_elasticsearch_index)
    await message.answer("Введите индекс Elasticsearch:")

@router.message(UploadProcess.waiting_elasticsearch_index)
async def process_elasticsearch_index_manual(message: Message, state: FSMContext):
    index = message.text.strip()
    if not index:
         await message.answer("Индекс не может быть пустым. Введите индекс Elasticsearch:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['es_index'] = index
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_elasticsearch_query)
    await message.answer("Введите JSON запрос Elasticsearch (опционально, {} для всех):")

@router.message(UploadProcess.waiting_elasticsearch_query)
async def process_elasticsearch_query_manual(message: Message, state: FSMContext):
    query_str = message.text.strip()
    if not query_str:
        query_str = "{}"

    try:
        json.loads(query_str)
    except json.JSONDecodeError:
        await message.answer("Неверный формат JSON запроса. Пожалуйста, введите валидный JSON или {} для всех записей:")
        return

    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['es_query'] = query_str
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_neo4j_uri)
async def process_neo4j_uri_manual(message: Message, state: FSMContext):
    uri = message.text.strip()
    if not uri:
         await message.answer("URI не может быть пустым. Введите URI Neo4j:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = uri
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_neo4j_user)
    await message.answer("Введите пользователя Neo4j:")

@router.message(UploadProcess.waiting_neo4j_user)
async def process_neo4j_user_manual(message: Message, state: FSMContext):
    user = message.text.strip()
    if not user:
         await message.answer("Пользователь не может быть пустым. Введите пользователя Neo4j:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['neo4j_user'] = user
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_neo4j_pass)
    await message.answer("Введите пароль Neo4j:")

@router.message(UploadProcess.waiting_neo4j_pass)
async def process_neo4j_pass_manual(message: Message, state: FSMContext):
    password = message.text.strip()
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['neo4j_pass'] = password
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_neo4j_query)
    await message.answer("Введите Cypher запрос:")

@router.message(UploadProcess.waiting_neo4j_query)
async def process_neo4j_query_manual(message: Message, state: FSMContext):
    query = message.text.strip()
    if not query:
         await message.answer("Cypher запрос не может быть пустым. Введите Cypher запрос:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_query'] = query
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_couchbase_cluster_url)
async def process_couchbase_cluster_url_manual(message: Message, state: FSMContext):
    url = message.text.strip()
    if not url:
         await message.answer("URL кластера не может быть пустым. Введите URL кластера Couchbase:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = url
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_couchbase_user)
    await message.answer("Введите пользователя Couchbase:")

@router.message(UploadProcess.waiting_couchbase_user)
async def process_couchbase_user_manual(message: Message, state: FSMContext):
    user = message.text.strip()
    if not user:
         await message.answer("Пользователь не может быть пустым. Введите пользователя Couchbase:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_user'] = user
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_couchbase_pass)
    await message.answer("Введите пароль Couchbase:")

@router.message(UploadProcess.waiting_couchbase_pass)
async def process_couchbase_pass_manual(message: Message, state: FSMContext):
    password = message.text.strip()
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_pass'] = password
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_couchbase_bucket)
    await message.answer("Введите имя bucket Couchbase:")

@router.message(UploadProcess.waiting_couchbase_bucket)
async def process_couchbase_bucket_manual(message: Message, state: FSMContext):
    bucket = message.text.strip()
    if not bucket:
         await message.answer("Имя bucket не может быть пустым. Введите имя bucket Couchbase:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['couchbase_bucket'] = bucket
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.waiting_couchbase_query)
    await message.answer("Введите N1QL запрос Couchbase:")

@router.message(UploadProcess.waiting_couchbase_query)
async def process_couchbase_query_manual(message: Message, state: FSMContext):
    query = message.text.strip()
    if not query:
         await message.answer("N1QL запрос не может быть пустым. Введите N1QL запрос Couchbase:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['couchbase_query'] = query
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_excel_filepath)
async def process_excel_filepath_manual(message: Message, state: FSMContext):
    filepath = message.text.strip()
    if not Path(filepath).is_file():
         await message.answer("Файл не найден или это не файл. Убедитесь, что путь указан верно и файл доступен на сервере бота. Введите путь к Excel файлу:")
         return
    if not (filepath.lower().endswith('.xlsx') or filepath.lower().endswith('.xls')):
         await message.answer("Файл должен быть в формате .xlsx или .xls. Введите путь к Excel файлу:")
         return

    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = filepath
    await state.update_data(source_params=current_params)
    await state.set_state(UploadProcess.select_tt_input_method)
    await message.answer("Все параметры источника введены.\nВыберите способ ввода параметров для True Tabs:", reply_markup=select_input_method_keyboard('tt'))

@router.message(UploadProcess.waiting_csv_filepath)
async def process_csv_filepath_manual(message: Message, state: FSMContext):
    filepath = message.text.strip()
    if not Path(filepath).is_file():
         await message.answer("Файл не найден или это не файл. Убедитесь, что путь указан верно и файл доступен на сервере бота. Введите путь к CSV файлу:")
         return
    if not filepath.lower().endswith('.csv'):
         await message.answer("Файл должен быть в формате .csv. Введите путь к CSV файлу:")
         return

    state_data = await state.get_data()
    current_params = state_data.get('source_params', {})
    current_params['source_url'] = filepath
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
             current_param_index=0,
             tt_params={}
        )
        await callback.message.edit_text("Введите токен авторизации True Tabs API:", reply_markup=cancel_kb)
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
        await message.answer("Токен не может быть пустым. Введите токен авторизации True Tabs API:")
        return
    state_data = await state.get_data()
    current_params = state_data.get('tt_params', {})
    current_params['upload_api_token'] = token
    await state.update_data(tt_params=current_params)
    await state.set_state(UploadProcess.waiting_datasheet_id)
    await message.answer("Введите Datasheet ID для загрузки (например, dst...):")

@router.message(UploadProcess.waiting_datasheet_id)
async def process_datasheet_id_manual(message: Message, state: FSMContext):
    datasheet_id = message.text.strip()
    if not datasheet_id or not datasheet_id.startswith("dst"):
         await message.answer("Неверный формат Datasheet ID. Он должен начинаться с 'dst'. Введите Datasheet ID:")
         return
    state_data = await state.get_data()
    current_params = state_data.get('tt_params', {})
    current_params['upload_datasheet_id'] = datasheet_id
    await state.update_data(tt_params=current_params)
    await state.set_state(UploadProcess.waiting_field_map_json)
    await message.answer("Введите сопоставление названий колонок с Field ID True Tabs в формате JSON строки (например, {\"Header1\": \"fldID1\", \"Header2\": \"fldID2\"}):")

@router.message(UploadProcess.waiting_field_map_json)
async def process_field_map_manual(message: Message, state: FSMContext):
    json_str = message.text.strip()
    try:
        field_map: Dict[str, str] = json.loads(json_str)
        if not isinstance(field_map, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in field_map.items()):
             await message.answer("Неверный формат JSON для сопоставления Field ID. Ожидается JSON объект, где ключи и значения - строки. Попробуйте снова.")
             return
        if not field_map:
             await message.answer("Сопоставление Field ID не может быть пустым. Введите сопоставление Field ID в формате JSON строки:")
             return

        state_data = await state.get_data()
        current_params = state_data.get('tt_params', {})
        current_params['upload_field_map_json'] = json_str
        await state.update_data(tt_params=current_params)

        await state.set_state(UploadProcess.confirm_parameters)
        source_params = state_data.get('source_params', {})
        tt_params = state_data.get('tt_params', {})

        confirm_text = f"<b>Собранные параметры:</b>\n\n"
        confirm_text += f"Источник: <b>{state_data.get('selected_source_type', 'Неизвестно')}</b>\n"
        for key, value in source_params.items():
             if key == 'source_pass' or key == 'neo4j_pass' or key == 'couchbase_pass':
                 confirm_text += f"  {key.replace('_', ' ').title()}: <code>***</code>\n"
             elif key in ['source_url', 'cassandra_addresses', 'neo4j_uri', 'couchbase_cluster_url'] and state_data.get('selected_source_type') not in ['excel', 'csv']:
                  confirm_text += f"  {key.replace('_', ' ').title()}: <code>{value}</code>\n"
             elif key == 'source_url' and state_data.get('selected_source_type') in ['excel', 'csv']:
                 confirm_text += f"  Путь к файлу: <code>{value}</code>\n"
             elif key == 'es_query':
                 try:
                      query_display = json.dumps(json.loads(value), indent=2, ensure_ascii=False)
                      confirm_text += f"  {key.replace('_', ' ').title()}:\n<pre><code class=\"language-json\">{query_display}</code></pre>\n"
                 except:
                      confirm_text += f"  {key.replace('_', ' ').title()}: <code>Некорректный JSON</code>\n"
             elif key == 'specific_params':
                  pass
             else:
                confirm_text += f"  {key.replace('_', ' ').title()}: <code>{value}</code>\n"

        confirm_text += f"\n<b>Параметры True Tabs:</b>\n"
        for key, value in tt_params.items():
            if key == 'upload_api_token':
                confirm_text += f"  {key.replace('_', ' ').title()}: <code>***</code>\n"
            elif key == 'upload_field_map_json':
                 try:
                     field_map_display = json.dumps(json.loads(value), indent=2, ensure_ascii=False)
                     confirm_text += f"  Сопоставление Field ID:\n<pre><code class=\"language-json\">{field_map_display}</code></pre>\n"
                 except:
                     confirm_text += f"  Сопоставление Field ID: <code>Некорректный JSON</code>\n"
            else:
                confirm_text += f"  {key.replace('_', ' ').title()}: <code>{value}</code>\n"

        confirm_text += f"\nВсе верно? Нажмите 'Загрузить' для старта операции."

        await message.answer(confirm_text, reply_markup=upload_confirm_keyboard(), parse_mode='HTML')

    except json.JSONDecodeError as e:
        await message.answer(f"Ошибка парсинга JSON: {e}. Пожалуйста, проверьте формат и попробуйте снова.")

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