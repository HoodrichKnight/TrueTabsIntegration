import os
import asyncio
import json
import sys
import time
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path
import validators
import re
import tempfile
import shutil
import logging # Импортируем модуль логирования

from aiogram import Router, F, Bot
from aiogram.types import Document
from aiogram.exceptions import TelegramBadRequest, TelegramAPIError # Импортируем TelegramAPIError
from typing import Union
from aiogram.types import Message, CallbackQuery, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton # Импортируем InlineKeyboardButton для клавиатуры отмены
from aiogram.utils.keyboard import InlineKeyboardBuilder # Импортируем для построения клавиатур
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import StateFilter, Command # Импортируем Command фильтр для команды /cancel
import asyncio
from signal import SIGTERM

from telegram_bot.keyboards import (
    main_menu_keyboard,
    source_selection_keyboard,
    upload_confirm_keyboard,
    select_input_method_keyboard,
    select_config_keyboard,
    operation_in_progress_keyboard # Импортируем клавиатуру "Операция в процессе"
)
from telegram_bot.utils.rust_executor import execute_rust_command # Убедитесь, что этот модуль существует
from telegram_bot.database import sqlite_db # Убедитесь, что этот модуль существует и содержит add_upload_record
from telegram_bot import config # Убедитесь, что этот модуль существует и содержит TEMP_FILES_DIR

# Removed top-level import of ConfigProcess to avoid circular import
# Instead, import ConfigProcess locally inside functions where needed

# Получаем логгер для этого модуля
logger = logging.getLogger(__name__)

# Инициализируем роутер
router = Router()

from aiogram.fsm.state import State, StatesGroup

class UploadProcess(StatesGroup):
    select_source = State() # Выбор типа источника
    select_source_input_method = State() # Выбор ручного ввода или сохраненной конфиги для источника

    choose_saved_source_method = State() # Выбор между дефолтной и из списка для источника
    choose_saved_tt_method = State() # Выбор между дефолтной и из списка для TT

    waiting_saved_source_selection = State() # Ожидание выбора сохраненной конфиги источника из списка
    waiting_saved_tt_selection = State() # Ожидание выбора сохраненной конфиги TT из списка


    # Состояния для ручного ввода параметров источника
    waiting_pg_url = State()
    waiting_pg_user = State()
    waiting_pg_pass = State()
    waiting_pg_query = State()

    waiting_mysql_url = State()
    waiting_mysql_user = State()
    waiting_mysql_pass = State()
    waiting_mysql_query = State()

    waiting_sqlite_url = State() # Может быть путем к файлу
    waiting_sqlite_query = State()

    waiting_redis_url = State()
    waiting_redis_pattern = State()

    waiting_mongodb_uri = State()
    waiting_mongo_db = State()
    waiting_mongo_collection = State()

    waiting_elasticsearch_url = State()
    waiting_elasticsearch_index = State()
    waiting_elasticsearch_query = State()

    waiting_file_upload = State() # Ожидание загрузки файла для CSV/Excel


    # Состояния для выбора метода ввода параметров True Tabs
    select_tt_input_method = State()

    # Состояния для ручного ввода параметров True Tabs
    waiting_upload_token = State()
    waiting_datasheet_id = State()
    waiting_field_map_json = State() # Ожидание JSON строки для сопоставления полей
    waiting_record_id = State() # Добавлено для update
    waiting_field_updates_json = State() # Добавлено для update

    # Состояние подтверждения параметров перед запуском
    confirm_parameters = State()

    # Состояние выполнения операции (для отображения "операция в процессе" и перехвата отмены)
    operation_in_progress = State()


# Глобальный словарь для отслеживания запущенных процессов Rust по chat_id
running_processes = {}

# Функция для корректного завершения процесса Rust
async def terminate_process(chat_id: int):
    process = running_processes.get(chat_id)
    if process and process.returncode is None:
        logger.info(f"Отправка SIGTERM процессу Rust для chat {chat_id} (PID {process.pid})")
        process.send_signal(SIGTERM)
        await asyncio.sleep(1)
        if process.returncode is None:
            logger.info(f"Принудительное завершение процесса Rust для chat {chat_id} (PID {process.pid})")
            process.kill()
        del running_processes[chat_id]


# --- Вспомогательные данные и функции ---

# Список обязательных колонок для определенных типов источников или действий
REQUIRED_COLUMN_NAMES: Dict[str, List[str]] = {
    'truetabs': ['ID', 'Name', 'Description'], # Пример для True Tabs
}

# Добавим недостающие специфичные состояния FSM для первого параметра TT, если нужно
# Например, для upload_datasheet_id и upload_field_map_json
# Сейчас есть waiting_upload_token, waiting_datasheet_id, waiting_field_map_json
# Если нужны другие, можно добавить здесь

# Добавим маппинг для аргументов действия 'update' если они используются здесь
# Например, для True Tabs Update:
UPDATE_ARG_MAP = {
    'upload_api_token': '--api-token',
    'upload_datasheet_id': '--datasheet-id',
    'upload_field_map_json': '--field-map-json',
    'record_id': '--record-id',
    'field_updates_json': '--field-updates-json',
}

# В handle_confirm_upload при формировании rust_args добавим поддержку update_arg_map
# Маппинги для аргументов действия 'update' уже добавлены и используются в rust_args


# Хэндлер для состояния waiting_record_id
@router.message(UploadProcess.waiting_record_id)
async def process_record_id(message: Message, state: FSMContext):
    """Обрабатывает ввод ID записи True Tabs для действия update."""
    user_input = message.text.strip()
    if not user_input:
        await message.answer("ID записи не может быть пустым. Пожалуйста, введите ID заново:", reply_markup=cancel_kb)
        return


    tt_params = (await state.get_data()).get('tt_params', {})
    tt_params['record_id'] = user_input
    await state.update_data(tt_params=tt_params)

    # Переход к следующему параметру (field_updates_json)
    await state.set_state(UploadProcess.waiting_field_updates_json)
    await message.answer(f"Введите {get_friendly_param_name('field_updates_json')} (JSON строка с обновлениями полей):", reply_markup=cancel_kb)


# Хэндлер для состояния waiting_field_updates_json
@router.message(UploadProcess.waiting_field_updates_json)
async def process_field_updates_json(message: Message, state: FSMContext):
    """Обрабатывает ввод JSON обновлений полей для действия update."""
    user_input = message.text.strip()

    # Валидация: если ввод не пустой, то должен быть валидным JSON
    if user_input:
        try:
            json.loads(user_input)
        except json.JSONDecodeError:
            await message.answer("Неверный формат JSON. Введите JSON строку обновлений полей заново:", reply_markup=cancel_kb)
            return
        except Exception as e:
            logger.error(f"Ошибка валидации JSON обновлений полей: {e}", exc_info=True)
            await message.answer("Произошла ошибка при валидации JSON. Введите JSON строку обновлений полей заново:", reply_markup=cancel_kb)
            return

    tt_params = (await state.get_data()).get('tt_params', {})
    tt_params['field_updates_json'] = user_input if user_input else None
    await state.update_data(tt_params=tt_params)

    # Ввод параметров True Tabs завершен.
    logger.info(f"Ввод параметров True Tabs завершен (update).")
    # Параметры источника и TT собраны. Переходим к подтверждению.
    await state.set_state(UploadProcess.confirm_parameters)
    state_data = await state.get_data()
    source_params = state_data.get('source_params', {})
    tt_params = state_data.get('tt_params', {})
    selected_source_type = state_data.get('selected_source_type', 'Неизвестно')
    # Строим сообщение для подтверждения
    confirm_text = build_confirmation_message(selected_source_type, source_params, tt_params)

    await message.answer(
        "Параметры True Tabs введены.\nВсе параметры собраны. Проверьте и нажмите 'Загрузить'.\n\n" + confirm_text,
        reply_markup=upload_confirm_keyboard(),
        parse_mode='HTML'
    )


# Более дружественные названия параметров для отображения пользователю
PARAM_NAMES_FRIENDLY: Dict[str, str] = {
    'source_url': 'URL/путь к источнику',
    'source_user': 'Пользователь',
    'source_pass': 'Пароль',
    'source_query': 'Запрос (SQL/JSON)',
    'mongo_db': 'Имя базы данных MongoDB',
    'mongo_collection': 'Имя коллекции MongoDB',
    'redis_pattern': 'Паттерн ключей Redis',
    'es_index': 'Имя индекса Elasticsearch',
    'es_query': 'JSON запрос Elasticsearch',
    'upload_api_token': 'API токен True Tabs',
    'upload_datasheet_id': 'ID таблицы True Tabs',
    'upload_field_map_json': 'JSON сопоставления полей',
    'source_url_file': 'Путь к файлу', # Для CSV/Excel
    'specific_params': 'Специфические параметры (JSON)',
    'source_pg_url': 'URL PostgreSQL',
    'source_mysql_url': 'URL MySQL',
    'source_sqlite_url': 'Путь к файлу SQLite',
    'source_mongodb_uri': 'URI MongoDB',
    'source_redis_url': 'URL Redis',
    'source_elasticsearch_url': 'URL Elasticsearch',
    'upload_expected_headers': 'Ожидаемые заголовки (JSON)',  # Добавлено для ручного ввода заголовков
    'record_id': 'ID записи True Tabs',
    'field_updates_json': 'JSON обновлений полей',
}

# Добавим другие специфичные состояния для первого параметра TT, если нужно
# Сейчас есть waiting_upload_token, waiting_datasheet_id, waiting_field_map_json
# Добавим состояния для record_id и field_updates_json для действия update

class UploadProcess(StatesGroup):
    select_source = State() # Выбор типа источника
    select_source_input_method = State() # Выбор ручного ввода или сохраненной конфиги для источника

    choose_saved_source_method = State() # Выбор между дефолтной и из списка для источника
    choose_saved_tt_method = State() # Выбор между дефолтной и из списка для TT

    waiting_saved_source_selection = State() # Ожидание выбора сохраненной конфиги источника из списка
    waiting_saved_tt_selection = State() # Ожидание выбора сохраненной конфиги TT из списка


    # Состояния для ручного ввода параметров источника
    waiting_pg_url = State()
    waiting_pg_user = State()
    waiting_pg_pass = State()
    waiting_pg_query = State()

    waiting_mysql_url = State()
    waiting_mysql_user = State()
    waiting_mysql_pass = State()
    waiting_mysql_query = State()

    waiting_sqlite_url = State() # Может быть путем к файлу
    waiting_sqlite_query = State()

    waiting_redis_url = State()
    waiting_redis_pattern = State()

    waiting_mongodb_uri = State()
    waiting_mongo_db = State()
    waiting_mongo_collection = State()

    waiting_elasticsearch_url = State()
    waiting_elasticsearch_index = State()
    waiting_elasticsearch_query = State()

    waiting_file_upload = State() # Ожидание загрузки файла для CSV/Excel


    # Состояния для выбора метода ввода параметров True Tabs
    select_tt_input_method = State()

    # Состояния для ручного ввода параметров True Tabs
    waiting_upload_token = State()
    waiting_datasheet_id = State()
    waiting_field_map_json = State() # Ожидание JSON строки для сопоставления полей
    waiting_record_id = State() # Добавлено для update
    waiting_field_updates_json = State() # Добавлено для update

    # Состояние подтверждения параметров перед запуском
    confirm_parameters = State()

    # Состояние выполнения операции (для отображения "операция в процессе" и перехвата отмены)
    operation_in_progress = State()

# Добавим другие специфичные состояния для первого параметра TT, если нужно
# Сейчас есть waiting_upload_token, waiting_datasheet_id, waiting_field_map_json
# Добавим состояния для record_id и field_updates_json для действия update

class UploadProcess(StatesGroup):
    select_source = State() # Выбор типа источника
    select_source_input_method = State() # Выбор ручного ввода или сохраненной конфиги для источника

    choose_saved_source_method = State() # Выбор между дефолтной и из списка для источника
    choose_saved_tt_method = State() # Выбор между дефолтной и из списка для TT

    waiting_saved_source_selection = State() # Ожидание выбора сохраненной конфиги источника из списка
    waiting_saved_tt_selection = State() # Ожидание выбора сохраненной конфиги TT из списка


    # Состояния для ручного ввода параметров источника
    waiting_pg_url = State()
    waiting_pg_user = State()
    waiting_pg_pass = State()
    waiting_pg_query = State()

    waiting_mysql_url = State()
    waiting_mysql_user = State()
    waiting_mysql_pass = State()
    waiting_mysql_query = State()

    waiting_sqlite_url = State() # Может быть путем к файлу
    waiting_sqlite_query = State()

    waiting_redis_url = State()
    waiting_redis_pattern = State()

    waiting_mongodb_uri = State()
    waiting_mongo_db = State()
    waiting_mongo_collection = State()

    waiting_elasticsearch_url = State()
    waiting_elasticsearch_index = State()
    waiting_elasticsearch_query = State()

    waiting_file_upload = State() # Ожидание загрузки файла для CSV/Excel


    # Состояния для выбора метода ввода параметров True Tabs
    select_tt_input_method = State()

    # Состояния для ручного ввода параметров True Tabs
    waiting_upload_token = State()
    waiting_datasheet_id = State()
    waiting_field_map_json = State() # Ожидание JSON строки для сопоставления полей
    waiting_record_id = State() # Добавлено для update
    waiting_field_updates_json = State() # Добавлено для update

    # Состояние подтверждения параметров перед запуском
    confirm_parameters = State()

    # Состояние выполнения операции (для отображения "операция в процессе" и перехвата отмены)
    operation_in_progress = State()


from telegram_bot.handlers.shared_constants import SOURCE_PARAMS_ORDER, get_friendly_param_name



# Клавиатура с кнопкой "Отмена" для сброса FSM
cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]])


# --- Класс состояний FSM для процесса загрузки/извлечения ---
class UploadProcess(StatesGroup):
    select_source = State() # Выбор типа источника
    select_source_input_method = State() # Выбор ручного ввода или сохраненной конфиги для источника

    choose_saved_source_method = State() # Выбор между дефолтной и из списка для источника
    choose_saved_tt_method = State() # Выбор между дефолтной и из списка для TT

    waiting_saved_source_selection = State() # Ожидание выбора сохраненной конфиги источника из списка
    waiting_saved_tt_selection = State() # Ожидание выбора сохраненной конфиги TT из списка


    # Состояния для ручного ввода параметров источника
    waiting_pg_url = State()
    waiting_pg_user = State()
    waiting_pg_pass = State()
    waiting_pg_query = State()

    waiting_mysql_url = State()
    waiting_mysql_user = State()
    waiting_mysql_pass = State()
    waiting_mysql_query = State()

    waiting_sqlite_url = State() # Может быть путем к файлу
    waiting_sqlite_query = State()

    waiting_redis_url = State()
    waiting_redis_pattern = State()

    waiting_mongodb_uri = State()
    waiting_mongo_db = State()
    waiting_mongo_collection = State()

    waiting_elasticsearch_url = State()
    waiting_elasticsearch_index = State()
    waiting_elasticsearch_query = State()

    waiting_file_upload = State() # Ожидание загрузки файла для CSV/Excel


    # Состояния для выбора метода ввода параметров True Tabs
    select_tt_input_method = State()

    # Состояния для ручного ввода параметров True Tabs
    waiting_upload_token = State()
    waiting_datasheet_id = State()
    waiting_field_map_json = State() # Ожидание JSON строки для сопоставления полей

    # Состояние подтверждения параметров перед запуском
    confirm_parameters = State()

    # Состояние выполнения операции (для отображения "операция в процессе" и перехвата отмены)
    operation_in_progress = State()


# Определяем порядок запроса параметров для каждого типа источника
# Этот порядок используется в FSM для ручного ввода
SOURCE_PARAMS_ORDER: Dict[str, List[str]] = {
    "postgres": ["source_url", "source_user", "source_pass", "source_query"],
    "mysql": ["source_url", "source_user", "source_pass", "source_query"],
    "sqlite": ["source_url", "source_query"], # source_url здесь - путь к файлу .db
    "mongodb": ["source_url", "mongo_db", "mongo_collection"], # source_url здесь - URI
    "redis": ["source_url", "redis_pattern"], # source_url здесь - URL
    "elasticsearch": ["source_url", "es_index", "es_query"], # source_url здесь - URL
    "csv": ["source_url"], # source_url здесь - путь к файлу .csv
    "excel": ["source_url"], # source_url здесь - путь к файлу .xlsx/.xls
}

# Список источников, которые временно отключены или в разработке
DISABLED_SOURCES: List[str] = [
    # 'labguru', # Пример отключенного источника
]


# --- Хэндлеры роутера ---

router = Router()

# Общий хэндлер для отмены любого активного FSM состояния
# Реагирует на команду /cancel или callback_data="cancel"
@router.message(Command("cancel"), StateFilter("*"))
@router.callback_query(F.data == "cancel", StateFilter("*"))
async def cancel_fsm(callback_or_message: Union[Message, CallbackQuery], state: FSMContext):
    """Сброс FSM состояния и возврат в главное меню."""
    current_state = await state.get_state()
    if current_state is None:
        # Если нет активного состояния FSM, просто возвращаемся в главное меню (для кнопки "Отмена" в меню)
        if isinstance(callback_or_message, CallbackQuery):
            await callback_or_message.message.edit_text(
                "Операция отменена. Чем могу помочь?",
                reply_markup=main_menu_keyboard()
            )
            await callback_or_message.answer("Отменено.")
        else:
            await callback_or_message.answer(
                "Операция отменена. Чем могу помочь?",
                reply_markup=main_menu_keyboard()
            )
        return

    logger.info(f"Отмена FSM состояния {current_state} для пользователя {callback_or_message.from_user.id}")

    # Очищаем состояние FSM
    await state.clear()

    # Отправляем сообщение об отмене и главное меню
    message_text = "Операция отменена."
    if isinstance(callback_or_message, CallbackQuery):
        try:
            await callback_or_message.message.edit_text(
                message_text,
                reply_markup=main_menu_keyboard()
            )
            await callback_or_message.answer("Отменено.")
        except TelegramBadRequest:
            # Если сообщение уже было изменено или удалено, отправляем новое
             await callback_or_message.message.answer(
                message_text,
                reply_markup=main_menu_keyboard()
            )
             await callback_or_message.answer("Отменено.")

    else: # Если была команда /cancel
        await callback_or_message.answer(
            message_text,
            reply_markup=main_menu_keyboard()
        )


# --- Хэндлер выбора источника данных ---
@router.callback_query(F.data == "select_source")
async def select_source_handler(callback: CallbackQuery, state: FSMContext):
    from telegram_bot.handlers.config_handlers import ConfigProcess
    """Начинает процесс выбора источника данных и запуска операции."""
    await state.set_state(UploadProcess.select_source)
    await callback.message.edit_text("Выберите источник данных:", reply_markup=source_selection_keyboard())
    await callback.answer()


# --- Хэндлер начала процесса загрузки по типу источника ---
@router.callback_query(F.data.startswith("start_upload_process:"))
async def start_upload_process(callback: CallbackQuery, state: FSMContext):
    from telegram_bot.handlers.config_handlers import ConfigProcess
    """Обрабатывает выбор типа источника и предлагает выбрать метод ввода параметров."""
    source_type = callback.data.split(":")[1]

    if source_type in DISABLED_SOURCES:
        await callback.message.edit_text(f"Источник данных '{source_type}' временно недоступен.", reply_markup=source_selection_keyboard())
        await callback.answer()
        return

    # Сохраняем выбранный тип источника и инициализируем словарь для параметров
    await state.update_data(selected_source_type=source_type, source_params={})

    # Переходим к выбору метода ввода параметров источника
    await state.set_state(UploadProcess.select_source_input_method)
    await callback.message.edit_text(
        f"Выбран источник: <b>{source_type.capitalize()}</b>.\nВыберите способ ввода параметров источника:",
        reply_markup=select_input_method_keyboard('source'),
        parse_mode='HTML'
    )
    await callback.answer()


# --- Хэндлер выбора метода ввода параметров источника (ручной или сохраненный) ---
@router.callback_query(F.data.startswith("select_input_method:"), UploadProcess.select_source_input_method)
async def select_source_input_method(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор ручного ввода или использования сохраненной конфигурации источника."""
    parts = callback.data.split(":")
    method = parts[1] # 'manual' or 'saved'
    config_type_from_callback = parts[2] # 'source' or 'tt' (должно быть 'source' на этом шаге)

    state_data = await state.get_data()
    source_type = state_data.get("selected_source_type")

    # Базовая проверка на корректность данных и наличие типа источника
    if config_type_from_callback != 'source' or not source_type:
         logger.error(f"Неверный callback_data или отсутствует source_type в select_source_input_method: {callback.data}, data: {state_data}")
         await callback.message.edit_text("Ошибка в данных запроса. Начните заново.", reply_markup=main_menu_keyboard())
         await state.clear()
         await callback.answer()
         return


    if method == 'manual':
        # Определяем порядок параметров для ручного ввода
        params_order = SOURCE_PARAMS_ORDER.get(source_type, [])
        # Исключаем source_type, т.к. он уже выбран
        params_order = [p for p in params_order if p != "source_type"]

        # Если для данного источника не требуются параметры, переходим сразу к выбору метода ввода TT
        if not params_order:
            await state.update_data(source_params={}) # Убедимся, что source_params пуст или инициализирован
            await state.set_state(UploadProcess.select_tt_input_method) # Переход к выбору метода ввода TT
            await callback.message.edit_text(
                f"Параметры источника для <b>{source_type.capitalize()}</b> не требуются.\nВыберите способ ввода параметров True Tabs:",
                reply_markup=select_input_method_keyboard('tt'),
                parse_mode='HTML'
            )
            await callback.answer()
            return

        # Сохраняем порядок параметров и индекс текущего параметра
        await state.update_data(param_keys_order=params_order, current_param_index=0)

        # Определяем начальное состояние FSM для первого параметра (для более специфичной валидации, если нужно)
        # Если специфичного состояния нет, можно использовать общее state(UploadProcess.waiting_source_param)
        first_param_key = params_order[0]
        initial_state = None

        # Определяем специфичное состояние для первого параметра, если оно существует
        if source_type == 'postgres': initial_state = UploadProcess.waiting_pg_url
        elif source_type == 'mysql': initial_state = UploadProcess.waiting_mysql_url
        elif source_type == 'sqlite': initial_state = UploadProcess.waiting_sqlite_url # Это может быть путь к файлу .db
        elif source_type == 'redis': initial_state = UploadProcess.waiting_redis_url
        elif source_type == 'mongodb': initial_state = UploadProcess.waiting_mongodb_uri
        elif source_type == 'elasticsearch': initial_state = UploadProcess.waiting_elasticsearch_url
        elif source_type in ['csv', 'excel']: initial_state = UploadProcess.waiting_file_upload # Для файловых источников ожидаем загрузку файла
        # Добавлены специфичные состояния для первого параметра, включая record_id и field_updates_json для update
        # Если нет специфичного состояния для первого параметра, используем общее
        if initial_state is None:
            logger.warning(f"Не определено специфичное начальное состояние FSM для {source_type}, первого параметра {first_param_key}. Используется общее.")
            # Можно создать общее состояние типа UploadProcess.waiting_generic_source_param
            # Но текущая структура предполагает специфичные состояния для ручного ввода
            # Для поддержанных источников, у нас есть специфичные состояния.
            # Если здесь оказалась неподдерживаемая комбинация, лучше сообщить об ошибке.
            await callback.message.edit_text(f"Ошибка конфигурации для типа источника '{source_type}'.", reply_markup=main_menu_keyboard())
            await state.clear()
            await callback.answer()
            return


        # Устанавливаем состояние FSM для ввода первого параметра
        await state.set_state(initial_state)
        friendly_name = get_friendly_param_name(first_param_key) # Получаем дружественное имя параметра
        await callback.message.edit_text(f"Введите {friendly_name}:", reply_markup=cancel_kb) # Просим ввести первый параметр

        await callback.answer()

    elif method == 'saved':
        # Пользователь выбрал использовать сохраненную конфигурацию
        # Сначала проверяем, есть ли дефолтная конфигурация для этого типа источника
        default_config = await sqlite_db.get_default_source_config(source_type)

        if default_config:
            # Если дефолтная есть, предлагаем использовать ее или выбрать из списка
            builder = InlineKeyboardBuilder()
            builder.row(
                InlineKeyboardButton(text=f"🚀 Использовать по умолчанию: {default_config.get('name', 'Без имени')}", callback_data=f"use_default_source_config:{default_config.get('name', 'N/A')}")
            )
            builder.row(
                InlineKeyboardButton(text="📋 Выбрать из списка сохраненных", callback_data="list_saved_source_configs_for_selection")
            )
            builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"))
            keyboard = builder.as_markup()

            text = f"Найдена конфигурация источника <b>{source_type.capitalize()}</b> по умолчанию.\nВыберите действие:"

            await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
            # Переходим в состояние выбора способа использования сохраненной конфиги
            await state.set_state(UploadProcess.choose_saved_source_method)

        else:
            # Если дефолтной нет, сразу предлагаем выбрать из списка
            text = f"Дефолтная конфигурация для источника <b>{source_type.capitalize()}</b> не найдена.\nВыберите сохраненную конфигурацию источника из списка:"
            source_configs = await sqlite_db.list_source_configs() # Получаем список всех сохраненных конфигов источников

            if not source_configs:
                # Если нет вообще никаких сохраненных конфигов источников
                await callback.message.edit_text("Сохраненных конфигураций источников не найдено. Пожалуйста, выберите ручной ввод.", reply_markup=select_input_method_keyboard('source'))
                # Возвращаемся в состояние выбора метода ввода
                await state.set_state(UploadProcess.select_source_input_method)
            else:
                # Отображаем список сохраненных конфигов источников
                keyboard = select_config_keyboard(source_configs, 'source_select') # Используем select_config_keyboard с префиксом 'source_select'
                await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
                # Переходим в состояние ожидания выбора из списка
                await state.set_state(UploadProcess.waiting_saved_source_selection)


        await callback.answer()

    else:
        # Неверный метод ввода (не 'manual' и не 'saved')
        logger.error(f"Неверный метод ввода в select_source_input_method: {method}")
        await callback.message.edit_text("Неверный выбор метода ввода. Начните заново.", reply_markup=main_menu_keyboard())
        await state.clear()
        await callback.answer()


# --- НОВЫЙ ХЭНДЛЕР: Выбор способа использования сохраненной конфигурации источника (дефолт или список) ---
@router.callback_query(F.data.startswith("use_default_source_config:"), UploadProcess.choose_saved_source_method)
async def use_default_source_config_handler(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор использования дефолтной конфигурации источника."""
    config_name = callback.data.split(":")[1] # Имя дефолтной конфиги (для надежности, хотя можно получить через get_default)
    source_type = (await state.get_data()).get("selected_source_type") # Получаем source_type из стейта

    if not source_type:
         logger.error("source_type отсутствует в state при использовании дефолтной конфиги источника.")
         await callback.message.edit_text("Произошла ошибка. Начните заново.", reply_markup=main_menu_keyboard())
         await state.clear()
         await callback.answer()
         return

    default_config = await sqlite_db.get_default_source_config(source_type) # Получаем дефолтную конфигу

    if default_config and default_config.get('name') == config_name: # Убеждаемся, что это действительно дефолтная конфига с таким именем
        # Сохраняем параметры дефолтной конфигурации источника в данные состояния
        await state.update_data(source_params=default_config)

        # Переходим к выбору метода ввода параметров True Tabs
        await state.set_state(UploadProcess.select_tt_input_method)
        await callback.message.edit_text(
            f"Использована конфигурация источника по умолчанию: <b>{default_config.get('name', 'Без имени')}</b> ({default_config.get('source_type')}).\n"
            f"Выберите способ ввода параметров True Tabs:",
            reply_markup=select_input_method_keyboard('tt'), # Переход к выбору метода ввода TT
            parse_mode='HTML'
        )

    else:
        logger.error(f"Дефолтная конфига источника не найдена или имя не совпадает при use_default_source_config: {config_name}")
        await callback.message.edit_text(f"Ошибка: Дефолтная конфигурация источника '{config_name}' не найдена.", reply_markup=main_menu_keyboard())
        await state.clear()

    await callback.answer()

# Хэндлер для callback_data="list_saved_source_configs_for_selection" - Переход к списку сохраненных конфигов источников
@router.callback_query(F.data == "list_saved_source_configs_for_selection", UploadProcess.choose_saved_source_method)
async def list_all_source_configs_for_selection(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор просмотра списка сохраненных конфигураций источников."""
    text = "Выберите сохраненную конфигурацию источника:"
    source_configs = await sqlite_db.list_source_configs() # Получаем список всех сохраненных конфигов

    if not source_configs:
         # Если нет сохраненных конфигов, возвращаемся к выбору метода ввода
         await callback.message.edit_text("Сохраненных конфигураций источников не найдено.", reply_markup=select_input_method_keyboard('source'))
         await state.set_state(UploadProcess.select_source_input_method)
    else:
        # Отображаем список сохраненных конфигов
        keyboard = select_config_keyboard(source_configs, 'source_select') # Используем select_config_keyboard с префиксом 'source_select'
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
        # Переходим в состояние ожидания выбора из списка
        await state.set_state(UploadProcess.waiting_saved_source_selection)

    await callback.answer()


# --- Хэндлер выбора сохраненной конфигурации источника (из списка) ---
# Срабатывает в состоянии UploadProcess.waiting_saved_source_selection
@router.callback_query(F.data.startswith("select_config:source_select:"), UploadProcess.waiting_saved_source_selection)
async def process_saved_source_config_selection(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор сохраненной конфигурации источника из списка."""
    config_name = callback.data.split(":")[2] # Имя выбранной конфигурации источника
    saved_config = await sqlite_db.get_source_config(config_name) # Получаем полные данные выбранной конфиги

    if saved_config:
        # Сохраняем параметры выбранной конфигурации источника в данные состояния
        # Убедимся, что source_type также сохраняется, если он не был в state
        await state.update_data(selected_source_type=saved_config.get('source_type'), source_params=saved_config)

        # Переходим к выбору метода ввода параметров True Tabs
        await state.set_state(UploadProcess.select_tt_input_method)
        await callback.message.edit_text(
            f"Использована конфигурация источника: <b>{saved_config.get('name', 'Без имени')}</b> ({saved_config.get('source_type')}).\n"
            f"Выберите способ ввода параметров True Tabs:",
            reply_markup=select_input_method_keyboard('tt'), # Переход к выбору метода ввода TT
            parse_mode='HTML'
        )

    else:
        logger.error(f"Выбранная сохраненная конфига источника '{config_name}' не найдена при selection.")
        await callback.message.edit_text(f"Ошибка: Конфигурация источника '{config_name}' не найдена.", reply_markup=main_menu_keyboard())
        await state.clear()

    await callback.answer()

# --- Обработчик ручного ввода параметров источника ---

# Общий хэндлер для всех состояний ожидания ввода параметра источника, кроме файловых
@router.message(StateFilter(
    UploadProcess.waiting_pg_url, UploadProcess.waiting_pg_user, UploadProcess.waiting_pg_pass, UploadProcess.waiting_pg_query,
    UploadProcess.waiting_mysql_url, UploadProcess.waiting_mysql_user, UploadProcess.waiting_mysql_pass, UploadProcess.waiting_mysql_query,
    UploadProcess.waiting_sqlite_url, UploadProcess.waiting_sqlite_query, # sqlite_url может быть путем, но вводится как текст
    UploadProcess.waiting_redis_url, UploadProcess.waiting_redis_pattern,
    UploadProcess.waiting_mongodb_uri, UploadProcess.waiting_mongo_db, UploadProcess.waiting_mongo_collection,
    UploadProcess.waiting_elasticsearch_url, UploadProcess.waiting_elasticsearch_index, UploadProcess.waiting_elasticsearch_query
))
async def process_source_param_manual(message: Message, state: FSMContext):
    """
    Обрабатывает ввод параметров источника пользователем в ручном режиме.
    Валидирует ввод и переходит к следующему параметру или к выбору метода ввода TT.
    """
    user_input = message.text.strip()

    state_data = await state.get_data()
    source_type = state_data['selected_source_type']
    param_keys_order = state_data['param_keys_order']
    current_param_index = state_data['current_param_index']
    # Инициализируем source_params, если он еще не инициализирован (например, если это первый параметр)
    source_params = state_data.get('source_params', {})

    current_param_key = param_keys_order[current_param_index]
    friendly_param_name = get_friendly_param_name(current_param_key)

    validation_error = None
    if not user_input and current_param_key not in ['source_pass', 'redis_pattern']: # Пароль и паттерн могут быть пустыми
        validation_error = f"Параметр '{friendly_param_name}' не может быть пустым."
    else:
        if current_param_key in ['source_url', 'source_pg_url', 'source_mysql_url', 'source_sqlite_url', 'source_mongodb_uri', 'source_redis_url', 'source_elasticsearch_url']:
            if current_param_key == 'source_sqlite_url':
                if not Path(user_input).exists():
                    validation_error = f"Путь к файлу SQLite не существует: {user_input}"
            else:
                if not validators.url(user_input):
                    validation_error = f"Неверный URL: {user_input}"
        elif current_param_key in ['source_query', 'es_query']:
            try:
                json.loads(user_input)
            except Exception:
                validation_error = f"Параметр '{friendly_param_name}' должен быть валидным JSON."
        elif current_param_key == 'upload_field_map_json':
            if user_input != '-':
                try:
                    json.loads(user_input)
                except Exception:
                    validation_error = f"Параметр '{friendly_param_name}' должен быть валидным JSON или '-'."

    if validation_error:
        await message.answer(f"Ошибка валидации: {validation_error}\nПожалуйста, введите параметр '{friendly_param_name}' снова:", reply_markup=cancel_kb)
        return

    source_params[current_param_key] = user_input
    await state.update_data(source_params=source_params)

    next_param_index = current_param_index + 1

    if next_param_index < len(param_keys_order):
        await state.update_data(current_param_index=next_param_index)
        next_param_key = param_keys_order[next_param_index]
        next_friendly_name = get_friendly_param_name(next_param_key)

        next_state = None
        if next_param_key == 'source_url':
            if source_type == 'postgres': next_state = UploadProcess.waiting_pg_url
            elif source_type == 'mysql': next_state = UploadProcess.waiting_mysql_url
            elif source_type == 'sqlite': next_state = UploadProcess.waiting_sqlite_url
            elif source_type == 'redis': next_state = UploadProcess.waiting_redis_url
            elif source_type == 'mongodb': next_state = UploadProcess.waiting_mongodb_uri
            elif source_type == 'elasticsearch': next_state = UploadProcess.waiting_elasticsearch_url
        elif next_param_key == 'source_user':
            if source_type in ['postgres', 'mysql']: next_state = UploadProcess.waiting_pg_user if source_type == 'postgres' else UploadProcess.waiting_mysql_user
        elif next_param_key == 'source_pass':
            if source_type in ['postgres', 'mysql']: next_state = UploadProcess.waiting_pg_pass if source_type == 'postgres' else UploadProcess.waiting_mysql_pass
        elif next_param_key == 'source_query':
            if source_type == 'postgres': next_state = UploadProcess.waiting_pg_query
            elif source_type == 'mysql': next_state = UploadProcess.waiting_mysql_query
            elif source_type == 'sqlite': next_state = UploadProcess.waiting_sqlite_query
        elif next_param_key == 'mongo_db': next_state = UploadProcess.waiting_mongo_db
        elif next_param_key == 'mongo_collection': next_state = UploadProcess.waiting_mongo_collection
        elif next_param_key == 'redis_pattern': next_state = UploadProcess.waiting_redis_pattern
        elif next_param_key == 'es_index': next_state = UploadProcess.waiting_elasticsearch_index
        elif next_param_key == 'es_query': next_state = UploadProcess.waiting_elasticsearch_query

        if next_state is None:
            logger.error(f"Не определено следующее состояние FSM для типа источника {source_type}, следующего параметра {next_param_key}.")
            await message.answer("Произошла внутренняя ошибка при определении следующего параметра. Начните заново.", reply_markup=main_menu_keyboard())
            await state.clear()
            return

        await state.set_state(next_state)
        await message.answer(f"Введите {next_friendly_name}:", reply_markup=cancel_kb)

    else:
        logger.info(f"Ввод параметров источника {source_type} завершен.")
        await state.set_state(UploadProcess.select_tt_input_method)
        await message.answer(
            f"Параметры источника для <b>{source_type.capitalize()}</b> введены.\nВыберите способ ввода параметров True Tabs:",
            reply_markup=select_input_method_keyboard('tt'),
            parse_mode='HTML'
        )

# Обработчик загрузки файла для файловых источников (csv)
@router.message(F.document, UploadProcess.waiting_file_upload)
async def process_uploaded_file(message: Message, state: FSMContext, bot: Bot):
    """Обрабатывает загрузку файла для источников типа CSV."""
    await message.answer("Получен файл, обрабатываю...")

    state_data = await state.get_data()
    source_type = state_data['selected_source_type']
    original_file_name = message.document.file_name

    # Проверка расширения файла
    allowed_extensions = {
        'csv': ['.csv'],
        # Removed excel extensions
    }
    expected_extensions = allowed_extensions.get(source_type, [])
    file_extension = Path(original_file_name).suffix.lower()

    if not expected_extensions or file_extension not in expected_extensions:
         friendly_name = get_friendly_param_name(source_type)
         await message.answer(f"Ошибка: Ожидался файл формата {', '.join(expected_extensions)} для источника '{friendly_name}'. Пожалуйста, отправьте корректный файл.", reply_markup=cancel_kb)
         # Остаемся в текущем состоянии UploadProcess.waiting_file_upload
         return

    temp_dir = None
    temp_file_path = None

    try:
        # Создаем временную директорию для хранения файла
        temp_dir = tempfile.mkdtemp(prefix=f"tt_upload_{message.from_user.id}_")
        # Формируем полный путь к временному файлу, сохраняя исходное имя
        temp_file_path = Path(temp_dir) / original_file_name

        # Скачиваем файл из Telegram на сервер бота
        file_info = await bot.get_file(message.document.file_id)
        await bot.download_file(file_info.file_path, temp_file_path)

        logger.info(f"Файл '{original_file_name}' скачан во временный путь: {temp_file_path} для chat {message.chat.id}")

        # Сохраняем путь к файлу как параметр источника (source_url) и путь к временной директории
        await state.update_data(
            source_params={'source_url': str(temp_file_path)}, # source_url теперь путь к временному файлу
            temp_file_upload_dir=temp_dir # Сохраняем путь к временной директории для последующей очистки
        )

        # Ввод параметров источника завершен (для файловых источников source_url - единственный параметр из списка)
        # Переходим к выбору метода ввода параметров True Tabs
        await state.set_state(UploadProcess.select_tt_input_method)

        await message.answer(
            f"Файл '{original_file_name}' успешно загружен.\nВыберите способ ввода параметров True Tabs:",
            reply_markup=select_input_method_keyboard('tt')
        )


    except TelegramBadRequest as e:
        logger.error(f"Telegram API error downloading file: {e}", exc_info=True)
        # Попытка очистить временную директорию, если она была создана
        if temp_dir and os.path.exists(temp_dir):
             try: shutil.rmtree(temp_dir)
             except Exception as cleanup_e: logger.error(f"Ошибка очистки temp dir {temp_dir} после ошибки скачивания: {cleanup_e}")

        await message.answer("Произошла ошибка при скачивании файла из Telegram. Попробуйте еще раз.", reply_markup=cancel_kb)
        # Остаемся в текущем состоянии UploadProcess.waiting_file_upload

    except Exception as e:
        logger.error(f"Error processing uploaded file: {e}", exc_info=True)
        # Попытка очистить временную директорию
        if temp_dir and os.path.exists(temp_dir):
             try: shutil.rmtree(temp_dir)
             except Exception as cleanup_e: logger.error(f"Ошибка очистки temp dir {temp_dir} после внутренней ошибки: {cleanup_e}")

        await message.answer("Произошла внутренняя ошибка при обработке файла.", reply_markup=cancel_kb)
        # Остаемся в текущем состоянии UploadProcess.waiting_file_upload


# --- Хэндлер выбора метода ввода параметров True Tabs (ручной или сохраненный) ---
@router.callback_query(F.data.startswith("select_input_method:"), UploadProcess.select_tt_input_method)
async def select_tt_input_method(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор ручного ввода или использования сохраненной конфигурации True Tabs."""
    parts = callback.data.split(":")
    method = parts[1] # 'manual' or 'saved'
    config_type_from_callback = parts[2] # 'source' or 'tt' (должно быть 'tt' на этом шаге)

    state_data = await state.get_data()
    # selected_source_type и source_params уже должны быть в state

    # Базовая проверка
    if config_type_from_callback != 'tt':
         logger.error(f"Неверный callback_data или отсутствует 'tt' в select_tt_input_method: {callback.data}, data: {state_data}")
         await callback.message.edit_text("Ошибка в данных запроса. Начните заново.", reply_markup=main_menu_keyboard())
         await state.clear()
         await callback.answer()
         return


    if method == 'manual':
        # Определяем порядок параметров для ручного ввода параметров True Tabs
        tt_params_order = ["upload_api_token", "upload_datasheet_id", "upload_field_map_json", "record_id", "field_updates_json"]
        # Инициализируем словарь для параметров True Tabs
        tt_params={}
        await state.update_data(tt_params=tt_params)
        # Сохраняем порядок параметров и индекс текущего параметра
        await state.update_data(tt_params_order=tt_params_order, current_tt_param_index=0)

        # Переходим в состояние ожидания ввода первого параметра True Tabs
        first_param_key = tt_params_order[0]
        initial_state = None
        # Определяем специфичное состояние для первого параметра True Tabs
        if first_param_key == 'upload_api_token': initial_state = UploadProcess.waiting_upload_token
        elif first_param_key == 'upload_datasheet_id': initial_state = UploadProcess.waiting_datasheet_id
        elif first_param_key == 'upload_field_map_json': initial_state = UploadProcess.waiting_field_map_json
        elif first_param_key == 'record_id': initial_state = UploadProcess.waiting_record_id
        elif first_param_key == 'field_updates_json': initial_state = UploadProcess.waiting_field_updates_json

        if initial_state:
            await state.set_state(initial_state)
            friendly_name = get_friendly_param_name(first_param_key)
            await callback.message.edit_text(f"Введите {friendly_name}:", reply_markup=cancel_kb)
        else:
             logger.error(f"Не определено специфичное начальное состояние FSM для первого параметра TT {first_param_key}.")
             await callback.message.edit_text("Ошибка конфигурации для параметров True Tabs. Начните заново.", reply_markup=main_menu_keyboard())
             await state.clear()

        await callback.answer()

    elif method == 'saved':
        # Пользователь выбрал использовать сохраненную конфигурацию True Tabs
        # Сначала проверяем, есть ли дефолтная конфигурация True Tabs
        default_config = await sqlite_db.get_default_tt_config()

        if default_config:
            # Если дефолтная есть, предлагаем использовать ее или выбрать из списка
            builder = InlineKeyboardBuilder()
            builder.row(
                InlineKeyboardButton(text=f"🚀 Использовать по умолчанию: {default_config.get('name', 'Без имени')}", callback_data=f"use_default_tt_config:{default_config.get('name', 'N/A')}")
            )
            builder.row(
                InlineKeyboardButton(text="📋 Выбрать из списка сохраненных", callback_data="list_saved_tt_configs_for_selection")
            )
            builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"))
            keyboard = builder.as_markup()

            text = "Найдена конфигурация True Tabs по умолчанию.\nВыберите действие:"

            await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
            # Переходим в состояние выбора способа использования сохраненной конфиги True Tabs
            await state.set_state(UploadProcess.choose_saved_tt_method)

        else:
            # Если дефолтной нет, сразу предлагаем выбрать из списка
            text = "Дефолтная конфигурация True Tabs не найдена.\nВыберите сохраненную конфигурацию True Tabs из списка:"
            tt_configs = await sqlite_db.list_tt_configs() # Получаем список всех сохраненных конфигов True Tabs

            if not tt_configs:
                # Если нет вообще никаких сохраненных конфигов True Tabs
                 await callback.message.edit_text("Сохраненных конфигураций True Tabs не найдено. Пожалуйста, выберите ручной ввод.", reply_markup=select_input_method_keyboard('tt'))
                 # Возвращаемся в состояние выбора метода ввода
                 await state.set_state(UploadProcess.select_tt_input_method)
            else:
                # Отображаем список сохраненных конфигов True Tabs
                keyboard = select_config_keyboard(tt_configs, 'tt_select') # Используем select_config_keyboard с префиксом 'tt_select'
                await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
                # Переходим в состояние ожидания выбора из списка
                await state.set_state(UploadProcess.waiting_saved_tt_selection)


        await callback.answer()

    else:
        # Неверный метод ввода
        logger.error(f"Неверный метод ввода в select_tt_input_method: {method}")
        await callback.message.edit_text("Неверный выбор метода ввода. Начните заново.", reply_markup=main_menu_keyboard())
        await state.clear()
        await callback.answer()


# --- НОВЫЙ ХЭНДЛЕР: Выбор способа использования сохраненной конфигурации True Tabs (дефолт или список) ---
@router.callback_query(F.data.startswith("use_default_tt_config:"), UploadProcess.choose_saved_tt_method)
async def use_default_tt_config_handler(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор использования дефолтной конфигурации True Tabs."""
    config_name = callback.data.split(":")[1] # Имя дефолтной конфиги (для надежности)

    default_config = await sqlite_db.get_default_tt_config() # Получаем дефолтную конфигу

    if default_config and default_config.get('name') == config_name: # Убеждаемся, что это действительно дефолтная конфига с таким именем
        # Сохраняем параметры дефолтной конфигурации True Tabs в данные состояния
        await state.update_data(tt_params={
            "upload_api_token": default_config.get("upload_api_token"),
            "upload_datasheet_id": default_config.get("upload_datasheet_id"),
            "upload_field_map_json": default_config.get("upload_field_map_json"),
        })

        # Параметры источника и TT собраны. Переходим к подтверждению.
        await state.set_state(UploadProcess.confirm_parameters)
        state_data = await state.get_data() # Получаем все данные состояния
        source_params = state_data.get('source_params', {})
        tt_params = state_data.get('tt_params', {})
        selected_source_type = state_data.get('selected_source_type', 'Неизвестно')
        # Строим сообщение для подтверждения
        confirm_text = build_confirmation_message(selected_source_type, source_params, tt_params)

        await callback.message.edit_text(
            f"Использована конфигурация True Tabs по умолчанию: <b>{default_config.get('name', 'Без имени')}</b>.\n"
            f"Все параметры собраны. Проверьте и нажмите 'Загрузить'.\n\n" + confirm_text,
            reply_markup=upload_confirm_keyboard(), # Клавиатура "Загрузить" / "Отмена"
            parse_mode='HTML'
        )

    else:
        logger.error(f"Дефолтная конфига TT не найдена или имя не совпадает при use_default_tt_config: {config_name}")
        await callback.message.edit_text(f"Ошибка: Дефолтная конфигурация True Tabs '{config_name}' не найдена.", reply_markup=main_menu_keyboard())
        await state.clear()

    await callback.answer()

# Хэндлер для callback_data="list_saved_tt_configs_for_selection" - Переход к списку сохраненных конфигов TT
@router.callback_query(F.data == "list_saved_tt_configs_for_selection", UploadProcess.choose_saved_tt_method)
async def list_all_tt_configs_for_selection(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор просмотра списка сохраненных конфигураций True Tabs."""
    text = "Выберите сохраненную конфигурацию True Tabs:"
    tt_configs = await sqlite_db.list_tt_configs() # Получаем список всех сохраненных конфигов TT

    if not tt_configs:
         # Если нет сохраненных конфигов, возвращаемся к выбору метода ввода
         await callback.message.edit_text("Сохраненных конфигураций True Tabs не найдено.", reply_markup=select_input_method_keyboard('tt'))
         await state.set_state(UploadProcess.select_tt_input_method)
    else:
        # Отображаем список сохраненных конфигов
        keyboard = select_config_keyboard(tt_configs, 'tt_select') # Используем select_config_keyboard с префиксом 'tt_select'
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
        # Переходим в состояние ожидания выбора из списка
        await state.set_state(UploadProcess.waiting_saved_tt_selection)

    await callback.answer()


# --- Хэндлер выбора сохраненной конфигурации True Tabs (из списка) ---
# Срабатывает в состоянии UploadProcess.waiting_saved_tt_selection
@router.callback_query(F.data.startswith("select_config:tt_select:"), UploadProcess.waiting_saved_tt_selection)
async def process_saved_tt_config_selection(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор сохраненной конфигурации True Tabs из списка."""
    config_name = callback.data.split(":")[2] # Имя выбранной конфигурации True Tabs
    saved_config = await sqlite_db.get_tt_config(config_name) # Получаем полные данные выбранной конфиги

    if saved_config:
        # Сохраняем параметры выбранной конфигурации True Tabs в данные состояния
        await state.update_data(tt_params={
            "upload_api_token": saved_config.get("upload_api_token"),
            "upload_datasheet_id": saved_config.get("upload_datasheet_id"),
            "upload_field_map_json": saved_config.get("upload_field_map_json"),
        })

        # Параметры источника и TT собраны. Переходим к подтверждению.
        await state.set_state(UploadProcess.confirm_parameters)
        state_data = await state.get_data() # Получаем все данные состояния
        source_params = state_data.get('source_params', {})
        tt_params = state_data.get('tt_params', {})
        selected_source_type = state_data.get('selected_source_type', 'Неизвестно')
        # Строим сообщение для подтверждения
        confirm_text = build_confirmation_message(selected_source_type, source_params, tt_params)


        await callback.message.edit_text(
            f"Использована конфигурация True Tabs: <b>{saved_config.get('name', 'Без имени')}</b>.\n"
            f"Все параметры собраны. Проверьте и нажмите 'Загрузить'.\n\n" + confirm_text,
            reply_markup=upload_confirm_keyboard(), # Клавиатура "Загрузить" / "Отмена"
            parse_mode='HTML'
        )

    else:
        logger.error(f"Выбранная сохраненная конфига TT '{config_name}' не найдена при selection.")
        await callback.message.edit_text(f"Ошибка: Конфигурация True Tabs '{config_name}' не найдена.", reply_markup=main_menu_keyboard())
        await state.clear()

    await callback.answer()


# --- Обработчик ручного ввода параметров True Tabs ---

# Хэндлер для состояния waiting_upload_token
@router.message(UploadProcess.waiting_upload_token)
async def process_upload_token(message: Message, state: FSMContext):
    """Обрабатывает ввод API токена True Tabs."""
    user_input = message.text.strip()
    # Добавлена базовая валидация токена (например, длина и допустимые символы)
    if not user_input or len(user_input) < 10 or not re.match(r'^[A-Za-z0-9\-_]+$', user_input):
        await message.answer("API токен True Tabs должен быть не менее 10 символов и содержать только буквы, цифры, дефисы или подчеркивания. Введите токен заново:", reply_markup=cancel_kb)
        return

    tt_params = (await state.get_data()).get('tt_params', {})
    tt_params['upload_api_token'] = user_input
    await state.update_data(tt_params=tt_params)

    # Переход к следующему параметру (datasheet_id)
    await state.set_state(UploadProcess.waiting_datasheet_id)
    await message.answer(f"Введите {get_friendly_param_name('upload_datasheet_id')}:", reply_markup=cancel_kb)

# Хэндлер для состояния waiting_datasheet_id
@router.message(UploadProcess.waiting_datasheet_id)
async def process_datasheet_id(message: Message, state: FSMContext):
    """Обрабатывает ввод ID таблицы True Tabs."""
    user_input = message.text.strip()
    # Добавлена базовая валидация ID таблицы (например, проверка на длину и допустимые символы)
    if not user_input or len(user_input) < 5 or not re.match(r'^[A-Za-z0-9\-_]+$', user_input):
        await message.answer("ID таблицы True Tabs должен быть не менее 5 символов и содержать только буквы, цифры, дефисы или подчеркивания. Введите ID заново:", reply_markup=cancel_kb)
        return

    tt_params = (await state.get_data()).get('tt_params', {})
    tt_params['upload_datasheet_id'] = user_input
    await state.update_data(tt_params=tt_params)

    # Переход к следующему параметру (field_map_json)
    await state.set_state(UploadProcess.waiting_field_map_json)
    await message.answer(
        f"Введите {get_friendly_param_name('upload_field_map_json')}\n(JSON строка сопоставления полей, например `{{\"SourceColumn\": \"TrueTabsField\", ...}}`. Может быть пустым.):",
        reply_markup=cancel_kb
    )

# Хэндлер для состояния waiting_field_map_json
@router.message(UploadProcess.waiting_field_map_json)
async def process_field_map_json(message: Message, state: FSMContext):
    """Обрабатывает ввод JSON сопоставления полей."""
    user_input = message.text.strip()

    # Валидация: если ввод не пустой, то должен быть валидным JSON
    if user_input:
        try:
            json.loads(user_input)
        except json.JSONDecodeError:
            await message.answer("Неверный формат JSON. Введите JSON строку сопоставления полей или отправьте '-' для пропуска:", reply_markup=cancel_kb)
            return
        except Exception as e:
             logger.error(f"Неожиданная ошибка валидации JSON сопоставления полей: {e}", exc_info=True)
             await message.answer("Произошла ошибка при валидации JSON. Введите JSON строку сопоставления полей или отправьте '-' для пропуска:", reply_markup=cancel_kb)
             return


    tt_params = (await state.get_data()).get('tt_params', {})
    tt_params['upload_field_map_json'] = user_input if user_input != '-' else None # Сохраняем None, если пользователь ввел '-'
    await state.update_data(tt_params=tt_params)


    # Ввод параметров True Tabs завершен.
    logger.info(f"Ввод параметров True Tabs завершен.")
    # Параметры источника и TT собраны. Переходим к подтверждению.
    await state.set_state(UploadProcess.confirm_parameters)
    state_data = await state.get_data()
    source_params = state_data.get('source_params', {})
    tt_params = state_data.get('tt_params', {})
    selected_source_type = state_data.get('selected_source_type', 'Неизвестно')
    # Строим сообщение для подтверждения
    confirm_text = build_confirmation_message(selected_source_type, source_params, tt_params)

    await message.answer(
        "Параметры True Tabs введены.\nВсе параметры собраны. Проверьте и нажмите 'Загрузить'.\n\n" + confirm_text,
        reply_markup=upload_confirm_keyboard(),
        parse_mode='HTML'
    )


# --- Вспомогательная функция для построения сообщения подтверждения перед запуском ---
def build_confirmation_message(source_type: str, source_params: Dict[str, Any], tt_params: Dict[str, Any]) -> str:
    """Формирует текст сообщения для подтверждения собранных параметров."""
    confirm_text = f"<b>Собранные параметры:</b>\n\n"
    confirm_text += f"Источник: <b>{source_type}</b>\n"

    # Отображаем параметры источника в определенном порядке
    source_param_order = SOURCE_PARAMS_ORDER.get(source_type, [])
    # Добавляем 'source_type' в начало, если его нет в порядке (для отображения)
    if 'source_type' not in source_param_order:
         source_param_order = ['source_type'] + [p for p in source_param_order if p != 'source_type']

    for key in source_param_order:
         # Получаем значение из source_params или из загруженной конфиги, если source_params неполные
         # В идеале, source_params всегда должен содержать полные данные после ручного ввода или загрузки конфиги.
         value = source_params.get(key)
         if value is not None: # Пропускаем параметры без значения
              friendly_key = get_friendly_param_name(key)
              # Скрываем чувствительные данные (пароли, токены)
              if key in ['source_pass', 'upload_api_token']:
                  confirm_text += f"  {friendly_key.capitalize()}: <code>***</code>\n"
          # Специальная обработка для пути к файлу (CSV)
          elif key == 'source_url' and source_type == 'csv':
               # Получаем имя файла из пути для более короткого отображения
               file_name = Path(value).name if isinstance(value, str) else value
               confirm_text += f"  {get_friendly_param_name('source_url_file').capitalize()}: <code>{file_name}</code>\n"
              # Специальная обработка для JSON (запросов, сопоставления полей)
              elif key in ['es_query', 'upload_field_map_json'] and isinstance(value, (str, dict)):
                  try:
                       # Пытаемся распарсить и красиво отформатировать JSON
                       value_to_dump = value if isinstance(value, dict) else json.loads(value)
                       # Используем ensure_ascii=False для поддержки кириллицы
                       query_display = json.dumps(value_to_dump, indent=2, ensure_ascii=False)
                       confirm_text += f"  {friendly_key.capitalize()}:\n<pre><code class=\"language-json\">{query_display}</code></pre>\n"
                  except:
                       # Если не удалось распарсить JSON, выводим как есть или сообщение об ошибке
                       confirm_text += f"  {friendly_key.capitalize()}: <code>Некорректный JSON или пусто</code>\n"
              # Специальная обработка для URL/URI
              elif key == 'source_url':
                   confirm_text += f"  {friendly_key.capitalize()}: <code>{value}</code>\n"
              # Остальные параметры отображаем как строку
              else:
                 confirm_text += f"  {friendly_key.capitalize()}: <code>{value}</code>\n"

    confirm_text += f"\n<b>Параметры True Tabs:</b>\n"
    # Отображаем параметры True Tabs в определенном порядке
    tt_param_order = ["upload_api_token", "upload_datasheet_id", "upload_field_map_json"]
    for key in tt_param_order:
         value = tt_params.get(key)
         if value is not None:
              friendly_key = get_friendly_param_name(key)
              # Скрываем токен
              if key == 'upload_api_token':
                  confirm_text += f"  {friendly_key.capitalize()}: <code>***</code>\n"
              # Специальная обработка для JSON сопоставления полей
              elif key == 'upload_field_map_json' and isinstance(value, (str, dict)):
                  try:
                       value_to_dump = value if isinstance(value, dict) else json.loads(value)
                       field_map_display = json.dumps(value_to_dump, indent=2, ensure_ascii=False)
                       # Используем более понятное название для отображения
                       confirm_text += f"  {get_friendly_param_name('upload_field_map_json_display').capitalize()}:\n<pre><code class=\"language-json\">{field_map_display}</code></pre>\n"
                  except:
                      confirm_text += f"  {friendly_key.capitalize()}: <code>Некорректный JSON или пусто</code>\n"
              # Остальные параметры TT (datasheet_id) отображаем как строку
              else:
                 confirm_text += f"  {friendly_key.capitalize()}: <code>{value}</code>\n"


    confirm_text += f"\nВсе верно? Нажмите 'Загрузить' для старта операции."
    return confirm_text


# --- Хэндлер подтверждения загрузки/выполнения операции ---
@router.callback_query(F.data == "confirm_upload", StateFilter(UploadProcess.confirm_parameters))
async def handle_confirm_upload(callback: CallbackQuery, state: FSMContext, bot: Bot):
    """
    Обрабатывает подтверждение запуска операции после сбора всех параметров.
    Формирует аргументы для Rust утилиты, запускает ее и обрабатывает результат.
    """
    # Удаляем клавиатуру подтверждения, чтобы избежать повторных нажатий
    await callback.message.edit_reply_markup(reply_markup=None)


    state_data = await state.get_data()
    source_type = state_data.get("selected_source_type", "unknown")
    source_params = state_data.get("source_params", {}) # Параметры источника
    tt_params = state_data.get("tt_params", {}) # Параметры True Tabs
    temp_upload_dir = state_data.get('temp_file_upload_dir') # Временная директория для загруженного файла


    # Определяем action ('extract', 'update') динамически, возможно из FSM state
    rust_action = "extract"
    if tt_params and tt_params.get("upload_api_token") and tt_params.get("upload_datasheet_id"):
        rust_action = "update"

    # Формируем путь для выходного файла XLSX (только для действия extract)
    output_filepath = None
    if rust_action == 'extract':
         output_filename = f"extract_result_{callback.from_user.id}_{source_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
         output_filepath = Path(config.TEMP_FILES_DIR) / output_filename


    # --- Формируем аргументы для Rust утилиты ---
    # Этот блок должен совпадать с логикой формирования аргументов в scheduled_handlers.py
    # Лучше вынести эту логику в отдельную функцию в utils или общем модуле
    rust_args = []
    rust_args.append("--action")
    rust_args.append(rust_action) # Действие для Rust

    rust_args.append("--source")
    rust_args.append(source_type) # Тип источника

    # Маппинг ключей параметров бота на аргументы Rust
    # Этот маппинг должен быть консистентным с Rust Args и Scheduled Handlers
    rust_arg_map = {
         'source_url': '--connection',
         'source_user': '--user', 'source_pass': '--pass',
         'source_query': '--query',
         'db_name': '--db-name', 'collection_name': '--collection', # Для MongoDB
         'key_pattern': '--key-pattern', # Для Redis
         'org': '--org', 'bucket': '--bucket', 'index': '--index', # Для Elasticsearch (или других)
         'es_query': '--query', # Для Elasticsearch
         'redis_pattern': '--key-pattern', # Для Redis
         'mongo_db': '--db-name', # Для MongoDB
         'mongo_collection': '--collection', # Для MongoDB
         'specific_params': '--specific-params-json', # Для других специфических параметров
    }
    # Добавляем маппинг для аргументов действия 'update'
    if rust_action == 'update':
        tt_arg_map = {
            'upload_api_token': '--api-token',
            'upload_datasheet_id': '--datasheet-id',
            'upload_field_map_json': '--field-map-json',
            # Добавлены дополнительные аргументы для update, если нужно
            'record_id': '--record-id',
            'field_updates_json': '--field-updates-json',
        }
        for key, value in tt_params.items():
            if value is None or value == "":
                continue
            rust_arg_name = tt_arg_map.get(key)
            if rust_arg_name:
                rust_args.append(rust_arg_name)
                rust_args.append(str(value))

    # Добавляем параметры источника в аргументы Rust
    # Используем собранные source_params
    for key, value in source_params.items():
        # Пропускаем None, пустые строки, ключи, которые не маппятся, или внутренние ключи
        if value is None or value == "" or key not in rust_arg_map or key in ['id', 'name', 'source_type', 'is_default']:
            continue

        rust_arg_name = rust_arg_map[key]

        # Специальная обработка для JSON параметров
        if key in ['es_query', 'specific_params']:
            if isinstance(value, dict): value_to_dump = value
            elif isinstance(value, str):
                 try: value_to_dump = json.loads(value)
                 except json.JSONDecodeError:
                      logger.error(f"Неверный JSON string для параметра {key} при формировании args Rust в handle_confirm_upload.")
                      await callback.message.edit_text(f"Ошибка: Неверный формат JSON параметра '{get_friendly_param_name(key)}'. Отмена операции.", reply_markup=main_menu_keyboard())
                      await state.clear()
                      await callback.answer()
                      return
            else:
                 logger.error(f"Неожиданный тип ({type(value)}) для параметра {key} при формировании args Rust.")
                 await callback.message.edit_text(f"Ошибка: Неожиданный тип данных для параметра '{get_friendly_param_name(key)}'. Отмена операции.", reply_markup=main_menu_keyboard())
                 await state.clear()
                 await callback.answer()
                 return

            rust_args.append(rust_arg_name)
            rust_args.append(json.dumps(value_to_dump)) # Передаем как JSON строку

        else:
            # Остальные параметры передаем как строки
            rust_args.append(rust_arg_name)
            rust_args.append(str(value))

    # Добавляем путь выходного файла, если действие extract
    if output_filepath:
         rust_args.append("--output-xlsx-path")
         rust_args.append(str(output_filepath))

    # Обработка --expected-headers, если они есть в source_params
    expected_headers = source_params.get('upload_expected_headers')
    if expected_headers:
        try:
            # Если expected_headers - строка, пытаемся распарсить как JSON
            if isinstance(expected_headers, str):
                expected_headers_json = json.loads(expected_headers)
            else:
                expected_headers_json = expected_headers
            rust_args.append("--expected-headers")
            rust_args.append(json.dumps(expected_headers_json))
        except Exception as e:
            logger.error(f"Ошибка при обработке expected_headers: {e}")
            await callback.message.edit_text("Ошибка: Неверный формат ожидаемых заголовков. Отмена операции.", reply_markup=main_menu_keyboard())
            await state.clear()
            await callback.answer()
            return

    # Переводим FSM в состояние "операция в процессе"
    await state.set_state(UploadProcess.operation_in_progress)

    # Отправляем пользователю сообщение о запуске операции
    starting_message = await callback.message.edit_text(
        "🚀 Операция запущена...",
        reply_markup=operation_in_progress_keyboard() # Отображаем клавиатуру "Отмена операции"
    )
    await callback.answer("Запускаю операцию...")

    # Запускаем выполнение Rust утилиты в отдельной задаче, чтобы не блокировать хэндлер
    # Передаем все необходимые данные и экземпляр бота в новую задачу
    asyncio.create_task(process_upload_task(
        bot, # Экземпляр бота для отправки сообщений
        callback.message.chat.id, # ID чата пользователя
        rust_args, # Аргументы для Rust утилиты
        source_type, # Тип источника (для логгирования и истории)
        tt_params.get("upload_datasheet_id", "N/A"), # Datasheet ID (для истории)
        str(output_filepath) if output_filepath else None, # Путь к выходному файлу (для сохранения в истории и отправки)
        temp_upload_dir, # Временная директория для очистки (если был загружен файл)
        starting_message, # Сообщение, которое нужно будет редактировать (статус/результат)
        state, # Состояние FSM (для сброса в конце)
    ))




# --- Вспомогательная функция для выполнения Rust задачи и обработки результата ---
# Эта функция выполняется в отдельной задаче, не блокируя основной цикл бота
async def process_upload_task(
    bot: Bot, chat_id: int, rust_args: list, source_type: str, datasheet_id: str,
    output_filepath: Optional[str], temp_upload_dir: Optional[str],
    status_message: Message, state: FSMContext): # Принимаем сообщение и состояние FSM

    process = None
    communicate_future = None
    execution_info = None
    final_status = "ERROR" # Изначально считаем ошибкой
    duration = 0.0 # Длительность выполнения
    extracted_rows = None # Количество извлеченных строк (из результата Rust)
    uploaded_records = None # Количество загруженных записей (из результата Rust)
    datasheet_id_from_result = datasheet_id # Сохраняем ID таблицы из параметров или получаем из результата Rust
    final_generated_file_path = None # Путь к файлу, если успешно создан Rust утилитой
    error_message = "Произошла неизвестная ошибка выполнения Rust утилиты." # Сообщение об ошибке или успехе
    start_time = time.time() # Время начала выполнения операции

    try:
        # Информируем пользователя о запуске с конкретными аргументами (опционально, для дебага)
        # Можно отправить отдельное сообщение с аргументами или добавить в статусное.
        # await bot.send_message(chat_id, f"Debug: Запускаю Rust с аргументами: `{rust_args}`", parse_mode='MarkdownV2')

        # Обновляем статусное сообщение
        await status_message.edit_text("⚙️ Выполняю Rust утилиту...", reply_markup=operation_in_progress_keyboard())

        # Выполняем Rust команду. execute_rust_command не блокирует, возвращает процесс и future для ожидания.
        execution_info = await execute_rust_command(rust_args)
        # Время завершения выполнения execute_rust_command (может быть запуском или ошибкой запуска)
        end_time_launch = time.time()


        if execution_info["status"] == "ERROR":
            # Обработка ошибки, если процесс Rust не удалось ЗАПУСТИТЬ (например, неверный путь к exe, нет прав)
            final_status = "ERROR"
            error_message = execution_info.get("message", "Ошибка при запуске процесса Rust.")
            duration = execution_info.get("duration_seconds", end_time_launch - start_time) # Время до ошибки запуска
            logger.error(f"Ошибка запуска Rust процесса для chat {chat_id}: {error_message}", exc_info=True)
            # Нет process или future для ожидания, переходим сразу в блок finally

        else:
            # Процесс успешно запущен, получаем его данные
            process = execution_info["process"]
            communicate_future = execution_info["communicate_future"]
            start_time = execution_info["start_time"] # Используем точное время старта процесса

            try:
                # Ожидаем завершения процесса и получения его вывода (stdout и stderr)
                stdout_data, stderr_data = await communicate_future
                end_time_execution = time.time() # Время завершения работы Rust процесса
                duration = end_time_execution - start_time # Общее время выполнения Rust процесса

                # Декодируем вывод
                stdout_str = stdout_data.decode('utf-8', errors='ignore')
                stderr_str = stderr_data.decode('utf-8', errors='ignore')

                # Логгируем вывод Rust утилиты
                logger.info(f"Rust stdout (PID {process.pid}) для chat {chat_id}:\n{stdout_str}")
                logger.error(f"Rust stderr (PID {process.pid}) для chat {chat_id}:\n{stderr_str}") # stderr часто содержит ошибки, логгируем как ERROR
                logger.info(f"Rust процесс PID {process.pid} для chat {chat_id} завершен с кодом: {process.returncode}")


                # Попытка парсить JSON выход от Rust утилиты (предполагаем, что Rust выводит результат в JSON в stdout)
                try:
                    json_result: Dict[str, Any] = json.loads(stdout_str)
                    # Извлекаем ожидаемые поля из JSON результата Rust
                    final_status = json_result.get("status", "ERROR") # Статус из JSON ('SUCCESS', 'ERROR')
                    error_message = json_result.get("message", "Сообщение от утилиты отсутствует.") # Сообщение от утилиты
                    # duration уже рассчитана выше
                    extracted_rows = json_result.get("extracted_rows") # Количество извлеченных строк
                    uploaded_records = json_result.get("uploaded_records") # Количество загруженных записей
                    datasheet_id_from_result = json_result.get("datasheet_id", datasheet_id_from_result) # ID таблицы из результата (если есть)
                    final_generated_file_path = json_result.get("file_path") # Путь к файлу, если успешно создан (для extract)

                    # Если статус SUCCESS из JSON, но сообщение отсутствует, используем дефолтное
                    if final_status == "SUCCESS" and (error_message == "Сообщение от утилиты отсутствует." or error_message == ""):
                         error_message = "Операция выполнена успешно."


                except json.JSONDecodeError:
                    # Ошибка, если stdout не является валидным JSON
                    final_status = "ERROR"
                    error_message = f"Rust процесс завершился с кодом {process.returncode}, но stdout не является валидным JSON. Stderr:\n{stderr_str}\nStdout:\n{stdout_str}"
                    logger.error(f"Ошибка парсинга JSON stdout от Rust для chat {chat_id}: {error_message}", exc_info=True)
                except Exception as e:
                     # Другие ошибки при обработке JSON результата
                     final_status = "ERROR"
                     error_message = f"Ошибка при обработке JSON результата Rust: {e}. Stderr:\n{stderr_str}\nStdout:\n{stdout_str}"
                     logger.error(f"Ошибка обработки JSON результата Rust для chat {chat_id}: {e}", exc_info=True)

                # Если статус из JSON не SUCCESS, и код завершения процесса не 0, убедимся, что статус ERROR
                if final_status != "SUCCESS" and process.returncode != 0:
                    if error_message == "Сообщение от утилиты отсутствует." or \
                       error_message.startswith("Rust процесс завершился с кодом"):
                           # Если сообщение об ошибке не было установлено из JSON, используем сообщение о коде завершения и stderr
                           error_message = f"Rust процесс завершился с ошибкой (код {process.returncode}). Stderr:\n{stderr_str}\nStdout:\n{stdout_str}"
                    final_status = "ERROR" # Подтверждаем статус ошибки

            except asyncio.CancelledError:
                # Перехват отмены задачи (например, если пользователь нажал "Отмена операции")
                logger.info(f"Задача Communicate cancelled for PID {process.pid} for chat {chat_id}")
                final_status = "CANCELLED" # Статус "Отменено"
                error_message = "Операция была отменена пользователем." # Сообщение об отмене
                duration = time.time() - start_time # Рассчитываем длительность до отмены
    # Здесь нужно попытаться корректно завершить запущенный процесс Rust,
    # отправив ему сигнал (например, SIGTERM), чтобы он завершился чисто.
    # process.terminate() или process.kill() могут помочь.

    # В handle_confirm_upload при запуске процесса добавить запись в running_processes
    # В handle_cancel_operation вызвать terminate_process(chat_id)

            except Exception as e:
                # Перехват других неожиданных ошибок во время ожидания communicate()
                final_status = "ERROR"
                error_message = f"Произошла внутренняя ошибка во время выполнения Rust процесса: {e}"
                duration = time.time() - start_time
                logger.error(f"Unexpected error during Rust process execution for chat {chat_id}: {e}", exc_info=True)

    except Exception as e:
        # Перехват ошибок, которые могли произойти до communicate() (например, при запуске execute_rust_command)
        final_status = "ERROR"
        error_message = f"Произошла внутренняя ошибка при запуске или выполнении операции: {e}"
        duration = time.time() - start_time
        logger.error(f"Unexpected error in process_upload_task (outer) for chat {chat_id}: {e}", exc_info=True)

    finally:
        logger.info(f"Операция завершена для chat {chat_id} со статусом: {final_status}")
        # Очищаем временную директорию, если она была создана для загруженного файла
        if temp_upload_dir and os.path.exists(temp_upload_dir):
            try:
                shutil.rmtree(temp_upload_dir)
                logger.info(f"Временная директория {temp_upload_dir} очищена.")
            except Exception as cleanup_e:
                logger.error(f"Ошибка очистки временной директории {temp_upload_dir}: {cleanup_e}")

        # Добавляем запись в историю операций с финальным статусом
        try:
             await sqlite_db.add_upload_record(
                 source_type=source_type, # Тип источника
                 status=final_status, # Финальный статус ('SUCCESS', 'ERROR', 'CANCELLED')
                 # Путь к файлу сохраняем только если операция успешна и файл был создан
                 file_path=final_generated_file_path if final_status == "SUCCESS" and final_generated_file_path else None,
                 error_message=error_message, # Сообщение об ошибке или успехе
                 true_tabs_datasheet_id=datasheet_id_from_result, # ID таблицы TT
                 duration_seconds=duration # Длительность выполнения
             )
             logger.info(f"Запись истории добавлена для chat {chat_id} со статусом: {final_status}")
        except Exception as e:
             logger.error(f"Ошибка при добавлении записи истории для chat {chat_id}: {e}", exc_info=True)
             # Если не удалось сохранить в историю, пытаемся уведомить пользователя
             try:
                 await bot.send_message(chat_id, f"⚠️ Операция завершена со статусом '{final_status}', но произошла ошибка при сохранении в историю: {e}", parse_mode='HTML')
             except Exception as send_e:
                  logger.error(f"Ошибка при отправке сообщения об ошибке сохранения истории: {send_e}")


        # --- Отправляем финальное сообщение пользователю ---
        try:
            final_message_text = f"✅ <b>Операция успешно завершена!</b>\n" if final_status == "SUCCESS" else \
                                 f"⚠️ <b>Операция отменена.</b>\n" if final_status == "CANCELLED" else \
                                 f"❌ <b>Операция завершилась с ошибкой!</b>\n"

            # Добавляем информацию об источнике и TT
            final_message_text += f"Источник: <code>{source_type}</code>\n"
            if datasheet_id_from_result and datasheet_id_from_result != 'N/A':
                final_message_text += f"Datasheet ID: <code>{datasheet_id_from_result}</code>\n"

            if final_status == "SUCCESS":
                # Добавляем детали извлечения/загрузки при успехе
                if extracted_rows is not None:
                    final_message_text += f"Извлечено строк: {extracted_rows}\n"
                    if uploaded_records is not None:
                        final_message_text += f"Загружено записей: {uploaded_records}\n"
                    final_message_text += f"Время выполнения: {duration:.2f} секунд\n"
                    # Указываем путь к файлу, если он был создан
                    if final_generated_file_path and os.path.exists(final_generated_file_path):
                        final_message_text += f"Файл результата сохранен на сервере бота: <code>{final_generated_file_path}</code>"
                    # Включаем сообщение от утилиты, если оно есть и не стандартное сообщение об успехе
                    if error_message != "Сообщение от утилиты отсутствует." and error_message != "Операция выполнена успешно.":
                        final_message_text += f"\n<i>Сообщение от утилиты:</i> {error_message}"

            # Отправляем файл результата, если он был создан и операция успешна
            if final_generated_file_path and os.path.exists(final_generated_file_path):
                try:
                    # Отправляем файл как документ
                    await bot.send_document(chat_id, document=FSInputFile(final_generated_file_path, filename=os.path.basename(final_generated_file_path)), caption="Файл результата:")
                    # Рассмотреть удаление файла после отправки, чтобы не засорять TEMP_FILES_DIR
                    # shutil.rmtree(Path(final_generated_file_path).parent) # Удаление временной директории

                    # Добавим удаление временных файлов после отправки результата
                    if final_generated_file_path and Path(final_generated_file_path).exists():
                        try:
                            shutil.rmtree(Path(final_generated_file_path).parent)
                            logger.info(f"Временные файлы удалены: {final_generated_file_path}")
                        except Exception as e:
                            logger.error(f"Ошибка при удалении временных файлов {final_generated_file_path}: {e}")

                except TelegramAPIError as e:
                    logger.error(f"Telegram API error sending result file to chat {chat_id}: {e}", exc_info=True)
                    # Если не удалось отправить файл, отправляем отдельное сообщение об этом
                    await bot.send_message(chat_id, f"❌ Ошибка при отправке файла результата: {e}")
                except Exception as e:
                    logger.error(f"Error sending result file to chat {chat_id}: {e}", exc_info=True)
                    await bot.send_message(chat_id, f"❌ Произошла внутренняя ошибка при отправке файла результата: {e}")


            elif final_status == "CANCELLED":
                # Детали для отмененной операции
                final_message_text += f"Время до отмены: {duration:.2f} секунд\n"
                final_message_text += f"Причина: {error_message}"

            else: # Status is ERROR
                # Детали для операции с ошибкой
                if extracted_rows is not None:
                    final_message_text += f"Извлечено строк (до ошибки): {extracted_rows}\n"
                if uploaded_records is not None:
                    final_message_text += f"Загружено записей (до ошибки): {uploaded_records}\n"
                final_message_text += f"Время выполнения: {duration:.2f} секунд\n\n"

                # Включаем сообщение об ошибке от утилиты или обработчика
                # Здесь можно добавить логику для улучшенного форматирования ошибок, если необходимо
                final_message_text += f"Сообщение об ошибке:\n<pre><code>{error_message}</code></pre>"

                # Файл результата при ошибке не отправляется, если он был создан, он, вероятно, неполный/некорректный


            # Отправляем финальное сообщение о статусе выполнения операции
            # Редактируем исходное статусное сообщение
            try:
                await status_message.edit_text(final_message_text, reply_markup=main_menu_keyboard(), parse_mode='HTML')
            except Exception as e:
                logger.error(f"Ошибка редактирования финального статус сообщения для chat {chat_id}: {e}", exc_info=True)
                # Если не удалось отредактировать, отправляем новое сообщение
                try:
                    await bot.send_message(chat_id, final_message_text, reply_markup=main_menu_keyboard(), parse_mode='HTML')
                except Exception as send_e:
                    logger.error(f"Ошибка отправки нового финального сообщения для chat {chat_id}: {send_e}")


        except Exception as e:
            logger.error(f"Критическая ошибка в блоке finally process_upload_task для chat {chat_id}: {e}", exc_info=True)
            # Если даже здесь произошла ошибка, пытаемся отправить самое простое сообщение
            try:
                await bot.send_message(chat_id, "Произошла критическая ошибка после выполнения операции.", reply_markup=main_menu_keyboard())
            except Exception as send_e:
                logger.error(f"Последняя попытка отправить сообщение chat {chat_id} не удалась: {send_e}")


        # Очищаем состояние FSM в конце операции
        try:
            await state.clear()
            logger.info(f"FSM state очищен для chat {chat_id}")
        except Exception as e:
            logger.error(f"Ошибка очистки FSM state для chat {chat_id}: {e}", exc_info=True)

# Хэндлер для кнопки "Отмена операции" во время выполнения
@router.callback_query(F.data == "cancel_operation", StateFilter(UploadProcess.operation_in_progress))
async def handle_cancel_operation(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает запрос пользователя на отмену текущей операции."""
    logger.info(f"Пользователь {callback.from_user.id} запросил отмену операции.")

    # Удаляем кнопку отмены, чтобы избежать повторных нажатий
    await callback.message.edit_reply_markup(reply_markup=None)

    # Отменяем текущее FSM состояние (это вызовет asyncio.CancelledError в ожидающих задачах, например, communicate_future)
    await state.clear()

    await callback.message.edit_text("⚠️ Запрос на отмену операции отправлен. Ожидайте завершения процесса...")
    await callback.answer("Запрос на отмену отправлен.")

    # Реализуем логику отправки сигнала на завершение запущенному процессу Rust
    await terminate_process(callback.from_user.id)

