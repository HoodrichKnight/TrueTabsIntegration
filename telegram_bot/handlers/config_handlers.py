import os
import asyncio
import json
import sys
import time
from datetime import datetime
from typing import Dict, Any, Optional, List # Импортируем List
from pathlib import Path
import validators
import re
import tempfile
import shutil


from aiogram import Router, F, Bot
from aiogram.types import Document
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import Message, CallbackQuery, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder # Импортируем InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import StateFilter
from ..keyboards import (
    main_menu_keyboard,
    source_selection_keyboard,
    upload_confirm_keyboard,
    select_input_method_keyboard,
    select_config_keyboard,
    operation_in_progress_keyboard
)
from ..utils.rust_executor import execute_rust_command
from ..database import sqlite_db
from .. import config
from ..database.sqlite_db import ( # Импортируем все необходимые функции БД, включая get_default_*
    add_source_config, get_source_config, list_source_configs, delete_source_config, update_source_config,
    add_tt_config, get_tt_config, list_tt_configs, delete_tt_config, update_tt_config,
    set_default_source_config, get_default_source_config, set_default_tt_config, get_default_tt_config
)


# ... (Остальная часть файла: REQUIRED_COLUMN_NAMES, PARAM_NAMES_FRIENDLY, get_friendly_param_name, cancel_kb) ...

router = Router()

# Класс состояний для процесса загрузки/извлечения
class UploadProcess(StatesGroup):
    select_source = State()
    select_source_input_method = State()
    # select_saved_source_config = State() # Больше не нужно

    # НОВЫЕ СОСТОЯНИЯ для выбора метода сохранения после выбора "Использовать сохраненную"
    choose_saved_source_method = State()
    choose_saved_tt_method = State()


    waiting_saved_source_selection = State() # Ожидание выбора из списка сохраненных источников
    waiting_saved_tt_selection = State() # Ожидание выбора из списка сохраненных TT


    waiting_pg_url = State()
    waiting_pg_user = State()
    waiting_pg_pass = State()
    waiting_pg_query = State()

    waiting_mysql_url = State()
    waiting_mysql_user = State()
    waiting_mysql_pass = State()
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

    waiting_file_upload = State()

    # TODO: Добавить состояния для Labguru и других источников

    select_tt_input_method = State()
    # select_saved_tt_config = State() # Больше не нужно


    waiting_upload_token = State()
    waiting_datasheet_id = State()
    waiting_field_map_json = State()

    confirm_parameters = State()
    operation_in_progress = State()


# ... (Остальная часть файла: SOURCE_PARAMS_ORDER, DISABLED_SOURCES, is_valid_url, is_valid_json) ...

# --- Хэндлер выбора источника данных (без изменений) ---
@router.callback_query(F.data == "select_source", ~StateFilter(ConfigProcess.waiting_config_name, ConfigProcess.waiting_source_config_type, ConfigProcess.waiting_source_param, ConfigProcess.waiting_tt_param)) # Исключаем состояния FSM добавления/редактирования конфигов
async def select_source_handler(callback: CallbackQuery, state: FSMContext):
     await state.set_state(UploadProcess.select_source)
     await callback.message.edit_text("Выберите источник данных:", reply_markup=source_selection_keyboard())
     await callback.answer()


# --- Хэндлер начала процесса загрузки по типу источника (без изменений) ---
@router.callback_query(F.data.startswith("start_upload_process:"))
async def start_upload_process(callback: CallbackQuery, state: FSMContext):
    source_type = callback.data.split(":")[1]

    if source_type in DISABLED_SOURCES:
        await callback.message.edit_text(f"Источник данных '{source_type}' временно недоступен.")
        await callback.answer()
        return

    await state.update_data(selected_source_type=source_type, source_params={})

    # Переходим сразу к выбору метода ввода параметров для источника
    await state.set_state(UploadProcess.select_source_input_method)
    await callback.message.edit_text(
        f"Выбран источник: <b>{source_type.capitalize()}</b>.\nВыберите способ ввода параметров источника:",
        reply_markup=select_input_method_keyboard('source'), # передаем 'source' для callback_data
        parse_mode='HTML'
    )
    await callback.answer()


# --- Хэндлер выбора метода ввода параметров источника (модифицирован) ---
# Обрабатывает select_input_method:manual:source и select_input_method:saved:source
@router.callback_query(F.data.startswith("select_input_method:"), UploadProcess.select_source_input_method)
async def select_source_input_method(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    method = parts[1] # 'manual' или 'saved'
    config_type_from_callback = parts[2] # 'source' или 'tt'

    state_data = await state.get_data()
    source_type = state_data.get("selected_source_type") # Тип источника уже должен быть в state

    if config_type_from_callback != 'source' or not source_type:
         # Проверка на всякий случай, если callback_data не соответствует логике
         await callback.message.edit_text("Ошибка в данных запроса. Начните заново.", reply_markup=main_menu_keyboard())
         await state.clear()
         await callback.answer()
         return


    if method == 'manual':
        # Логика для ручного ввода параметров источника остается прежней
        params_order = SOURCE_PARAMS_ORDER.get(source_type, [])
        params_order = [p for p in params_order if p != "source_type"]

        if not params_order:
            await state.update_data(source_params={})
            await state.set_state(UploadProcess.select_tt_input_method)
            await callback.message.edit_text(f"Параметры источника для <b>{source_type.capitalize()}</b> не требуются.\nВыберите способ ввода параметров True Tabs:",
                                             reply_markup=select_input_method_keyboard('tt'),
                                             parse_mode='HTML')
            await callback.answer()
            return

        first_param_key = params_order[0]
        initial_state = None

        if source_type == 'postgres': initial_state = UploadProcess.waiting_pg_url
        elif source_type == 'mysql': initial_state = UploadProcess.waiting_mysql_url
        elif source_type == 'sqlite': initial_state = UploadProcess.waiting_sqlite_url
        elif source_type == 'redis': initial_state = UploadProcess.waiting_redis_url
        elif source_type == 'mongodb': initial_state = UploadProcess.waiting_mongodb_uri
        elif source_type == 'elasticsearch': initial_state = UploadProcess.waiting_elasticsearch_url
        elif source_type in ['csv', 'excel']: initial_state = UploadProcess.waiting_file_upload
        # TODO: Добавить другие типы источников и их начальные состояния

        if initial_state:
            await state.update_data(param_keys_order=params_order, current_param_index=0)
            await state.set_state(initial_state)
            friendly_name = get_friendly_param_name(first_param_key)
            await callback.message.edit_text(f"Введите {friendly_name}:", reply_markup=cancel_kb)
        else:
             await callback.message.edit_text(f"Неизвестный тип источника '{source_type}'. Начните заново.", reply_markup=main_menu_keyboard())
             await state.clear()

        await callback.answer()

    elif method == 'saved':
        # --- ЛОГИКА ДЛЯ "ИСПОЛЬЗОВАТЬ СОХРАНЕННУЮ" ---
        # Сначала проверяем наличие дефолтной конфигурации для этого типа источника
        default_config = await sqlite_db.get_default_source_config(source_type)

        if default_config:
            # Если дефолтная конфигурация найдена, предлагаем выбор: использовать дефолтную или выбрать из списка
            builder = InlineKeyboardBuilder()
            builder.row(
                # Кнопка для использования дефолтной конфигурации
                InlineKeyboardButton(text=f"🚀 Использовать по умолчанию: {default_config.get('name', 'Без имени')}", callback_data=f"use_default_source_config:{default_config.get('name', 'N/A')}") # Используем имя дефолтного конфига
            )
            builder.row(
                # Кнопка для просмотра списка всех сохраненных конфигураций
                InlineKeyboardButton(text="📋 Выбрать из списка", callback_data="list_saved_source_configs_for_selection") # Новый callback
            )
            builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")) # Кнопка отмены
            keyboard = builder.as_markup()

            text = f"Найдена конфигурация по умолчанию для источника <b>{source_type.capitalize()}</b>.\nВыберите действие:"

            await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
            # Переходим в НОВОЕ состояние ожидания выбора способа использования сохраненной конфиги
            await state.set_state(UploadProcess.choose_saved_source_method)

        else:
            # Если дефолтная конфигурация не найдена, сразу переходим к отображению списка всех сохраненных
            text = "Дефолтная конфигурация для этого источника не найдена.\nВыберите сохраненную конфигурацию источника из списка:"
            source_configs = await sqlite_db.list_source_configs() # Получаем список всех сохраненных конфигов

            if not source_configs:
                await callback.message.edit_text("Сохраненных конфигураций источников не найдено. Пожалуйста, выберите ручной ввод.", reply_markup=select_input_method_keyboard('source'))
                await state.set_state(UploadProcess.select_source_input_method) # Остаемся в этом же состоянии
            else:
                # Отображаем список всех сохраненных конфигов для выбора
                keyboard = select_config_keyboard(source_configs, 'source_select')
                await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
                # Переходим в состояние ожидания выбора из списка
                await state.set_state(UploadProcess.waiting_saved_source_selection) # Используем существующее состояние


        await callback.answer()

    else:
        await callback.message.edit_text("Неверный выбор метода ввода.", reply_markup=main_menu_keyboard())
        await state.clear()
        await callback.answer()


# --- НОВЫЙ ХЭНДЛЕР: Выбор способа использования сохраненной конфигурации источника (дефолт или список) ---
@router.callback_query(F.data.startswith("use_default_source_config:"), UploadProcess.choose_saved_source_method)
async def use_default_source_config_handler(callback: CallbackQuery, state: FSMContext):
    # Этот хэндлер срабатывает при нажатии "🚀 Использовать по умолчанию"
    config_name = callback.data.split(":")[1]
    # Получаем полные данные дефолтной конфигурации по имени
    default_config = await sqlite_db.get_source_config(config_name)

    if default_config:
        # Загружаем параметры дефолтной конфигурации источника в данные состояния
        await state.update_data(selected_source_type=default_config.get('source_type'), source_params=default_config)

        # Переходим к следующему этапу - выбор метода ввода параметров True Tabs
        await state.set_state(UploadProcess.select_tt_input_method)
        await callback.message.edit_text(f"Использована конфигурация источника по умолчанию: <b>{default_config.get('name', 'Без имени')}</b> ({default_config.get('source_type')}).\n"
                                         f"Выберите способ ввода параметров True Tabs:",
                                         reply_markup=select_input_method_keyboard('tt'),
                                         parse_mode='HTML')

    else:
        # Если конфигурация не найдена (удалена после отображения кнопки?), сообщаем об ошибке
        await callback.message.edit_text(f"Ошибка: Дефолтная конфигурация источника '{config_name}' не найдена.", reply_markup=main_menu_keyboard())
        await state.clear()

    await callback.answer()


@router.callback_query(F.data == "list_saved_source_configs_for_selection", UploadProcess.choose_saved_source_method)
async def list_all_source_configs_for_selection(callback: CallbackQuery, state: FSMContext):
    # Этот хэндлер срабатывает при нажатии "📋 Выбрать из списка"
    text = "Выберите сохраненную конфигурацию источника:"
    source_configs = await sqlite_db.list_source_configs() # Получаем список всех сохраненных конфигов

    if not source_configs:
         # Если вдруг список пуст (удалены?), сообщаем об этом
         await callback.message.edit_text("Сохраненных конфигураций источников не найдено.", reply_markup=select_input_method_keyboard('source'))
         await state.set_state(UploadProcess.select_source_input_method) # Возвращаемся к выбору метода ввода
    else:
        # Отображаем список для выбора
        keyboard = select_config_keyboard(source_configs, 'source_select')
        await callback.message.edit_text(text, reply_markup=keyboard)
        # Переходим в состояние ожидания выбора из списка
        await state.set_state(UploadProcess.waiting_saved_source_selection) # Используем существующее состояние

    await callback.answer()


# --- Хэндлер выбора сохраненной конфигурации источника (из списка) ---
# Срабатывает в состоянии UploadProcess.waiting_saved_source_selection
@router.callback_query(F.data.startswith("select_config:source_select:"), UploadProcess.waiting_saved_source_selection)
async def process_saved_source_config_selection(callback: CallbackQuery, state: FSMContext):
    config_name = callback.data.split(":")[2]
    saved_config = await sqlite_db.get_source_config(config_name)

    if saved_config:
        await state.update_data(selected_source_type=saved_config.get('source_type'), source_params=saved_config)

        await state.set_state(UploadProcess.select_tt_input_method)
        await callback.message.edit_text(f"Использована конфигурация источника: <b>{saved_config.get('name', 'Без имени')}</b> ({saved_config.get('source_type')}).\n"
                                         f"Выберите способ ввода параметров True Tabs:",
                                         reply_markup=select_input_method_keyboard('tt'),
                                         parse_mode='HTML')

    else:
        await callback.message.edit_text(f"Ошибка: Конфигурация источника '{config_name}' не найдена.", reply_markup=main_menu_keyboard())
        await state.clear()

    await callback.answer()

# --- Обработчик загруженного файла (без изменений) ---
@router.message(F.document, UploadProcess.waiting_file_upload)
async def process_uploaded_file(message: Message, state: FSMContext, bot: Bot):
    await message.answer("Получен файл, обрабатываю...")

    state_data = await state.get_data()
    source_type = state_data['selected_source_type']
    original_file_name = message.document.file_name

    allowed_extensions = {
        'csv': ['.csv'],
        'excel': ['.xlsx', '.xls'],
    }
    expected_extensions = allowed_extensions.get(source_type, [])
    file_extension = Path(original_file_name).suffix.lower()

    if not expected_extensions or file_extension not in expected_extensions:
         friendly_name = get_friendly_param_name(source_type)
         await message.answer(f"Ошибка: Ожидался файл формата {', '.join(expected_extensions)} для источника '{friendly_name}'. Пожалуйста, отправьте корректный файл.")
         return

    temp_dir = None
    temp_file_path = None

    try:
        temp_dir = tempfile.mkdtemp(prefix=f"tt_upload_{message.from_user.id}_")
        temp_file_path = Path(temp_dir) / original_file_name

        file_info = await bot.get_file(message.document.file_id)
        await bot.download_file(file_info.file_path, temp_file_path)

        print(f"Файл скачан во временный путь: {temp_file_path}")

        await state.update_data(
            source_params={'source_url': str(temp_file_path)},
            temp_file_upload_dir=temp_dir
        )

        await state.set_state(UploadProcess.select_tt_input_method)

        await message.answer(f"Файл '{original_file_name}' успешно загружен.\nВыберите способ ввода параметров True Tabs:",
                             reply_markup=select_input_method_keyboard('tt'))


    except TelegramBadRequest as e:
        print(f"Telegram API error downloading file: {e}", file=sys.stderr)
        if temp_dir and os.path.exists(temp_dir):
             try: shutil.rmtree(temp_dir)
             except Exception as cleanup_e: print(f"Ошибка очистки temp dir {temp_dir} после ошибки скачивания: {cleanup_e}", file=sys.stderr)

        await message.answer("Произошла ошибка при скачивании файла из Telegram. Попробуйте еще раз.")
        await state.clear()
    except Exception as e:
        print(f"Error processing uploaded file: {e}", file=sys.stderr)
        if temp_dir and os.path.exists(temp_dir):
             try: shutil.rmtree(temp_dir)
             except Exception as cleanup_e: print(f"Ошибка очистки temp dir {temp_dir} после внутренней ошибки: {cleanup_e}", file=sys.stderr)
        await message.answer("Произошла внутренняя ошибка при обработке файла.")
        await state.clear()


# ... (Остальные хэндлеры ручного ввода параметров источника - без изменений) ...

# --- Хэндлер выбора метода ввода параметров True Tabs (модифицирован) ---
# Обрабатывает select_input_method:manual:tt и select_input_method:saved:tt
@router.callback_query(F.data.startswith("select_input_method:"), UploadProcess.select_tt_input_method)
async def select_tt_input_method(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    method = parts[1] # 'manual' или 'saved'
    config_type_from_callback = parts[2] # 'source' или 'tt'

    state_data = await state.get_data()
    # selected_source_type и source_params уже должны быть в state

    if config_type_from_callback != 'tt':
         await callback.message.edit_text("Ошибка в данных запроса. Начните заново.", reply_markup=main_menu_keyboard())
         await state.clear()
         await callback.answer()
         return


    if method == 'manual':
        # Логика для ручного ввода параметров TT остается прежней
        await state.set_state(UploadProcess.waiting_upload_token)
        tt_params={}
        await state.update_data(tt_params=tt_params)
        tt_params_order = ["upload_api_token", "upload_datasheet_id", "upload_field_map_json"]
        await state.update_data(tt_params_order=tt_params_order, current_tt_param_index=0)

        first_param_key = tt_params_order[0]
        friendly_name = get_friendly_param_name(first_param_key)
        await callback.message.edit_text(f"Введите {friendly_name}:", reply_markup=cancel_kb)
        await callback.answer()

    elif method == 'saved':
        # --- ЛОГИКА ДЛЯ "ИСПОЛЬЗОВАТЬ СОХРАНЕННУЮ" ---
        # Проверяем наличие дефолтной конфигурации True Tabs
        default_config = await sqlite_db.get_default_tt_config()

        if default_config:
            # Если дефолтная конфигурация найдена, предлагаем выбор: использовать дефолтную или выбрать из списка
            builder = InlineKeyboardBuilder()
            builder.row(
                # Кнопка для использования дефолтной конфигурации
                InlineKeyboardButton(text=f"🚀 Использовать по умолчанию: {default_config.get('name', 'Без имени')}", callback_data=f"use_default_tt_config:{default_config.get('name', 'N/A')}") # Используем имя дефолтного конфига
            )
            builder.row(
                # Кнопка для просмотра списка всех сохраненных конфигураций
                InlineKeyboardButton(text="📋 Выбрать из списка", callback_data="list_saved_tt_configs_for_selection") # Новый callback
            )
            builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")) # Кнопка отмены
            keyboard = builder.as_markup()

            text = f"Найдена конфигурация True Tabs по умолчанию.\nВыберите действие:"

            await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
            # Переходим в НОВОЕ состояние ожидания выбора способа использования сохраненной конфиги
            await state.set_state(UploadProcess.choose_saved_tt_method)

        else:
            # Если дефолтная конфигурация не найдена, сразу переходим к отображению списка всех сохраненных
            text = "Дефолтная конфигурация True Tabs не найдена.\nВыберите сохраненную конфигурацию True Tabs из списка:"
            tt_configs = await sqlite_db.list_tt_configs() # Получаем список всех сохраненных конфигов

            if not tt_configs:
                 await callback.message.edit_text("Сохраненных конфигураций True Tabs не найдено. Пожалуйста, выберите ручной ввод.", reply_markup=select_input_method_keyboard('tt'))
                 await state.set_state(UploadProcess.select_tt_input_method) # Остаемся в этом же состоянии
            else:
                # Отображаем список всех сохраненных конфигов для выбора
                keyboard = select_config_keyboard(tt_configs, 'tt_select')
                await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
                # Переходим в состояние ожидания выбора из списка
                await state.set_state(UploadProcess.waiting_saved_tt_selection) # Используем существующее состояние


        await callback.answer()

    else:
        await callback.message.edit_text("Неверный выбор метода ввода.", reply_markup=main_menu_keyboard())
        await state.clear()
        await callback.answer()

# --- НОВЫЙ ХЭНДЛЕР: Выбор способа использования сохраненной конфигурации True Tabs (дефолт или список) ---
@router.callback_query(F.data.startswith("use_default_tt_config:"), UploadProcess.choose_saved_tt_method)
async def use_default_tt_config_handler(callback: CallbackQuery, state: FSMContext):
    # Этот хэндлер срабатывает при нажатии "🚀 Использовать по умолчанию"
    config_name = callback.data.split(":")[1]
    # Получаем полные данные дефолтной конфигурации по имени
    default_config = await sqlite_db.get_tt_config(config_name)

    if default_config:
        # Загружаем параметры дефолтной конфигурации True Tabs в данные состояния
        await state.update_data(tt_params={
            "upload_api_token": default_config.get("upload_api_token"),
            "upload_datasheet_id": default_config.get("upload_datasheet_id"),
            "upload_field_map_json": default_config.get("upload_field_map_json"),
        })

        # Все параметры (источник и TT) собраны, переходим к подтверждению
        await state.set_state(UploadProcess.confirm_parameters)
        state_data = await state.get_data()
        source_params = state_data.get('source_params', {})
        tt_params = state_data.get('tt_params', {})
        confirm_text = build_confirmation_message(state_data.get('selected_source_type', 'Неизвестно'), source_params, tt_params)

        await callback.message.edit_text(f"Использована конфигурация True Tabs по умолчанию: <b>{default_config.get('name', 'Без имени')}</b>.\n"
                                         f"Все параметры собраны. Проверьте и нажмите 'Загрузить'.\n\n" + confirm_text,
                                         reply_markup=upload_confirm_keyboard(),
                                         parse_mode='HTML')

    else:
        # Если конфигурация не найдена, сообщаем об ошибке
        await callback.message.edit_text(f"Ошибка: Дефолтная конфигурация True Tabs '{config_name}' не найдена.", reply_markup=main_menu_keyboard())
        await state.clear()

    await callback.answer()

@router.callback_query(F.data == "list_saved_tt_configs_for_selection", UploadProcess.choose_saved_tt_method)
async def list_all_tt_configs_for_selection(callback: CallbackQuery, state: FSMContext):
    # Этот хэндлер срабатывает при нажатии "📋 Выбрать из списка"
    text = "Выберите сохраненную конфигурацию True Tabs:"
    tt_configs = await sqlite_db.list_tt_configs() # Получаем список всех сохраненных конфигов

    if not tt_configs:
         # Если вдруг список пуст, сообщаем об этом
         await callback.message.edit_text("Сохраненных конфигураций True Tabs не найдено.", reply_markup=select_input_method_keyboard('tt'))
         await state.set_state(UploadProcess.select_tt_input_method) # Возвращаемся к выбору метода ввода
    else:
        # Отображаем список для выбора
        keyboard = select_config_keyboard(tt_configs, 'tt_select')
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML') # Убедитесь, что парсинг HTML включен
        # Переходим в состояние ожидания выбора из списка
        await state.set_state(UploadProcess.waiting_saved_tt_selection) # Используем существующее состояние

    await callback.answer()


# --- Хэндлер выбора сохраненной конфигурации True Tabs (из списка) ---
# Срабатывает в состоянии UploadProcess.waiting_saved_tt_selection
@router.callback_query(F.data.startswith("select_config:tt_select:"), UploadProcess.waiting_saved_tt_selection)
async def process_saved_tt_config_selection(callback: CallbackQuery, state: FSMContext):
    config_name = callback.data.split(":")[2]
    saved_config = await sqlite_db.get_tt_config(config_name)

    if saved_config:
        await state.update_data(tt_params={
            "upload_api_token": saved_config.get("upload_api_token"),
            "upload_datasheet_id": saved_config.get("upload_datasheet_id"),
            "upload_field_map_json": saved_config.get("upload_field_map_json"),
        })

        await state.set_state(UploadProcess.confirm_parameters)
        state_data = await state.get_data()
        source_params = state_data.get('source_params', {})
        tt_params = state_data.get('tt_params', {})
        confirm_text = build_confirmation_message(state_data.get('selected_source_type', 'Неизвестно'), source_params, tt_params)

        await callback.message.edit_text(f"Использована конфигурация True Tabs: <b>{saved_config.get('name', 'Без имени')}</b>.\n"
                                         f"Все параметры собраны. Проверьте и нажмите 'Загрузить'.\n\n" + confirm_text,
                                         reply_markup=upload_confirm_keyboard(),
                                         parse_mode='HTML')

    else:
        await callback.message.edit_text(f"Ошибка: Конфигурация True Tabs '{config_name}' не найдена.", reply_markup=main_menu_keyboard())
        await state.clear()

    await callback.answer()


# ... (Остальные хэндлеры ручного ввода параметров TT - без изменений) ...

# --- Вспомогательная функция для построения сообщения подтверждения (без изменений) ---
def build_confirmation_message(source_type: str, source_params: Dict[str, Any], tt_params: Dict[str, Any]) -> str:
    confirm_text = f"<b>Собранные параметры:</b>\n\n"
    confirm_text += f"Источник: <b>{source_type}</b>\n"

    source_param_order = SOURCE_PARAMS_ORDER.get(source_type, [])
    if 'source_type' not in source_param_order:
         source_param_order = ['source_type'] + [p for p in source_param_order if p != 'source_type']


    for key in source_param_order:
         value = source_params.get(key)
         if value is not None:
              friendly_key = get_friendly_param_name(key)
              if key in ['source_pass', 'neo4j_pass', 'couchbase_pass', 'upload_api_token']:
                  confirm_text += f"  {friendly_key.capitalize()}: <code>***</code>\n"
              elif key in ['source_url', 'cassandra_addresses', 'neo4j_uri', 'couchbase_cluster_url'] and source_type not in ['excel', 'csv']:
                   confirm_text += f"  {friendly_key.capitalize()}: <code>{value}</code>\n"
              elif key == 'source_url' and source_type in ['excel', 'csv']:
                  confirm_text += f"  {get_friendly_param_name('source_url_file').capitalize()}: <code>{value}</code>\n"
              elif key in ['es_query', 'specific_params'] and isinstance(value, (str, dict)):
                  try:
                       value_to_dump = value if isinstance(value, dict) else json.loads(value)
                       query_display = json.dumps(value_to_dump, indent=2, ensure_ascii=False)
                       confirm_text += f"  {friendly_key.capitalize()}:\n<pre><code class=\"language-json\">{query_display}</code></pre>\n"
                  except:
                       confirm_text += f"  {friendly_key.capitalize()}: <code>Некорректный JSON</code>\n"
              else:
                 confirm_text += f"  {friendly_key.capitalize()}: <code>{value}</code>\n"


    confirm_text += f"\n<b>Параметры True Tabs:</b>\n"
    tt_param_order = ["upload_api_token", "upload_datasheet_id", "upload_field_map_json"]
    for key in tt_param_order:
         value = tt_params.get(key)
         if value is not None:
              friendly_key = get_friendly_param_name(key)
              if key == 'upload_api_token':
                  confirm_text += f"  {friendly_key.capitalize()}: <code>***</code>\n"
              elif key == 'upload_field_map_json':
                  try:
                      field_map_display = json.dumps(json.loads(value), indent=2, ensure_ascii=False)
                      confirm_text += f"  {get_friendly_param_name('upload_field_map_json_display').capitalize()}:\n<pre><code class=\"language-json\">{field_map_display}</code></pre>\n"
                  except:
                      confirm_text += f"  {friendly_key.capitalize()}: <code>Некорректный JSON</code>\n"
              else:
                 confirm_text += f"  {friendly_key.capitalize()}: <code>{value}</code>\n"


    confirm_text += f"\nВсе верно? Нажмите 'Загрузить' для старта операции."
    return confirm_text


# --- Хэндлер подтверждения загрузки/выполнения операции (без изменений) ---
@router.callback_query(F.data == "confirm_upload", StateFilter(UploadProcess.confirm_parameters))
async def handle_confirm_upload(callback: CallbackQuery, state: FSMContext, bot: Bot):
    state_data = await state.get_data()
    source_type = state_data.get("selected_source_type", "unknown")
    source_params = state_data.get("source_params", {})
    tt_params = state_data.get("tt_params", {})
    temp_upload_dir = state_data.get('temp_file_upload_dir')


    # TODO: Определить action ('extract', 'update')
    rust_action = "extract"

    output_filename = f"extract_result_{callback.from_user.id}_{source_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    output_filepath = Path(config.TEMP_FILES_DIR) / output_filename


    rust_args = []
    rust_args.append("--action")
    rust_args.append(rust_action)

    rust_args.append("--source")
    rust_args.append(source_type)

    rust_arg_map = {
         'source_url': '--connection',
         'source_user': '--user', 'source_pass': '--pass',
         'source_query': '--query',
         'db_name': '--db-name', 'collection_name': '--collection',
         'key_pattern': '--key-pattern',
         'org': '--org', 'bucket': '--bucket', 'index': '--index',
         'es_query': '--query',
         'redis_pattern': '--key-pattern',
         'mongo_db': '--db-name',
         'mongo_collection': '--collection',
         'specific_params': '--specific-params-json',
         'cassandra_addresses': '--connection', # Добавлен маппинг для адресов Cassandra
         'neo4j_uri': '--connection', # Добавлен маппинг для URI Neo4j
         'couchbase_cluster_url': '--connection', # Добавлен маппинг для URL Couchbase
         'neo4j_user': '--user', # Добавлен маппинг для пользователя Neo4j
         'neo4j_pass': '--pass', # Добавлен маппинг для пароля Neo4j
         'couchbase_user': '--user', # Добавлен маппинг для пользователя Couchbase
         'couchbase_pass': '--pass', # Добавлен маппинг для пароля Couchbase
         'couchbase_bucket': '--bucket', # Добавлен маппинг для бакета Couchbase

    }

    for key, value in source_params.items():
        if value is None or value == "" or key not in rust_arg_map or key in ['source_type', 'name']:
            continue

        rust_arg_name = rust_arg_map[key]

        if key in ['es_query', 'specific_params']:
            if isinstance(value, dict):
                 value_to_dump = value
            elif isinstance(value, str):
                 try:
                      value_to_dump = json.loads(value)
                 except json.JSONDecodeError:
                      print(f"Ошибка при подготовке аргументов Rust: Невалидный JSON string для параметра {key}.", file=sys.stderr)
                      await callback.message.edit_text(f"Ошибка: Неверный формат JSON параметра '{get_friendly_param_name(key)}'. Отмена операции.", reply_markup=main_menu_keyboard())
                      await state.clear()
                      await callback.answer()
                      return
            else:
                 print(f"Ошибка при подготовке аргументов Rust: Неожиданный тип ({type(value)}) для параметра {key}.", file=sys.stderr)
                 await callback.message.edit_text(f"Ошибка: Неожиданный тип данных для параметра '{get_friendly_param_name(key)}'. Отмена операции.", reply_markup=main_menu_keyboard())
                 await state.clear()
                 await callback.answer()
                 return

            rust_args.append(rust_arg_name)
            rust_args.append(json.dumps(value_to_dump))

        # Особая обработка для Cassandra addresses, которые могут быть строкой "host1,host2"
        elif key == 'cassandra_addresses' and source_type == 'cassandra':
             # Rust ожидает --connection host1 --connection host2 ... или один --connection с хостами через запятую
             # Предполагаем, что Rust ожидает один --connection с адресами через запятую.
             # Если Rust ожидает отдельные аргументы, эту логику нужно изменить.
             if isinstance(value, str) and value:
                  rust_args.append(rust_arg_name)
                  rust_args.append(value) # Передаем строку адресов как есть
             elif isinstance(value, list) and value:
                  rust_args.append(rust_arg_name)
                  rust_args.append(','.join(value)) # Объединяем список в строку через запятую
             # Если value None или пустое, оно уже пропущено в начале цикла

        else:
            rust_args.append(rust_arg_name)
            rust_args.append(str(value))


    expected_headers_from_state = state_data.get('expected_headers')
    if expected_headers_from_state:
        try:
             rust_args.append("--expected-headers")
             rust_args.append(json.dumps(expected_headers_from_state))
        except Exception as e:
             print(f"Ошибка при добавлении expected_headers в аргументы Rust: {e}", file=sys.stderr)
             await callback.message.edit_text("Ошибка при обработке ожидаемых заголовков. Отмена операции.", reply_markup=main_menu_keyboard())
             await state.clear()
             await callback.answer()
             return

    rust_args.append("--output-xlsx-path")
    rust_args.append(str(output_filepath))


    # Устанавливаем состояние выполнения операции
    await state.set_state(UploadProcess.operation_in_progress)

    starting_message = await callback.message.edit_text(
        "Запуск операции...",
        reply_markup=operation_in_progress_keyboard()
    )
    await callback.answer()

    asyncio.create_task(process_upload_task(
        bot,
        callback.message.chat.id,
        rust_args,
        source_type,
        tt_params.get("upload_datasheet_id", "N/A"),
        str(output_filepath),
        temp_upload_dir,
        starting_message,
        state,
    ))

# --- Асинхронная задача выполнения операции (без изменений FSM логики) ---
async def process_upload_task(
    bot: Bot, chat_id: int, rust_args: list, source_type: str, datasheet_id: str,
    output_filepath: str, temp_upload_dir: str, message: Message, state: FSMContext):

    process = None
    communicate_future = None
    execution_info = None
    final_status = "ERROR"
    final_message_text = "Произошла неизвестная ошибка."
    duration = 0.0
    extracted_rows = None
    uploaded_records = None
    datasheet_id_from_result = datasheet_id
    final_file_path = None
    error_message = "Неизвестная ошибка выполнения."
    start_time = time.time()

    try:
        execution_info = await execute_rust_command(rust_args)

        if execution_info["status"] == "ERROR":
            final_status = "ERROR"
            error_message = execution_info.get("message", "Ошибка при запуске процесса Rust.")
            duration = execution_info.get("duration_seconds", time.time() - start_time)
            print(f"Ошибка при запуске Rust процесса: {error_message}", file=sys.stderr)
        else:
            process = execution_info["process"]
            communicate_future = execution_info["communicate_future"]
            start_time = execution_info["start_time"]
            command_string = execution_info["command_string"]

            await state.update_data(
                running_process_pid=process.pid,
                running_process_command=command_string,
                running_process_future=communicate_future,
                running_process_object=process,
                process_start_time=start_time
            )

            print(f"Rust process PID: {process.pid} started.", file=sys.stderr)

            try:
                stdout_data, stderr_data = await communicate_future
                end_time = time.time()
                duration = end_time - start_time

                stdout_str = stdout_data.decode('utf-8', errors='ignore')
                stderr_str = stderr_data.decode('utf-8', errors='ignore')

                print(f"Rust stdout (PID {process.pid}):\n{stdout_str}", file=sys.stderr)
                print(f"Rust stderr (PID {process.pid}):\n{stderr_str}", file=sys.stderr)
                print(f"Rust процесс PID {process.pid} завершен с кодом: {process.returncode}", file=sys.stderr)

                try:
                    json_result: Dict[str, Any] = json.loads(stdout_str)
                    final_status = json_result.get("status", "ERROR")
                    error_message = json_result.get("message", "Сообщение от утилиты отсутствует.")
                    extracted_rows = json_result.get("extracted_rows")
                    uploaded_records = json_result.get("uploaded_records")
                    datasheet_id_from_result = json_result.get("datasheet_id", datasheet_id_from_result)
                    final_file_path = json_result.get("file_path")

                    if final_status == "SUCCESS" and error_message == "Сообщение от утилиты отсутствует.":
                         error_message = "Operation completed successfully."


                except json.JSONDecodeError:
                    final_status = "ERROR"
                    error_message = f"Rust процесс завершился с кодом {process.returncode}, но stdout не является валидным JSON. Stderr:\n{stderr_str}\nStdout:\n{stdout_str}"
                except Exception as e:
                     final_status = "ERROR"
                     error_message = f"Ошибка при обработке JSON результата Rust: {e}. Stderr:\n{stderr_str}\nStdout:\n{stdout_str}"

                if final_status != "SUCCESS" and process.returncode != 0:
                     if error_message == "Сообщение от утилиты отсутствует." or \
                        error_message.startswith("Rust процесс завершился с кодом"):
                           error_message = f"Rust процесс завершился с ошибкой (код {process.returncode}). Stderr:\n{stderr_str}\nStdout:\n{stdout_str}"
                     final_status = "ERROR"


            except asyncio.CancelledError:
                print(f"Задача Communicate отменена для PID {process.pid}", file=sys.stderr)
                final_status = "CANCELLED"
                error_message = "Операция отменена пользователем."
                duration = time.time() - start_time

            except Exception as e:
                final_status = "ERROR"
                error_message = f"Произошла внутренняя ошибка во время выполнения Rust процесса: {e}"
                duration = time.time() - start_time
                print(f"Unexpected error during Rust process execution: {e}", file=sys.stderr)


    except Exception as e:
        final_status = "ERROR"
        error_message = f"Произошла внутренняя ошибка при запуске или выполнении операции: {e}"
        duration = time.time() - start_time
        print(f"Unexpected error in process_upload_task (outer): {e}", file=sys.stderr)


    finally:
        print(f"Завершение process_upload_task для PID {process.pid if process else 'N/A'} со статусом: {final_status}", file=sys.stderr)
        await state.update_data(
             running_process_pid=None,
             running_process_command=None,
             running_process_future=None,
             running_process_object=None,
             process_start_time=None
        )
        await state.clear()


        if temp_upload_dir and os.path.exists(temp_upload_dir):
            try:
                shutil.rmtree(temp_upload_dir)
                print(f"Временная директория {temp_upload_dir} удалена.")
            except Exception as e:
                print(f"Ошибка при удалении временной директории {temp_upload_dir}: {e}", file=sys.stderr)

        try:
             await sqlite_db.add_upload_record(
                 source_type=source_type,
                 status=final_status,
                 file_path=final_file_path,
                 error_message=error_message,
                 true_tabs_datasheet_id=datasheet_id_from_result,
                 duration_seconds=duration
             )
             print(f"Запись истории добавлена со статусом: {final_status}", file=sys.stderr)
        except Exception as e:
             print(f"Ошибка при добавлении записи истории: {e}", file=sys.stderr)
             try:
                 await bot.send_message(chat_id, f"⚠️ Операция завершена со статусом '{final_status}', но произошла ошибка при сохранении в историю: {e}", parse_mode='HTML')
             except Exception as send_e:
                  print(f"Ошибка при отправке сообщения об ошибке истории: {send_e}", file=sys.stderr)


        try:
            final_message_text = f"✅ <b>Операция успешно завершена!</b>\n" if final_status == "SUCCESS" else \
                                 f"⚠️ <b>Операция отменена.</b>\n" if final_status == "CANCELLED" else \
                                 f"❌ <b>Операция завершилась с ошибкой!</b>\n"

            final_message_text += f"Источник: <code>{source_type}</code>\n"
            if datasheet_id_from_result and datasheet_id_from_result != 'N/A':
                final_message_text += f"Datasheet ID: <code>{datasheet_id_from_result}</code>\n"

            if final_status == "SUCCESS":
                if extracted_rows is not None:
                     final_message_text += f"Извлечено строк: {extracted_rows}\n"
                if uploaded_records is not None:
                     final_message_text += f"Загружено записей: {uploaded_records}\n"
                final_message_text += f"Время выполнения: {duration:.2f} секунд\n"
                if final_file_path:
                     final_message_text += f"Файл сохранен на сервере бота: <code>{final_file_path}</code>"
                if error_message != "Сообщение от утилиты отсутствует." and error_message != "Operation completed successfully.":
                    final_message_text += f"\n<i>Сообщение от утилиты:</i> {error_message}"

                await message.edit_text(final_message_text, parse_mode='HTML', reply_markup=main_menu_keyboard())

                if final_file_path and os.path.exists(final_file_path):
                     try:
                         await bot.send_document(chat_id, document= FSInputFile(final_file_path, filename=os.path.basename(final_file_path)))
                     except Exception as e:
                          print(f"Ошибка при отправке файла в Telegram: {e}", file=sys.stderr)
                          await bot.send_message(chat_id, f"❌ Ошибка при отправке файла: {e}")

            elif final_status == "CANCELLED":
                 final_message_text += f"Время до отмены: {duration:.2f} секунд\n"
                 final_message_text += f"Причина: {error_message}"

                 await message.edit_text(final_message_text, parse_mode='HTML', reply_markup=main_menu_keyboard())

            else:
                if extracted_rows is not None:
                     final_message_text += f"Извлечено строк (до ошибки): {extracted_rows}\n"
                if uploaded_records is not None:
                     final_message_text += f"Загружено записей (до ошибки): {uploaded_records}\n"
                final_message_text += f"Время выполнения: {duration:.2f} секунд\n\n"

                final_message_text += error_message

                await message.edit_text(final_message_text, parse_mode='HTML', reply_markup=main_menu_keyboard())

        except Exception as e:
            print(f"Критическая ошибка при финальном обновлении сообщения пользователя: {e}", file=sys.stderr)
            try:
                await bot.send_message(chat_id, f"❌ Произошла критическая ошибка при завершении операции: {e}", parse_mode='HTML')
            except Exception as send_e:
                 print(f"Ошибка при отправке критического сообщения об ошибке: {send_e}", file=sys.stderr)


# --- Хэндлер для отмены запущенной операции (без изменений) ---
@router.callback_query(F.data == "cancel_operation", StateFilter(UploadProcess.operation_in_progress))
async def handle_cancel_operation(callback: CallbackQuery, state: FSMContext):
    state_data = await state.get_data()
    stored_process: Optional[asyncio.subprocess.Process] = state_data.get("running_process_object")
    process_future: Optional[asyncio.Task] = state_data.get("running_process_future")
    process_pid = state_data.get("running_process_pid")

    await callback.answer("Запрос на отмену отправлен...")

    if stored_process and stored_process.returncode is None:
         print(f"Получен запрос на отмену операции PID: {process_pid}", file=sys.stderr)
         try:
             stored_process.terminate()
             if process_future and not process_future.done():
                  process_future.cancel()
         except ProcessLookupError:
              print(f"Попытка отменить процесс PID {process_pid}, но он уже не найден.", file=sys.stderr)
         except Exception as e:
             print(f"Ошибка при попытке завершить процесс PID {process_pid}: {e}", file=sys.stderr)
             try:
                  await callback.message.answer(f"Произошла ошибка при попытке отмены операции: {e}")
             except Exception as send_e:
                  print(f"Ошибка при отправке сообщения об ошибке отмены: {send_e}", file=sys.stderr)
    elif stored_process and stored_process.returncode is not None:
        print(f"Отмена нажата, но процесс PID {process_pid} уже завершился с кодом {stored_process.returncode}", file=sys.stderr)
        try:
            await callback.message.edit_text("Операция уже завершена.", reply_markup=main_menu_keyboard())
        except Exception as e:
             print(f"Ошибка при обновлении сообщения после отмены уже завершенного процесса: {e}", file=sys.stderr)

    else:
        print("Отмена нажата, но информация о процессе не найдена в состоянии.", file=sys.stderr)
        await state.clear()
        try:
             await callback.message.edit_text("Операция уже не выполняется.", reply_markup=main_menu_keyboard())
        except Exception as e:
             print(f"Ошибка при обновлении сообщения после отмены при отсутствии информации о процессе: {e}", file=sys.stderr)


# --- Вспомогательная клавиатура отмены (без изменений) ---
cancel_kb = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
])

# --- Хэндлер отмены FSM (без изменений) ---
@router.callback_query(F.data == "cancel")
async def cancel_fsm(callback: CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await callback.answer("Нет активной операции для отмены.")
        return

    if current_state == UploadProcess.operation_in_progress:
        await handle_cancel_operation(callback, state)
    else:
        await state.clear()
        await callback.message.edit_text("Операция отменена.", reply_markup=main_menu_keyboard())
        await callback.answer()