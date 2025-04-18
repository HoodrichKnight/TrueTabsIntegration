import json

from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from keyboards.inline import (
    main_menu_keyboard, manage_configs_menu_keyboard,
    manage_source_configs_keyboard, manage_tt_configs_keyboard,
    list_configs_keyboard, config_actions_keyboard, delete_confirm_keyboard,
    source_selection_keyboard
)
from database import sqlite_db
from .upload_handlers import SOURCE_PARAMS_EXAMPLE

router = Router()

class ConfigAddProcess(StatesGroup):
    waiting_config_name = State()
    waiting_source_config_type = State()
    waiting_source_param = State()
    waiting_tt_config_name = State()
    waiting_tt_param = State()

@router.callback_query(F.data == "manage_configs")
async def manage_configs_menu(callback: CallbackQuery):
    await callback.message.edit_text(
        "Выберите, какими конфигурациями управлять:",
        reply_markup=manage_configs_menu_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data == "manage_source_configs")
async def manage_source_configs_menu(callback: CallbackQuery):
    await callback.message.edit_text(
        "Управление конфигурациями источников:",
        reply_markup=manage_source_configs_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data == "manage_tt_configs")
async def manage_tt_configs_menu(callback: CallbackQuery):
    await callback.message.edit_text(
        "Управление конфигурациями True Tabs:",
        reply_markup=manage_tt_configs_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data == "add_source_config")
async def start_add_source_config(callback: CallbackQuery, state: FSMContext):
    await state.update_data(config_type='source')
    await state.set_state(ConfigAddProcess.waiting_config_name)
    await callback.message.edit_text("Введите уникальное имя для этой конфигурации источника:")
    await callback.answer()

@router.callback_query(F.data == "add_tt_config")
async def start_add_tt_config(callback: CallbackQuery, state: FSMContext):
    await state.update_data(config_type='tt')
    await state.set_state(ConfigAddProcess.waiting_config_name)
    await callback.message.edit_text("Введите уникальное имя для этой конфигурации True Tabs:")
    await callback.answer()

@router.message(ConfigAddProcess.waiting_config_name)
async def process_config_name(message: Message, state: FSMContext):
    config_name = message.text.strip()
    if not config_name:
        await message.answer("Имя конфигурации не может быть пустым. Введите имя:")
        return

    state_data = await state.get_data()
    config_type = state_data['config_type']

    if config_type == 'source':
         existing = await sqlite_db.get_source_config(config_name)
    elif config_type == 'tt':
         existing = await sqlite_db.get_tt_config(config_name)
    else:
         await message.answer("Неизвестный тип конфигурации.", reply_markup=main_menu_keyboard())
         await state.clear()
         return

    if existing:
        await message.answer(f"Конфигурация с именем '{config_name}' уже существует. Выберите другое имя:")
        return

    await state.update_data(name=config_name, current_params={})

    if config_type == 'source':
        await state.set_state(ConfigAddProcess.waiting_source_config_type)
        await message.answer("Выберите тип источника для этой конфигурации:", reply_markup=source_selection_keyboard()) # Используем клавиатуру выбора источника
    elif config_type == 'tt':
        tt_params_order = ["upload_api_token", "upload_datasheet_id", "upload_field_map_json"]
        await state.update_data(param_keys_order=tt_params_order, current_param_index=0)
        await state.set_state(ConfigAddProcess.waiting_tt_param)
        await message.answer(f"Введите параметр '{tt_params_order[0]}':")


@router.callback_query(F.data.startswith("select_source:"), ConfigAddProcess.waiting_source_config_type)
async def process_source_config_type(callback: CallbackQuery, state: FSMContext):
    source_type = callback.data.split(":")[1]
    await state.update_data(source_type=source_type)

    params_order = SOURCE_PARAMS_EXAMPLE.get(source_type, [])
    params_order = [p for p in params_order if p != "source_type"]

    if not params_order:
        state_data = await state.get_data()
        config_name = state_data['name']
        await sqlite_db.add_source_config(config_name, source_type, {})
        await state.clear()
        await callback.message.edit_text(f"Конфигурация источника '{config_name}' ({source_type}) успешно добавлена.", reply_markup=manage_source_configs_keyboard())
        await callback.answer()
        return

    await state.update_data(param_keys_order=params_order, current_param_index=0)
    await state.set_state(ConfigAddProcess.waiting_source_param)
    await callback.message.edit_text(f"Введите параметр '{params_order[0]}':")
    await callback.answer()

@router.message(ConfigAddProcess.waiting_source_param)
async def process_source_param(message: Message, state: FSMContext):
    user_input = message.text.strip()
    # TODO: Добавить валидацию введенного значения в зависимости от текущего параметра

    state_data = await state.get_data()
    param_keys_order = state_data['param_keys_order']
    current_param_index = state_data['current_param_index']
    current_params = state_data['current_params']

    current_param_key = param_keys_order[current_param_index]
    current_params[current_param_key] = user_input

    next_param_index = current_param_index + 1

    if next_param_index < len(param_keys_order):
        await state.update_data(current_params=current_params, current_param_index=next_param_index)
        next_param_key = param_keys_order[next_param_index]
        await message.answer(f"Введите параметр '{next_param_key}':")
    else:
        config_name = state_data['name']
        source_type = state_data['source_type']
        await sqlite_db.add_source_config(config_name, source_type, current_params)
        await state.clear()
        await message.answer(f"Конфигурация источника '{config_name}' ({source_type}) успешно добавлена.", reply_markup=manage_source_configs_keyboard())

@router.message(ConfigAddProcess.waiting_tt_param)
async def process_tt_param(message: Message, state: FSMContext):
    user_input = message.text.strip()
    # TODO: Добавить валидацию введенного значения

    state_data = await state.get_data()
    param_keys_order = state_data['param_keys_order']
    current_param_index = state_data['current_param_index']
    current_params = state_data['current_params']

    current_param_key = param_keys_order[current_param_index]
    current_params[current_param_key] = user_input

    next_param_index = current_param_index + 1

    if next_param_index < len(param_keys_order):
        await state.update_data(current_params=current_params, current_param_index=next_param_index)
        next_param_key = param_keys_order[next_param_index]
        await message.answer(f"Введите параметр '{next_param_key}':")
    else:
        config_name = state_data['name']
        if all(key in current_params for key in ["upload_api_token", "upload_datasheet_id", "upload_field_map_json"]):
             await sqlite_db.add_tt_config(
                 config_name,
                 current_params["upload_api_token"],
                 current_params["upload_datasheet_id"],
                 current_params["upload_field_map_json"]
             )
             await state.clear()
             await message.answer(f"Конфигурация True Tabs '{config_name}' успешно добавлена.", reply_markup=manage_tt_configs_keyboard())
        else:
             await state.clear()
             await message.answer("Ошибка: Не все обязательные параметры True Tabs были собраны. Начните заново.", reply_markup=manage_tt_configs_keyboard())

@router.callback_query(F.data == "list_source_configs")
async def list_source_configs_handler(callback: CallbackQuery):
    configs = await sqlite_db.list_source_configs()
    if not configs:
        text = "Сохраненные конфигурации источников отсутствуют."
        keyboard = manage_source_configs_keyboard()
    else:
        text = "📋 Сохраненные конфигурации источников:\n\n"
        builder = InlineKeyboardBuilder()
        for config in configs:
             text += f"- {config['name']} ({config['source_type']})\n"
             builder.row(InlineKeyboardButton(text=f"🔧 {config['name']}", callback_data=f"source_config_actions:{config['name']}"))

        builder.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="manage_source_configs"))
        keyboard = builder.as_markup()


    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@router.callback_query(F.data == "list_tt_configs")
async def list_tt_configs_handler(callback: CallbackQuery):
    configs = await sqlite_db.list_tt_configs()
    if not configs:
        text = "Сохраненные конфигурации True Tabs отсутствуют."
        keyboard = manage_tt_configs_keyboard()
    else:
        text = "📋 Сохраненные конфигурации True Tabs:\n\n"
        builder = InlineKeyboardBuilder()
        for config in configs:
             text += f"- {config['name']} (Datasheet ID: {config['upload_datasheet_id']})\n"
             builder.row(InlineKeyboardButton(text=f"🔧 {config['name']}", callback_data=f"tt_config_actions:{config['name']}")) # Кнопка действия

        builder.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="manage_tt_configs"))
        keyboard = builder.as_markup()


    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("source_config_actions:"))
async def source_config_actions_handler(callback: CallbackQuery):
    config_name = callback.data.split(":")[1]
    config = await sqlite_db.get_source_config(config_name)
    if config:
        details_text = f"<b>Конфигурация источника:</b> {config_name}\n"
        details_text += f"Тип источника: <code>{config.get('source_type', 'N/A')}</code>\n"
        details_text += f"URL/Путь: <code>{config.get('source_url', 'N/A')}</code>\n"
        details_text += f"Пользователь: <code>{config.get('source_user', 'N/A')}</code>\n"
        details_text += f"Запрос: <code>{config.get('source_query', 'N/A')}</code>\n"
        if config.get('specific_params'):
             details_text += f"Спец. параметры: <pre><code class=\"language-json\">{json.dumps(config['specific_params'], indent=2, ensure_ascii=False)}</code></pre>\n"


        await callback.message.edit_text(
            details_text,
            reply_markup=config_actions_keyboard(config_name, 'source'),
            parse_mode='HTML'
        )
    else:
        await callback.message.edit_text(f"Конфигурация источника '{config_name}' не найдена.", reply_markup=manage_source_configs_keyboard())

    await callback.answer()


@router.callback_query(F.data.startswith("tt_config_actions:"))
async def tt_config_actions_handler(callback: CallbackQuery):
    config_name = callback.data.split(":")[1]
    config = await sqlite_db.get_tt_config(config_name)
    if config:
        details_text = f"<b>Конфигурация True Tabs:</b> {config_name}\n"
        details_text += f"Datasheet ID: <code>{config.get('upload_datasheet_id', 'N/A')}</code>\n"
        try:
             field_map_display = json.dumps(json.loads(config.get('upload_field_map_json', '{}')), indent=2, ensure_ascii=False)
             details_text += f"Сопоставление Field ID:\n<pre><code class=\"language-json\">{field_map_display}</code></pre>\n"
        except:
             details_text += f"Сопоставление Field ID: <code>Некорректный JSON</code>\n"


        await callback.message.edit_text(
            details_text,
            reply_markup=config_actions_keyboard(config_name, 'tt'),
            parse_mode='HTML'
        )
    else:
        await callback.message.edit_text(f"Конфигурация True Tabs '{config_name}' не найдена.", reply_markup=manage_tt_configs_keyboard())

    await callback.answer()

@router.callback_query(F.data.startswith("delete_source_config_confirm:"))
async def delete_source_config_confirm_handler(callback: CallbackQuery):
    config_name = callback.data.split(":")[1]
    await callback.message.edit_text(
        f"Вы уверены, что хотите удалить конфигурацию источника '{config_name}'?",
        reply_markup=delete_confirm_keyboard(config_name, 'source')
    )
    await callback.answer()

@router.callback_query(F.data.startswith("delete_tt_config_confirm:"))
async def delete_tt_config_confirm_handler(callback: CallbackQuery):
    config_name = callback.data.split(":")[1]
    await callback.message.edit_text(
        f"Вы уверены, что хотите удалить конфигурацию True Tabs '{config_name}'?",
        reply_markup=delete_confirm_keyboard(config_name, 'tt')
    )
    await callback.answer()


@router.callback_query(F.data.startswith("delete_source_config:"))
async def delete_source_config_handler(callback: CallbackQuery):
    config_name = callback.data.split(":")[1]
    success = await sqlite_db.delete_source_config(config_name)
    if success:
        await callback.message.edit_text(f"Конфигурация источника '{config_name}' успешно удалена.", reply_markup=manage_source_configs_keyboard())
    else:
        await callback.message.edit_text(f"Ошибка при удалении конфигурации источника '{config_name}'.", reply_markup=manage_source_configs_keyboard())
    await callback.answer()

@router.callback_query(F.data.startswith("delete_tt_config:"))
async def delete_tt_config_handler(callback: CallbackQuery):
    config_name = callback.data.split(":")[1]
    success = await sqlite_db.delete_tt_config(config_name)
    if success:
        await callback.message.edit_text(f"Конфигурация True Tabs '{config_name}' успешно удалена.", reply_markup=manage_tt_configs_keyboard())
    else:
        await callback.message.edit_text(f"Ошибка при удалении конфигурации True Tabs '{config_name}'.", reply_markup=manage_tt_configs_keyboard())
    await callback.answer()