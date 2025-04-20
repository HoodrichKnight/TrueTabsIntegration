from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, FSInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from ..keyboards import main_menu_keyboard
from ..utils.rust_executor import execute_rust_command
from ..database.sqlite_db import add_upload_record
import os
import sys
import asyncio
import json
from .. import config
from datetime import datetime
from typing import Dict, Any

router = Router()

class UploadProcess(StatesGroup):
    waiting_for_params = State()

@router.message(UploadProcess.waiting_for_params)
async def process_params_input(message: Message, state: FSMContext, bot: Bot):
    user_input = message.text
    if user_input.lower() == "/cancel":
        await state.clear()
        await message.answer("Операция отменена.", reply_markup=main_menu_keyboard())
        return

    try:
        params: Dict[str, Any] = json.loads(user_input)
        state_data = await state.get_data()
        source_type = state_data.get("selected_source_type")

        if not source_type:
             await message.answer("Ошибка: Тип источника не определен. Начните заново с /start.", reply_markup=main_menu_keyboard())
             await state.clear()
             return

        params["source_type"] = source_type

        required_upload_params = ["upload_api_token", "upload_datasheet_id", "upload_field_map_json"]
        for param_name in required_upload_params:
            if param_name not in params or not params[param_name]:
                await message.answer(f"Ошибка: Обязательный параметр '{param_name}' отсутствует или пуст в JSON.", reply_markup=main_menu_keyboard())
                await state.clear()
                return

        if not os.path.exists(config.TEMP_FILES_DIR):
             os.makedirs(config.TEMP_FILES_DIR)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"upload_{message.from_user.id}_{source_type}_{timestamp}.xlsx"
        output_filepath = os.path.join(config.TEMP_FILES_DIR, output_filename)
        params["output_xlsx_path"] = output_filepath

        if config.TRUE_TABS_API_TOKEN:
             params["upload_api_token"] = config.TRUE_TABS_API_TOKEN
        if config.TRUE_TABS_DATASHEET_ID:
             params["upload_datasheet_id"] = config.TRUE_TABS_DATASHEET_ID


        await state.clear()
        await message.answer("Параметры получены. Начинаю извлечение и загрузку. Это может занять время...", reply_markup=main_menu_keyboard())

        asyncio.create_task(process_upload_task(bot, message.chat.id, params))


    except json.JSONDecodeError as e:
        await message.answer(f"Ошибка парсинга JSON: {e}. Пожалуйста, проверьте формат и попробуйте снова.", reply_markup=main_menu_keyboard())
        await state.clear()
    except Exception as e:
        await message.answer(f"Произошла ошибка при обработке параметров: {e}", reply_markup=main_menu_keyboard())
        await state.clear()

async def process_upload_task(bot: Bot, chat_id: int, params: Dict[str, Any]):
    """Запускает выполнение Rust команды и обрабатывает результат."""
    source_type = params.get("source_type", "unknown")
    output_filepath = params.get("output_xlsx_path")
    datasheet_id_for_history = params.get("upload_datasheet_id", "N/A")

    rust_args = []
    for key, value in params.items():
        if value is not None:
            rust_args.append(f"--{key.replace('_', '-')}")
            if isinstance(value, (dict, list)):
                 rust_args.append(json.dumps(value))
            elif not isinstance(value, bool):
                 rust_args.append(str(value))

    result = await execute_rust_command(rust_args)

    await handle_upload_result(bot, chat_id, result, source_type, datasheet_id_for_history, output_filepath)

async def handle_upload_result(bot: Bot, chat_id: int, result: Dict[str, Any], source_type: str, datasheet_id: str, output_filepath: str = None):
    status = result.get("status", "ERROR")
    file_path = result.get("file_path")
    message_text = result.get("message", "Неизвестная ошибка.")
    duration = result.get("duration", 0.0)

    await add_upload_record(
        source_type=source_type,
        status=status,
        file_path=file_path,
        error_message=message_text,
        true_tabs_datasheet_id=datasheet_id,
        duration_seconds=duration
    )

    if status == "SUCCESS":
        final_message_text = f"✅ Загрузка успешно завершена!\n"
        final_message_text += f"Источник: {source_type}\n"
        final_message_text += f"Datasheet ID: {datasheet_id}\n"
        final_message_text += f"Время выполнения: {duration:.2f} секунд\n"
        final_message_text += f"Файл сохранен на сервере бота: <code>{file_path}</code>"

        await bot.send_message(chat_id, final_message_text, parse_mode='HTML')

        if file_path and os.path.exists(file_path):
            try:
                await bot.send_document(chat_id, document= FSInputFile(file_path, filename=os.path.basename(file_path)))
                # os.remove(file_path)
            except Exception as e:
                 print(f"Ошибка при отправке файла в Telegram: {e}", file=sys.stderr)
                 await bot.send_message(chat_id, f"❌ Ошибка при отправке файла: {e}")
        else:
             await bot.send_message(chat_id, "⚠️ Не удалось найти или отправить файл XLSX.")

    else:
        final_message_text = f"❌ Ошибка при извлечении или загрузке данных!\n"
        final_message_text += f"Источник: {source_type}\n"
        final_message_text += f"Время выполнения: {duration:.2f} секунд\n"
        final_message_text += f"Сообщение об ошибке:\n<pre><code>{message_text}</code></pre>"

        await bot.send_message(chat_id, final_message_text, parse_mode='HTML')