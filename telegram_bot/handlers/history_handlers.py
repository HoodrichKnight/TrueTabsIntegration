from aiogram import Router, F
from aiogram.types import CallbackQuery
from ..keyboards import history_pagination_keyboard, main_menu_keyboard
from ..database.sqlite_db import get_upload_history, count_upload_history
import os
from datetime import datetime

router = Router()

RECORDS_PER_PAGE = 5

@router.callback_query(F.data.startswith("view_history:"))
async def handle_view_history(callback: CallbackQuery):
    try:
        offset = int(callback.data.split(":")[1])
    except (IndexError, ValueError):
        offset = 0

    total_records = await count_upload_history()
    history_records = await get_upload_history(limit=RECORDS_PER_PAGE, offset=offset)

    if not history_records:
        text = "История загрузок пуста."
        keyboard = main_menu_keyboard()
    else:
        text = "📊 <b>История загрузок:</b>\n\n"
        for record in history_records:
            timestamp_local = datetime.fromisoformat(record["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
            text += f"#{record['id']}: {timestamp_local}\n"
            text += f"  Источник: <code>{record['source_type']}</code>\n"
            text += f"  Статус: {'✅ Успех' if record['status'] == 'SUCCESS' else '❌ Ошибка'}\n"
            if record['duration_seconds'] is not None:
                 text += f"  Время: {record['duration_seconds']:.2f} сек\n"
            if record['file_path']:
                text += f"  Файл: <code>{os.path.basename(record['file_path'])}</code>\n"
            if record['true_tabs_datasheet_id'] and record['true_tabs_datasheet_id'] != 'N/A':
                 text += f"  Datasheet: <code>{record['true_tabs_datasheet_id']}</code>\n"
            if record['error_message'] and record['status'] == 'ERROR':
                 text += f"  Ошибка: <pre>{record['error_message'][:150]}...</pre>\n"
            text += "---\n"

        keyboard = history_pagination_keyboard(offset, total_records, RECORDS_PER_PAGE)

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()

@router.callback_query(F.data == "ignore")
async def handle_ignore_callback(callback: CallbackQuery):
    await callback.answer()