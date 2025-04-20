import json # Добавлен для возможного отображения параметров в будущем
from aiogram import Router, F, Bot # Добавлен Bot для отправки файла
from aiogram.types import CallbackQuery, InlineKeyboardButton, FSInputFile # Добавлен InlineKeyboardButton, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder # Добавлен InlineKeyboardBuilder
from ..keyboards import history_pagination_keyboard, main_menu_keyboard
from ..database.sqlite_db import get_upload_history, count_upload_history, get_upload_history_by_id # Добавлен get_upload_history_by_id
import os
import sys
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
    # Убедимся, что offset не превышает общее количество записей
    if offset >= total_records and total_records > 0:
        offset = max(0, total_records - RECORDS_PER_PAGE)


    history_records = await get_upload_history(limit=RECORDS_PER_PAGE, offset=offset)

    text = "📊 <b>История загрузок:</b>\n\n"
    builder = InlineKeyboardBuilder() # Используем билдер для создания кнопок записей и пагинации

    if not history_records:
        text = "История загрузок пуста."
        # Если история пуста, просто показываем кнопку назад в меню
        builder.row(InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="main_menu"))
    else:
        for record in history_records:
            # Форматируем дату и время для отображения
            timestamp_local = datetime.fromisoformat(record["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
            # Краткая сводка для списка
            record_summary = f"#{record['id']}: {timestamp_local} - {record['source_type']} - {'✅ Успех' if record['status'] == 'SUCCESS' else '❌ Ошибка'}"
            text += f"{record_summary}\n"
            # Добавляем кнопку "Детали" для каждой записи в отдельную строку
            builder.row(InlineKeyboardButton(text=f"👁️ Детали #{record['id']}", callback_data=f"view_history_details:{record['id']}"))
        text += "---\n" # Разделитель перед кнопками пагинации

        # Добавляем кнопки пагинации
        prev_offset = max(0, offset - RECORDS_PER_PAGE)
        next_offset = offset + RECORDS_PER_PAGE
        has_prev = offset > 0
        has_next = next_offset < total_records

        pagination_buttons = []
        if has_prev:
            pagination_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"view_history:{prev_offset}"))
        current_page_start = offset + 1
        current_page_end = min(offset + RECORDS_PER_PAGE, total_records)
        pagination_buttons.append(InlineKeyboardButton(text=f"{current_page_start}-{current_page_end} из {total_records}", callback_data="ignore")) # Кнопка "игнорировать" для отображения текущей страницы
        if has_next:
            pagination_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"view_history:{next_offset}"))

        # Добавляем кнопки пагинации в одну строку (если есть)
        if pagination_buttons:
             builder.row(*pagination_buttons)

        # Добавляем кнопку "Назад в меню"
        builder.row(InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="main_menu"))

    # Редактируем сообщение с обновленным текстом и клавиатурой
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode='HTML') # Используем as_markup() билдера
    await callback.answer()

# --- Хэндлер для просмотра деталей конкретной записи истории ---
@router.callback_query(F.data.startswith("view_history_details:"))
async def handle_view_history_details(callback: CallbackQuery, bot: Bot): # Добавлен bot для отправки файла
    try:
        record_id = int(callback.data.split(":")[1])
    except (IndexError, ValueError):
        await callback.message.edit_text("Неверный ID записи истории.")
        await callback.answer()
        return

    # Получаем полную запись истории из БД по ID
    record = await get_upload_history_by_id(record_id) # Нужна эта функция в sqlite_db.py

    if not record:
        await callback.message.edit_text(f"Запись истории с ID #{record_id} не найдена.")
        await callback.answer()
        return

    # Форматируем подробную информацию для отображения
    details_text = f"📊 <b>Детали операции #{record['id']}</b>\n\n"
    timestamp_local = datetime.fromisoformat(record["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
    details_text += f"Дата и время: {timestamp_local}\n"
    details_text += f"Источник: <code>{record['source_type']}</code>\n"
    details_text += f"Статус: {'✅ Успех' if record['status'] == 'SUCCESS' else '❌ Ошибка'}\n"
    if record['duration_seconds'] is not None:
        details_text += f"Время выполнения: {record['duration_seconds']:.2f} сек\n"
    if record['file_path']:
        details_text += f"Файл: <code>{os.path.basename(record['file_path'])}</code>\n" # Показываем только имя файла
    if record['true_tabs_datasheet_id'] and record['true_tabs_datasheet_id'] != 'N/A':
        details_text += f"Datasheet ID: <code>{record['true_tabs_datasheet_id']}</code>\n"

    # Полное сообщение об ошибке, если есть и статус - Ошибка
    if record['error_message'] and record['status'] == 'ERROR':
        details_text += f"\n<b>Сообщение об ошибке:</b>\n<pre><code>{record['error_message']}</code></pre>\n"

    if record.get('parameters'): # Если поле с параметрами существует в записи истории
         try:
              params_display = json.dumps(json.loads(record['parameters']), indent=2, ensure_ascii=False)
              details_text += f"\n<b>Использованные параметры:</b>\n<pre><code class=\"language-json\">{params_display}</code></pre>\n"
         except Exception:
              details_text += f"\n<b>Использованные параметры:</b> <code>Некорректный JSON</code>\n"


    builder = InlineKeyboardBuilder()
    # Кнопка для повторной отправки файла, если операция была успешной и файл существует на сервере бота
    if record['status'] == 'SUCCESS' and record['file_path'] and os.path.exists(record['file_path']):
         builder.row(InlineKeyboardButton(text="📎 Отправить файл", callback_data=f"send_history_file:{record['id']}"))

    # Кнопка для возврата к списку истории (на первую страницу)
    builder.row(InlineKeyboardButton(text="⬅️ Назад к списку", callback_data=f"view_history:0"))

    # Редактируем сообщение с подробной информацией и кнопками действий
    await callback.message.edit_text(details_text, reply_markup=builder.as_markup(), parse_mode='HTML')
    await callback.answer()

# --- Хэндлер для повторной отправки файла истории ---
@router.callback_query(F.data.startswith("send_history_file:"))
async def handle_send_history_file(callback: CallbackQuery, bot: Bot):
    try:
        record_id = int(callback.data.split(":")[1])
    except (IndexError, ValueError):
        await callback.answer("Неверный ID записи истории для отправки файла.")
        return

    # Получаем запись истории по ID
    record = await get_upload_history_by_id(record_id) # Нужна эта функция в sqlite_db.py

    # Проверяем, что запись найдена, статус Успех, путь к файлу есть и файл существует
    if not record or record['status'] != 'SUCCESS' or not record['file_path'] or not os.path.exists(record['file_path']):
        await callback.answer("Файл не найден или операция не была успешной.")
        return

    try:
        # Отправляем файл пользователю
        await bot.send_document(callback.message.chat.id, document=FSInputFile(record['file_path'], filename=os.path.basename(record['file_path'])))
        await callback.answer("Файл отправлен.")
    except Exception as e:
        print(f"Ошибка при повторной отправке файла истории: {e}", file=sys.stderr)
        await callback.answer("Произошла ошибка при отправке файла.")


@router.callback_query(F.data == "ignore")
async def handle_ignore_callback(callback: CallbackQuery):
    # Хэндлер для кнопок, которые должны просто игнорироваться (например, кнопка текущей страницы пагинации)
    await callback.answer()