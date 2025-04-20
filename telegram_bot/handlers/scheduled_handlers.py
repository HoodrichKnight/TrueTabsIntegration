import asyncio
import json
import sys
import time
import os
import shutil
import uuid # Импортируем uuid для генерации уникальных ID заданий
from datetime import datetime
from typing import Dict, Any, Optional, List

from aiogram import Bot, Router, F
from aiogram.types import FSInputFile, Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramAPIError
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import StateFilter

# Импорт APScheduler trigger classes и scheduler instance
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger # Хотя пока не используем, импортируем для FSM
from apscheduler.triggers.date import DateTrigger # Хотя пока не используем, импортируем для FSM
from apscheduler.jobstores.base import JobLookupError
# *** Важно: Импортируем экземпляр планировщика из вашего bot.py ***
# Этот импорт вызывает циклическую зависимость, поэтому удаляем его
# try:
#     from bot import scheduler
# except ImportError:
#     # Если импорт не удался (например, при запуске напрямую scheduled_handlers),
#     # нужно как-то получить доступ к планировщику. В продакшене лучше использовать DI.
#     # Для быстрой реализации, можем просто выводить ошибку, если scheduler недоступен.
#     print("WARNING: APScheduler instance 'scheduler' not found. Scheduled job execution/management may fail.", file=sys.stderr)
#     scheduler = None # Устанавливаем в None, если импорт не удался


from ..utils.rust_executor import execute_rust_command
from ..database import sqlite_db
from .. import config
from .upload_handlers import SOURCE_PARAMS_ORDER, get_friendly_param_name

# Импортируем клавиатуры
from ..keyboards.inline import (
    main_menu_keyboard,
    manage_schedules_menu_keyboard,
    select_config_keyboard,
    select_schedule_action_keyboard,
    select_schedule_trigger_type_keyboard, # Импортируем новую клавиатуру
    confirm_schedule_keyboard, # Импортируем новую клавиатуру подтверждения
)

# Создаем Router для хэндлеров UI управления расписанием
router = Router()

# Определяем FSM состояния для процесса добавления/редактирования запланированного задания
class ScheduleProcess(StatesGroup):
    # Состояния для добавления нового задания
    waiting_schedule_name = State() # Ожидание ввода имени задания
    select_schedule_action = State() # Выбор действия (extract/update)
    select_schedule_source_config = State() # Выбор сохраненной конфиги источника
    select_schedule_tt_config = State() # Выбор сохраненной конфиги True Tabs
    select_schedule_trigger_type = State() # Выбор типа триггера (interval/cron/date)
    # Состояния для ввода параметров триггера
    waiting_interval_args = State() # Ожидание параметров для IntervalTrigger - НОВОЕ
    waiting_cron_args = State() # Ожидание параметров для CronTrigger - НОВОЕ
    waiting_date_args = State() # Ожидание параметров для DateTrigger - НОВОЕ
    confirm_schedule = State() # Подтверждение создания задания - НОВОЕ
    # ... состояния для редактирования/удаления ...


import logging
from aiogram import Bot

# --- Функции выполнения запланированных задач (без изменений) ---
# async def scheduled_task_executor(bot: Bot, chat_id: int, ...): ...
# async def run_rust_task_for_scheduled_job(...): ...

async def scheduled_task_executor(
    bot: Bot,
    chat_id: int,
    source_config_name: str,
    tt_config_name: str,
    action: str,
    job_name: str,
):
    """
    Executes the scheduled task based on the action and configurations.
    """
    logging.info(f"Scheduled task '{job_name}' started for chat_id={chat_id}, action={action}")

    try:
        # Example: handle different actions
        if action == "extract":
            # Call extraction logic here, e.g., run rust task or other
            # For example, call run_rust_task_for_scheduled_job or similar
            logging.info(f"Executing extract action for job '{job_name}'")
            # Placeholder: await run_rust_task_for_scheduled_job(bot, chat_id, source_config_name, tt_config_name, job_name)
        elif action == "update":
            logging.info(f"Executing update action for job '{job_name}'")
            # Placeholder for update action logic
        else:
            logging.warning(f"Unknown action '{action}' for scheduled task '{job_name}'")

        # Notify user about task completion (optional)
        await bot.send_message(chat_id, f"Scheduled task '{job_name}' executed successfully.")

    except Exception as e:
        logging.error(f"Error executing scheduled task '{job_name}': {e}", exc_info=True)
        await bot.send_message(chat_id, f"Error executing scheduled task '{job_name}': {e}")



@router.callback_query(F.data == "manage_schedules")
async def manage_schedules_menu_handler(callback: CallbackQuery):
    """
    Handler for the "Запланированные задания" button.
    Shows the main menu for managing scheduled jobs.
    """
    await callback.message.edit_text(
        "Меню управления запланированными заданиями:",
        reply_markup=manage_schedules_menu_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data == "list_schedules")
async def list_schedules_handler(callback: CallbackQuery):
    """
    Handler to list all scheduled jobs for the user.
    """
    chat_id = callback.message.chat.id
    jobs = await sqlite_db.list_scheduled_jobs(chat_id)

    if not jobs:
        await callback.message.edit_text(
            "У вас нет запланированных заданий.",
            reply_markup=manage_schedules_menu_keyboard()
        )
        await callback.answer()
        return

    # Build a list of jobs with buttons to view details
    builder = InlineKeyboardBuilder()
    for job in jobs:
        builder.button(text=job.get('name', 'Без имени'), callback_data=f"view_schedule_details:{job.get('job_id')}")
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="manage_schedules"))
    await callback.message.edit_text(
        "Ваши запланированные задания:",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@router.callback_query(F.data.startswith("view_schedule_details:"))
async def view_schedule_details_handler(callback: CallbackQuery):
    """
    Handler to show details of a scheduled job and provide action buttons.
    """
    job_id = callback.data.split(":")[1]

    # Получаем данные задания из базы по job_id
    job = await sqlite_db.get_scheduled_job(job_id)
    if not job:
        await callback.message.edit_text(
            f"❌ Запланированное задание с ID {job_id} не найдено.",
            reply_markup=manage_schedules_menu_keyboard()
        )
        await callback.answer()
        return

    # Получаем последнюю запись истории загрузок для этого задания
    last_upload = await sqlite_db.get_last_upload_for_scheduled_job(job.get('chat_id'), job.get('action'))

    last_run_info = "Нет данных о последнем запуске."
    if last_upload:
        status = last_upload.get('status', 'Неизвестно')
        timestamp = last_upload.get('timestamp', 'Неизвестно')
        last_run_info = f"Статус последнего запуска: <b>{status}</b>\nВремя последнего запуска: <b>{timestamp}</b>"

    # Получаем время следующего запуска из APScheduler
    next_run_time_str = "Неизвестно"
    if scheduler:
        try:
            aps_job = scheduler.get_job(job.get('job_id', ''))
            if aps_job and aps_job.next_run_time:
                next_run_time_str = aps_job.next_run_time.strftime('%Y-%m-%d %H:%M:%S %Z')
            elif aps_job and not aps_job.next_run_time:
                next_run_time_str = "Нет запланированных запусков (возможно, завершено)"
            else:
                next_run_time_str = "Задание не найдено в планировщике"
        except Exception as e:
            next_run_time_str = "Ошибка получения времени"
    else:
        next_run_time_str = "Планировщик недоступен"

    # Формируем текст с деталями задания
    details_text = (
        f"<b>Детали запланированного задания:</b>\n\n"
        f"Имя: <b>{job.get('name')}</b>\n"
        f"Действие: <b>{job.get('action')}</b>\n"
        f"Источник: <b>{job.get('source_config_name')}</b>\n"
        f"True Tabs: <b>{job.get('tt_config_name')}</b>\n"
        f"Тип триггера: <b>{job.get('trigger_type')}</b>\n"
        f"Аргументы триггера: <code>{job.get('trigger_args_json')}</code>\n\n"
        f"Следующий запуск: {next_run_time_str}\n\n"
        f"{last_run_info}\n"
    )

    # Проверяем, есть ли у задания статус паузы (если есть поле is_paused)
    is_paused = job.get('is_paused', False)

    # Отправляем сообщение с деталями и клавиатурой действий
    await callback.message.edit_text(
        details_text,
        reply_markup=schedule_details_actions_keyboard(job_id, is_paused),
        parse_mode='HTML'
    )
    await callback.answer()


# --- Handlers for Adding a New Scheduled Job (FSM) ---

# Handler for "add_schedule" callback - Starts the FSM (without changes)
@router.callback_query(F.data == "add_schedule")
async def start_add_schedule_handler(callback: CallbackQuery, state: FSMContext):
    """Начинает FSM процесс добавления нового запланированного задания."""
    await state.set_state(ScheduleProcess.waiting_schedule_name)
    await callback.message.edit_text(
        "Введите уникальное имя для нового запланированного задания:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]])
    )
    await callback.answer()

# --- Handlers for Editing and Deleting Scheduled Jobs ---

# Handler to show schedule details actions keyboard with edit and delete options
def schedule_details_actions_keyboard(job_id: str, is_paused: bool) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_schedule:{job_id}")],
        [InlineKeyboardButton(text="🗑️ Удалить", callback_data=f"delete_schedule:{job_id}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="manage_schedules")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# Handler for "edit_schedule" callback - Starts FSM for editing a scheduled job
@router.callback_query(F.data.startswith("edit_schedule:"))
async def start_edit_schedule_handler(callback: CallbackQuery, state: FSMContext):
    job_id = callback.data.split(":")[1]
    job = await sqlite_db.get_scheduled_job(job_id)
    if not job:
        await callback.message.edit_text(f"❌ Запланированное задание с ID {job_id} не найдено.", reply_markup=manage_schedules_menu_keyboard())
        await callback.answer()
        return

    # Save job data in state for editing
    await state.update_data(editing_job=job)
    # Start editing by asking which field to edit
    await callback.message.edit_text(
        f"Редактирование задания: <b>{job.get('name')}</b>\nВыберите, что хотите изменить:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Имя", callback_data="edit_field:name")],
            [InlineKeyboardButton(text="Действие", callback_data="edit_field:action")],
            [InlineKeyboardButton(text="Источник", callback_data="edit_field:source_config")],
            [InlineKeyboardButton(text="True Tabs", callback_data="edit_field:tt_config")],
            [InlineKeyboardButton(text="Тип триггера", callback_data="edit_field:trigger_type")],
            [InlineKeyboardButton(text="Параметры триггера", callback_data="edit_field:trigger_args")],
            [InlineKeyboardButton(text="Включено/Отключено", callback_data="edit_field:enabled")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_edit")]
        ]),
        parse_mode='HTML'
    )
    await state.set_state(ScheduleProcess.waiting_schedule_name)  # Reuse or create a new state for editing selection
    await callback.answer()

# Handler for editing fields selection
@router.callback_query(F.data.startswith("edit_field:"), StateFilter(ScheduleProcess.waiting_schedule_name))
async def edit_field_selection_handler(callback: CallbackQuery, state: FSMContext):
    field = callback.data.split(":")[1]
    await state.update_data(edit_field=field)
    job = (await state.get_data()).get('editing_job')

    if not job:
        await callback.message.edit_text("Ошибка: данные задания не найдены. Начните заново.", reply_markup=manage_schedules_menu_keyboard())
        await state.clear()
        await callback.answer()
        return

    if field == "name":
        await callback.message.edit_text("Введите новое имя задания:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit")]]))
        await state.set_state(ScheduleProcess.waiting_schedule_name)
    elif field == "action":
        await callback.message.edit_text("Выберите новое действие:", reply_markup=select_schedule_action_keyboard())
        await state.set_state(ScheduleProcess.select_schedule_action)
    elif field == "source_config":
        source_configs = await sqlite_db.list_source_configs()
        if not source_configs:
            await callback.message.edit_text("🚫 Нет сохраненных конфигураций источников.", reply_markup=manage_schedules_menu_keyboard())
            await state.clear()
            await callback.answer()
            return
        await callback.message.edit_text("Выберите новую конфигурацию источника:", reply_markup=select_config_keyboard(source_configs, 'schedule_source_select'))
        await state.set_state(ScheduleProcess.select_schedule_source_config)
    elif field == "tt_config":
        tt_configs = await sqlite_db.list_tt_configs()
        if not tt_configs:
            await callback.message.edit_text("🚫 Нет сохраненных конфигураций True Tabs.", reply_markup=manage_schedules_menu_keyboard())
            await state.clear()
            await callback.answer()
            return
        await callback.message.edit_text("Выберите новую конфигурацию True Tabs:", reply_markup=select_config_keyboard(tt_configs, 'schedule_tt_select'))
        await state.set_state(ScheduleProcess.select_schedule_tt_config)
    elif field == "trigger_type":
        await callback.message.edit_text("Выберите новый тип триггера:", reply_markup=select_schedule_trigger_type_keyboard())
        await state.set_state(ScheduleProcess.select_schedule_trigger_type)
    elif field == "trigger_args":
        # For simplicity, ask user to re-enter trigger args depending on current trigger type
        current_trigger_type = job.get('trigger_type')
        if current_trigger_type == 'interval':
            await callback.message.edit_text("Введите новый интервал в формате: weeks=W,days=D,hours=H,minutes=M,seconds=S", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit")]]))
            await state.set_state(ScheduleProcess.waiting_interval_args)
        elif current_trigger_type == 'cron':
            await callback.message.edit_text("Введите новое Cron выражение (например, `0 * * * *`):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit")]]))
            await state.set_state(ScheduleProcess.waiting_cron_args)
        elif current_trigger_type == 'date':
            await callback.message.edit_text("Введите дату и время в формате `YYYY-MM-DD HH:MM:SS`:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit")]]))
            await state.set_state(ScheduleProcess.waiting_date_args)
        else:
            await callback.message.edit_text("Неизвестный тип триггера. Отмена.", reply_markup=manage_schedules_menu_keyboard())
            await state.clear()
    elif field == "enabled":
        # Toggle enabled status
        current_enabled = job.get('enabled', True)
        new_enabled = not current_enabled
        await state.update_data(schedule_enabled=new_enabled)
        await callback.message.edit_text(f"Статус задания изменен на: {'Включено' if new_enabled else 'Отключено'}. Подтвердите изменения?", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Подтвердить", callback_data="confirm_edit_schedule")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_edit")]
        ]))
        await state.set_state(ScheduleProcess.confirm_schedule)
    else:
        await callback.message.edit_text("Неизвестное поле для редактирования.", reply_markup=manage_schedules_menu_keyboard())
        await state.clear()
    await callback.answer()

# Handler for confirming edited schedule
@router.callback_query(F.data == "confirm_edit_schedule", StateFilter(ScheduleProcess.confirm_schedule))
async def confirm_edit_schedule_handler(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = data.get('editing_job')
    if not job:
        await callback.message.edit_text("Ошибка: данные задания не найдены. Начните заново.", reply_markup=manage_schedules_menu_keyboard())
        await state.clear()
        await callback.answer()
        return

    # Prepare updated fields
    updated_name = data.get('schedule_name', job.get('name'))
    updated_action = data.get('schedule_action', job.get('action'))
    updated_source_config_name = data.get('schedule_source_config_name', job.get('source_config_name'))
    updated_tt_config_name = data.get('schedule_tt_config_name', job.get('tt_config_name'))
    updated_trigger_type = data.get('schedule_trigger_type', job.get('trigger_type'))
    updated_trigger_args = data.get('schedule_trigger_args', json.loads(job.get('trigger_args_json')))
    updated_enabled = data.get('schedule_enabled', job.get('enabled', True))

    # Validate updated name uniqueness if changed
    if updated_name != job.get('name'):
        existing_jobs = await sqlite_db.list_scheduled_jobs(job.get('chat_id'))
        if any(j.get('name') == updated_name for j in existing_jobs):
            await callback.message.edit_text(f"Задание с именем '{updated_name}' уже существует. Пожалуйста, выберите другое имя.", reply_markup=manage_schedules_menu_keyboard())
            await state.clear()
            await callback.answer()
            return

    # Update DB
    success = await sqlite_db.update_scheduled_job(
        job_id=job.get('job_id'),
        name=updated_name,
        chat_id=job.get('chat_id'),
        source_config_name=updated_source_config_name,
        tt_config_name=updated_tt_config_name,
        action=updated_action,
        trigger_type=updated_trigger_type,
        trigger_args_json=json.dumps(updated_trigger_args),
        enabled=updated_enabled
    )

    if not success:
        await callback.message.edit_text("Ошибка при обновлении задания в базе данных.", reply_markup=manage_schedules_menu_keyboard())
        await state.clear()
        await callback.answer()
        return

    # Remove old job from scheduler and add updated job
    try:
        scheduler.remove_job(job.get('job_id'))
    except Exception:
        pass

    # Create new trigger object
    trigger = None
    try:
        if updated_trigger_type == 'interval':
            trigger = IntervalTrigger(**updated_trigger_args)
        elif updated_trigger_type == 'cron':
            cron_expression = updated_trigger_args.get('cron_expression')
            trigger = CronTrigger.from_crontab(cron_expression)
        elif updated_trigger_type == 'date':
            run_date_str = updated_trigger_args.get('run_date')
            run_date = datetime.fromisoformat(run_date_str)
            trigger = DateTrigger(run_date=run_date)
    except Exception as e:
        await callback.message.edit_text(f"Ошибка при создании триггера: {e}", reply_markup=manage_schedules_menu_keyboard())
        await state.clear()
        await callback.answer()
        return

    # Add updated job to scheduler
    try:
        scheduler.add_job(
            scheduled_task_executor,
            trigger=trigger,
            id=job.get('job_id'),
            name=updated_name,
            kwargs={
                'bot': callback.bot,
                'chat_id': job.get('chat_id'),
                'source_config_name': updated_source_config_name,
                'tt_config_name': updated_tt_config_name,
                'action': updated_action,
                'job_name': updated_name
            }
        )
    except Exception as e:
        await callback.message.edit_text(f"Ошибка при добавлении обновленного задания в планировщик: {e}", reply_markup=manage_schedules_menu_keyboard())
        await state.clear()
        await callback.answer()
        return

    await callback.message.edit_text(f"✅ Задание '{updated_name}' успешно обновлено.", reply_markup=manage_schedules_menu_keyboard())
    await state.clear()
    await callback.answer()

# Handler for "delete_schedule" callback - Asks for confirmation before deleting
@router.callback_query(F.data.startswith("delete_schedule:"))
async def delete_schedule_handler(callback: CallbackQuery, state: FSMContext):
    job_id = callback.data.split(":")[1]
    job = await sqlite_db.get_scheduled_job(job_id)
    if not job:
        await callback.message.edit_text(f"❌ Запланированное задание с ID {job_id} не найдено.", reply_markup=manage_schedules_menu_keyboard())
        await callback.answer()
        return

    await state.update_data(deleting_job=job)
    await callback.message.edit_text(
        f"Вы уверены, что хотите удалить задание: <b>{job.get('name')}</b>?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Да, удалить", callback_data="confirm_delete_schedule")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_delete")]
        ]),
        parse_mode='HTML'
    )
    await callback.answer()

# Handler for confirming deletion
@router.callback_query(F.data == "confirm_delete_schedule")
async def confirm_delete_schedule_handler(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = data.get('deleting_job')
    if not job:
        await callback.message.edit_text("Ошибка: данные задания не найдены. Начните заново.", reply_markup=manage_schedules_menu_keyboard())
        await state.clear()
        await callback.answer()
        return

    # Delete from DB
    success = await sqlite_db.delete_scheduled_job(job.get('job_id'))
    if not success:
        await callback.message.edit_text("Ошибка при удалении задания из базы данных.", reply_markup=manage_schedules_menu_keyboard())
        await state.clear()
        await callback.answer()
        return

    # Remove from scheduler
    try:
        scheduler.remove_job(job.get('job_id'))
    except Exception:
        pass

    await callback.message.edit_text(f"✅ Задание '{job.get('name')}' успешно удалено.", reply_markup=manage_schedules_menu_keyboard())
    await state.clear()
    await callback.answer()

# Handler for canceling edit or delete
@router.callback_query(F.data.in_({"cancel_edit", "cancel_delete", "cancel"}))
async def cancel_edit_delete_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Операция отменена.", reply_markup=manage_schedules_menu_keyboard())
    await callback.answer()

# Handler for waiting_schedule_name state - Processes the job name (without changes)
@router.message(ScheduleProcess.waiting_schedule_name)
async def process_schedule_name(message: Message, state: FSMContext):
    """Обрабатывает введенное имя для запланированного задания."""
    job_name = message.text.strip()
    if not job_name:
        await message.answer("Имя задания не может быть пустым. Введите уникальное имя:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]) )
        return

    chat_id = message.chat.id
    existing_jobs = await sqlite_db.list_scheduled_jobs(chat_id)
    if any(job.get('name') == job_name for job in existing_jobs):
        await message.answer(f"Запланированное задание с именем '{job_name}' уже существует у вас. Введите другое уникальное имя:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]))
        return

    await state.update_data(schedule_name=job_name, chat_id=chat_id)

    await state.set_state(ScheduleProcess.select_schedule_action)
    await message.answer(
        f"Имя задания: <b>{job_name}</b>\nВыберите действие для выполнения:",
        reply_markup=select_schedule_action_keyboard(),
        parse_mode='HTML'
    )

# Handler for select_schedule_action state - Processes the selected action (without changes)
@router.callback_query(F.data.startswith("select_schedule_action:"), ScheduleProcess.select_schedule_action)
async def select_schedule_action_handler(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбранное действие для запланированного задания."""
    action = callback.data.split(":")[1]

    await state.update_data(schedule_action=action)

    await state.set_state(ScheduleProcess.select_schedule_source_config)
    source_configs = await sqlite_db.list_source_configs()

    if not source_configs:
        await callback.message.edit_text(
            "🚫 У вас нет сохраненных конфигураций источников. Невозможно создать запланированное задание, требующее источник.",
            reply_markup=manage_schedules_menu_keyboard()
        )
        await state.clear()
    else:
        state_data = await state.get_data() # Получаем данные еще раз, т.к. они могли обновиться
        schedule_name = state_data.get('schedule_name', 'Без имени')
        await callback.message.edit_text(
            f"Задание: <b>{schedule_name}</b> ({action})\nВыберите сохраненную конфигурацию источника:",
            reply_markup=select_config_keyboard(source_configs, 'schedule_source_select'),
            parse_mode='HTML'
        )

    await callback.answer()

# Handler for select_schedule_source_config state - Processes the selected source config (without changes)
@router.callback_query(F.data.startswith("select_config:schedule_source_select:"), ScheduleProcess.select_schedule_source_config)
async def process_schedule_source_config_selection(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбранную конфигурацию источника для запланированного задания."""
    source_config_name = callback.data.split(":")[2]

    await state.update_data(schedule_source_config_name=source_config_name)

    await state.set_state(ScheduleProcess.select_schedule_tt_config)
    tt_configs = await sqlite_db.list_tt_configs()

    if not tt_configs:
        await callback.message.edit_text(
            "🚫 У вас нет сохраненных конфигураций True Tabs. Невозможно создать запланированное задание.",
            reply_markup=manage_schedules_menu_keyboard()
        )
        await state.clear()
    else:
        state_data = await state.get_data()
        schedule_name = state_data.get('schedule_name', 'Без имени')
        schedule_action = state_data.get('schedule_action', 'Без действия')
        schedule_source_config_name = state_data.get('schedule_source_config_name', 'Без источника')
        schedule_tt_config_name = state_data.get('schedule_tt_config_name', 'Без TT')

        await callback.message.edit_text(
            f"Задание: <b>{schedule_name}</b> ({schedule_action})\nИсточник: <b>{source_config_name}</b>\nВыберите сохраненную конфигурацию True Tabs:",
            reply_markup=select_config_keyboard(tt_configs, 'schedule_tt_select'),
            parse_mode='HTML'
        )

    await callback.answer()

# Handler for select_schedule_tt_config state - Processes the selected TT config (Updates transition)
@router.callback_query(F.data.startswith("select_config:schedule_tt_select:"), ScheduleProcess.select_schedule_tt_config)
async def process_schedule_tt_config_selection(callback: CallbackQuery, state: FSMContext):
    """
    Processes the selected True Tabs configuration and transitions to selecting the trigger type.
    """
    tt_config_name = callback.data.split(":")[2]

    await state.update_data(schedule_tt_config_name=tt_config_name)

    # --- Transition to selecting the schedule trigger type ---
    await state.set_state(ScheduleProcess.select_schedule_trigger_type)

    state_data = await state.get_data()
    schedule_name = state_data.get('schedule_name', 'Без имени')
    schedule_action = state_data.get('schedule_action', 'Без действия')
    schedule_source_config_name = state_data.get('schedule_source_config_name', 'Без источника')
    schedule_tt_config_name = state_data.get('schedule_tt_config_name', 'Без TT')

    await callback.message.edit_text(
        f"Задание: <b>{schedule_name}</b> ({schedule_action})\n"
        f"Источник: <b>{schedule_source_config_name}</b>\nTrue Tabs: <b>{schedule_tt_config_name}</b>\n"
        f"Теперь выберите тип расписания (триггер):",
        reply_markup=select_schedule_trigger_type_keyboard(), # Используем новую клавиатуру
        parse_mode='HTML'
    )

    await callback.answer()

# Handler for select_schedule_trigger_type state - Processes the selected trigger type
@router.callback_query(F.data.startswith("select_trigger_type:"), ScheduleProcess.select_schedule_trigger_type)
async def select_schedule_trigger_type_handler(callback: CallbackQuery, state: FSMContext):
    """
    Processes the selected trigger type and prompts for trigger arguments.
    """
    trigger_type = callback.data.split(":")[1] # 'interval', 'cron', or 'date'

    # Сохраняем выбранный тип триггера
    await state.update_data(schedule_trigger_type=trigger_type)

    state_data = await state.get_data()
    schedule_name = state_data.get('schedule_name', 'Без имени')
    schedule_action = state_data.get('schedule_action', 'Без действия')
    schedule_source_config_name = state_data.get('schedule_source_config_name', 'Без источника')
    schedule_tt_config_name = state_data.get('schedule_tt_config_name', 'Без TT')

    # В зависимости от типа триггера, переходим в соответствующее состояние для ввода аргументов
    if trigger_type == 'interval':
        await state.set_state(ScheduleProcess.waiting_interval_args)
        await callback.message.edit_text(
            f"Задание: <b>{schedule_name}</b> ({schedule_action})\n"
            f"Источник: <b>{schedule_source_config_name}</b>\nTrue Tabs: <b>{schedule_tt_config_name}</b>\n"
            f"Тип расписания: <b>По интервалу</b>\n\n"
            f"Введите интервал в формате: `weeks=W,days=D,hours=H,minutes=M,seconds=S`\n"
            f"Например: `weeks=1,minutes=30` (каждую неделю и 30 минут)\n"
            f"Допускаются только числовые значения. Можно указать несколько частей через запятую.\n\n"
            f"Введите интервал:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]),
            parse_mode='HTML'
        )
    elif trigger_type == 'cron':
        await state.set_state(ScheduleProcess.waiting_cron_args)
        await callback.message.edit_text(
            f"Задание: <b>{schedule_name}</b> ({schedule_action})\n"
            f"Источник: <b>{schedule_source_config_name}</b>\nTrue Tabs: <b>{schedule_tt_config_name}</b>\n"
            f"Тип расписания: <b>Cron</b>\n\n"
            f"Введите Cron выражение (например, `0 * * * *` для каждый час):",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]])
            , parse_mode='HTML'
        )
    elif trigger_type == 'date':
        await state.set_state(ScheduleProcess.waiting_date_args)
        await callback.message.edit_text(
            f"Задание: <b>{schedule_name}</b> ({schedule_action})\n"
            f"Источник: <b>{schedule_source_config_name}</b>\nTrue Tabs: <b>{schedule_tt_config_name}</b>\n"
            f"Тип расписания: <b>Одноразовое (Date)</b>\n\n"
            f"Введите дату и время в формате `YYYY-MM-DD HH:MM:SS` (например, `2023-12-31 23:59:59`):",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]])
            , parse_mode='HTML'
        )

    else:
        error_msg = f"Неизвестный тип триггера: {trigger_type}"
        print(error_msg, file=sys.stderr)
        await callback.message.edit_text(error_msg, reply_markup=manage_schedules_menu_keyboard())
        await state.clear()

    await callback.answer()

# Handler for waiting_interval_args state - Processes interval arguments
@router.message(ScheduleProcess.waiting_interval_args)
async def process_interval_args(message: Message, state: FSMContext):
    """
    Processes the entered interval arguments and transitions to confirmation.
    Expects input like "weeks=W,days=D,...".
    """
    interval_input = message.text.strip()
    interval_args: Dict[str, int] = {}
    validation_error = None

    # Парсим строку ввода (например, "weeks=1,minutes=30")
    parts = interval_input.split(',')
    if not parts:
        validation_error = "Неверный формат. Введите интервал в формате `key=value` через запятую."
    else:
        for part in parts:
            try:
                key_value = part.split('=')
                if len(key_value) != 2:
                    validation_error = "Неверный формат части интервала. Ожидается `key=value`."
                    break
                key = key_value[0].strip().lower()
                value_str = key_value[1].strip()

                # Проверяем, что ключ является допустимым параметром интервала APScheduler
                if key not in ['weeks', 'days', 'hours', 'minutes', 'seconds']:
                    validation_error = f"Неизвестный параметр интервала: `{key}`. Допустимые: weeks, days, hours, minutes, seconds."
                    break

                # Проверяем, что значение является целым положительным числом
                value = int(value_str)
                if value < 0:
                    validation_error = f"Значение параметра `{key}` не может быть отрицательным."
                    break

                interval_args[key] = value

            except ValueError:
                validation_error = f"Неверное значение параметра `{key_value[0].strip()}`. Ожидается целое число."
                break
            except Exception:
                validation_error = "Неверный формат ввода интервала."
                break

    # Проверяем, что были указаны хоть какие-то параметры интервала и хотя бы один параметр больше нуля
    if not interval_args or all(value == 0 for value in interval_args.values()):
         validation_error = "Не указаны параметры интервала или все значения равны нулю. Пожалуйста, введите корректный интервал (например, `minutes=5`)."


    if validation_error:
        await message.answer(f"Ошибка валидации интервала: {validation_error}\nПожалуйста, введите интервал снова:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]))
        return

    # Сохраняем распарсенные аргументы интервала (как словарь) в данные состояния
    await state.update_data(schedule_trigger_args=interval_args)

    # --- Переходим к состоянию подтверждения ---
    await state.set_state(ScheduleProcess.confirm_schedule)

    # Отображаем сообщение для подтверждения со всеми собранными деталями
    state_data = await state.get_data()
    schedule_name = state_data.get('schedule_name', 'Без имени')
    schedule_action = state_data.get('schedule_action', 'Без действия')
    schedule_source_config_name = state_data.get('schedule_source_config_name', 'Без источника')
    schedule_tt_config_name = state_data.get('schedule_tt_config_name', 'Без TT')
    schedule_trigger_type = state_data.get('schedule_trigger_type', 'Без триггера')
    schedule_trigger_args = state_data.get('schedule_trigger_args', {}) # Получаем распарсенные аргументы

    # Форматируем аргументы интервала для отображения в сообщении
    formatted_interval_args = ", ".join([f"{key}={value}" for key, value in schedule_trigger_args.items()])


    confirm_text = (
        f"<b>Подтверждение создания запланированного задания:</b>\n\n"
        f"Имя: <b>{schedule_name}</b>\n"
        f"Действие: <b>{schedule_action}</b>\n"
        f"Источник: <b>{schedule_source_config_name}</b>\n"
        f"True Tabs: <b>{schedule_tt_config_name}</b>\n"
        f"Тип расписания: <b>{schedule_trigger_type.capitalize()}</b>\n"
        f"Параметры триггера: <code>{formatted_interval_args}</code>\n\n" # Отображаем отформатированные параметры
        f"Все верно? Нажмите 'Подтвердить и создать' или 'Отмена'."
    )

    await message.answer(
        confirm_text,
        reply_markup=confirm_schedule_keyboard(), # Используем клавиатуру подтверждения
        parse_mode='HTML'
    )

# Handler for waiting_cron_args state - Processes cron expression input
@router.message(ScheduleProcess.waiting_cron_args)
async def process_cron_args(message: Message, state: FSMContext):
    """
    Processes the entered cron expression and transitions to confirmation.
    """
    cron_expression = message.text.strip()

    # Basic validation of cron expression (very simple, can be improved)
    parts = cron_expression.split()
    if len(parts) != 5:
        await message.answer("Неверный формат Cron выражения. Ожидается 5 частей (минуты, часы, день месяца, месяц, день недели). Попробуйте снова:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]))
        return

    # Save cron expression in trigger args as dict with key 'cron_expression'
    await state.update_data(schedule_trigger_args={'cron_expression': cron_expression})

    #
    state_data = await state.get_data()
    schedule_name = state_data.get('schedule_name', 'Без имени')
    schedule_action = state_data.get('schedule_action', 'Без действия')
    schedule_source_config_name = state_data.get('schedule_source_config_name', 'Без источника')
    schedule_tt_config_name = state_data.get('schedule_tt_config_name', 'Без TT')
    schedule_trigger_type = state_data.get('schedule_trigger_type', 'Без триггера')
    schedule_trigger_args = state_data.get('schedule_trigger_args', {})

    confirm_text = (
        f"<b>Подтверждение создания запланированного задания:</b>\n\n"
        f"Имя: <b>{schedule_name}</b>\n"
        f"Действие: <b>{schedule_action}</b>\n"
        f"Источник: <b>{schedule_source_config_name}</b>\n"
        f"True Tabs: <b>{schedule_tt_config_name}</b>\n"
        f"Тип расписания: <b>{schedule_trigger_type.capitalize()}</b>\n"
        f"Параметры триггера: <code>{schedule_trigger_args.get('cron_expression')}</code>\n\n"
        f"Все верно? Нажмите 'Подтвердить и создать' или 'Отмена'."
    )

    await message.answer(confirm_text, reply_markup=confirm_schedule_keyboard(), parse_mode='HTML')

# Handler for waiting_date_args state - Processes date and time input
@router.message(ScheduleProcess.waiting_date_args)
async def process_date_args(message: Message, state: FSMContext):
    """
    Processes the entered date and time and transitions to confirmation.
    """
    date_str = message.text.strip()

    try:
        # Parse date string to datetime object
        run_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        if run_date < datetime.now():
            await message.answer("Дата и время не могут быть в прошлом. Введите корректную дату и время:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]))
            return
    except ValueError:
        await message.answer("Неверный формат даты и времени. Ожидается формат `YYYY-MM-DD HH:MM:SS`. Попробуйте снова:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]))
        return

    # Save run_date as ISO format string in trigger args
    await state.update_data(schedule_trigger_args={'run_date': run_date.isoformat()})

    # Transition to confirmation state
    await state.set_state(ScheduleProcess.confirm_schedule)

    # Show confirmation message
    state_data = await state.get_data()
    schedule_name = state_data.get('schedule_name', 'Без имени')
    schedule_action = state_data.get('schedule_action', 'Без действия')
    schedule_source_config_name = state_data.get('schedule_source_config_name', 'Без источника')
    schedule_tt_config_name = state_data.get('schedule_tt_config_name', 'Без TT')
    schedule_trigger_type = state_data.get('schedule_trigger_type', 'Без триггера')
    schedule_trigger_args = state_data.get('schedule_trigger_args', {})

    confirm_text = (
        f"<b>Подтверждение создания запланированного задания:</b>\n\n"
        f"Имя: <b>{schedule_name}</b>\n"
        f"Действие: <b>{schedule_action}</b>\n"
        f"Источник: <b>{schedule_source_config_name}</b>\n"
        f"True Tabs: <b>{schedule_tt_config_name}</b>\n"
        f"Тип расписания: <b>{schedule_trigger_type.capitalize()}</b>\n"
        f"Параметры триггера: <code>{schedule_trigger_args.get('run_date')}</code>\n\n"
        f"Все верно? Нажмите 'Подтвердить и создать' или 'Отмена'."
    )

    await message.answer(confirm_text, reply_markup=confirm_schedule_keyboard(), parse_mode='HTML')

# Handler for confirm_schedule state - Processes the confirmation
@router.callback_query(F.data == "confirm_create_schedule", ScheduleProcess.confirm_schedule)
async def confirm_schedule_handler(callback: CallbackQuery, state: FSMContext):
    global scheduler  # moved global declaration to be the first statement in the function

    """
    Handles the confirmation to create the scheduled job.
    Saves to DB and adds to APScheduler.
    """

    # Проверяем, что планировщик инициализирован
    if scheduler is None:
        error_msg = "Ошибка: Планировщик недоступен. Перезапустите бота."
        print(error_msg, file=sys.stderr)
        await callback.message.edit_text(error_msg, reply_markup=main_menu_keyboard())
        await state.clear()
        await callback.answer("Ошибка планировщика.")
        return

    # Получаем все собранные данные из FSM состояния
    state_data = await state.get_data()
    job_name = state_data.get('schedule_name')
    chat_id = state_data.get('chat_id')
    source_config_name = state_data.get('schedule_source_config_name')
    tt_config_name = state_data.get('schedule_tt_config_name')
    action = state_data.get('schedule_action')
    trigger_type = state_data.get('schedule_trigger_type')
    trigger_args = state_data.get('schedule_trigger_args') # Распарсенные аргументы (словарь)

    # Валидация: убедимся, что все необходимые данные собраны
    # trigger_args может быть пустым словарем для некоторых триггеров, поэтому проверяем не None
    if not all([job_name, chat_id, source_config_name, tt_config_name, action, trigger_type, trigger_args is not None]):
        error_msg = "Ошибка: Недостаточно данных для создания задания. Пожалуйста, начните заново."
        print(f"Неполные данные в состоянии для создания задания: {state_data}", file=sys.stderr)
        await callback.message.edit_text(error_msg, reply_markup=manage_schedules_menu_keyboard())
        await state.clear()
        await callback.answer("Недостаточно данных.")
        return

    # Генерируем уникальный ID задания для APScheduler и базы данных
    job_id = str(uuid.uuid4())

    # Создаем объект триггера APScheduler из собранных данных
    trigger = None
    try:
        if trigger_type == 'interval':
             # Создаем IntervalTrigger из словаря аргументов
             trigger = IntervalTrigger(**trigger_args)
        elif trigger_type == 'cron':
             # Аргументы для CronTrigger будут строкой (cron expression)
             cron_expression = trigger_args.get('cron_expression') # Предполагаем такой ключ
             if not cron_expression: raise ValueError("Cron expression is missing.")
             trigger = CronTrigger.from_crontab(cron_expression) # Или CronTrigger(**args)
        elif trigger_type == 'date':
             # Аргументы для DateTrigger будут дата и время (например, строка в ISO формате)
             run_date_str = trigger_args.get('run_date') # Предполагаем такой ключ
             if not run_date_str: raise ValueError("Run date is missing.")
             run_date = datetime.fromisoformat(run_date_str) # Парсим строку даты
             trigger = DateTrigger(run_date=run_date)

        if trigger is None:
             # Если тип триггера неизвестен или логика создания отсутствует
             raise ValueError(f"Неподдерживаемый или неполностью реализованный тип триггера: {trigger_type}")

    except Exception as e:
        # Обработка ошибок при создании объекта триггера (например, неверные аргументы)
        error_msg = f"Ошибка при создании расписания для задания '{job_name}': {e}. Задание не создано."
        print(error_msg, file=sys.stderr)
        await callback.message.edit_text(error_msg, reply_markup=manage_schedules_menu_keyboard())
        await state.clear()
        await callback.answer("Ошибка создания триггера.")
        return


    # Сохраняем запланированное задание в базу данных
    # Аргументы триггера сохраняем как JSON строку
    trigger_args_json_str = json.dumps(trigger_args)
    db_success = await sqlite_db.add_scheduled_job(
        job_id=job_id, # Уникальный ID задания
        name=job_name, # Имя задания
        chat_id=chat_id, # ID чата пользователя
        source_config_name=source_config_name, # Имя конфига источника
        tt_config_name=tt_config_name, # Имя конфига TT
        action=action, # Действие
        trigger_type=trigger_type, # Тип триггера
        trigger_args_json=trigger_args_json_str # Аргументы триггера как JSON строка
    )

    # Проверяем успешность сохранения в БД
    if not db_success:
        error_msg = f"Ошибка при сохранении задания '{job_name}' в базу данных. Возможно, имя задания уже занято (хотя мы проверяли ранее)."
        print(error_msg, file=sys.stderr)
        await callback.message.edit_text(error_msg, reply_markup=manage_schedules_menu_keyboard())
        await state.clear()
        await callback.answer("Ошибка сохранения в БД.")
        return

    # Добавляем запланированное задание в экземпляр планировщика APScheduler
    try:
        scheduler.add_job(
            scheduled_task_executor, # Функция, которую вызовет планировщик при срабатывании триггера
            trigger=trigger, # Созданный объект триггера
            id=job_id, # Используем тот же уникальный ID задания, что и в БД
            name=job_name, # Имя задания
            kwargs={ # Аргументы, которые будут переданы в scheduled_task_executor при вызове
                'bot': callback.bot, # Передаем экземпляр бота из callback_data!
                'chat_id': chat_id,
                'source_config_name': source_config_name,
                'tt_config_name': tt_config_name,
                'action': action,
                'job_name': job_name # Передаем имя задания для удобства в scheduled_task_executor
            },
            # replace_existing=False по умолчанию, что подходит для добавления нового уникального задания
        )
        logging.info(f"Запланированное задание '{job_name}' (ID: {job_id}) успешно добавлено в планировщик.")

        # Отправляем пользователю сообщение об успешном создании задания
        await callback.message.edit_text(
            f"✅ Запланированное задание '{job_name}' успешно создано и добавлено в расписание!",
            reply_markup=manage_schedules_menu_keyboard() # Возвращаемся в меню управления расписанием
        )
        # Очищаем FSM состояние после успешного создания задания
        await state.clear()
        await callback.answer("Задание успешно создано.")

    except Exception as e:
        # Обработка ошибок, если не удалось добавить задание в планировщик (например, проблема с хранилищем или триггером)
        print(f"Ошибка при добавлении задания '{job_name}' в планировщик: {e}", file=sys.stderr)
        # Если не удалось добавить в планировщик, нужно попытаться удалить запись из БД,
        # чтобы не было "фантомных" заданий, которые есть в БД, но не в расписании.
        db_delete_success = await sqlite_db.delete_scheduled_job(job_id)
        if db_delete_success:
            print(f"Задание '{job_name}' (ID: {job_id}) удалено из БД после ошибки добавления в планировщик.", file=sys.stderr)
            error_msg = f"Ошибка при добавлении задания '{job_name}' в расписание: {e}. Оно было удалено из базы данных для избежания расхождений."
        else:
             error_msg = f"Ошибка при добавлении задания '{job_name}' в расписание: {e}. Также произошла ошибка при попытке удалить запись из базы данных. Возможны расхождения."
             print(f"Критическая ошибка: Ошибка удаления задания '{job_name}' (ID: {job_id}) из БД после ошибки добавления в планировщик.", file=sys.stderr)

        # Отправляем сообщение об ошибке пользователю
        await callback.message.edit_text(error_msg, reply_markup=manage_schedules_menu_keyboard())
        await state.clear() # Очищаем FSM состояние
        await callback.answer("Произошла ошибка при добавлении в расписание.")