import asyncio
import logging
import sys
from typing import Union
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart, StateFilter
from aiogram.fsm.context import FSMContext # Импортируем для очистки состояния при отмене

from ..keyboards.inline import main_menu_keyboard # Импортируем клавиатуру главного меню
# from .upload_handlers import UploadProcess # Импортируем классы состояний для очистки при отмене
# from .config_handlers import ConfigProcess
# from .scheduled_handlers import ScheduleProcess # Импортируем классы состояний для очистки при отмене


logger = logging.getLogger(__name__) # Логгер для start_handlers

router = Router()

# Хэндлер на команду /start
@router.message(CommandStart())
async def command_start_handler(message: Message, state: FSMContext) -> None:
    """
    Обрабатывает команду /start. Приветствует пользователя и показывает главное меню.
    Также очищает текущее состояние FSM, если оно есть.
    """
    await state.clear() # Очищаем любое текущее состояние FSM при старте

    welcome_text = (
        f"Привет, {message.from_user.full_name}! 👋\n"
        f"Я бот для автоматизации загрузки данных в True Tabs из различных источников с помощью Rust утилиты.\n\n"
        f"Выберите действие:"
    )
    await message.answer(welcome_text, reply_markup=main_menu_keyboard())
    logger.info(f"Received /start command from user {message.from_user.id}")

# Общий хэндлер для кнопки "❌ Отмена" или callback "cancel"
# Он должен быть определен на уровне диспетчера или в роутере, который включен в диспетчер
# и срабатывает для всех состояний или без состояний, кроме специфических.
# В данном случае, добавим его в start_handlers, т.к. он связан с начальным меню и отменой.
@router.callback_query(F.data == "main_menu")
async def back_to_main_menu_handler(callback: CallbackQuery, state: FSMContext):
     """
     Обрабатывает callback "main_menu". Возвращает пользователя в главное меню и очищает состояние.
     """
     await state.clear() # Очищаем любое текущее состояние при возврате в главное меню
     await callback.message.edit_text("Выберите действие:", reply_markup=main_menu_keyboard())
     await callback.answer() # Отвечаем на callback, чтобы убрать "часики"


# Общий хэндлер для callback "cancel" во время любого FSM процесса
# Этот хэндлер должен иметь низкий приоритет или быть зарегистрирован так, чтобы не перекрывать
# специфические хэндлеры в FSM состояниях, если "cancel" используется в них для других целей.
# Однако, если "cancel" всегда означает отмену текущего FSM, этот подход работает.
@router.callback_query(F.data == "cancel") # Сработает на callback_data == "cancel"
@router.message(F.text.lower() == "отмена", StateFilter("*")) # Сработает на текст "отмена" в любом состоянии
async def cancel_fsm_process(callback_or_message: Union[Message, CallbackQuery], state: FSMContext):
    """
    Обрабатывает команду или callback для отмены текущего FSM процесса.
    Возвращает пользователя в главное меню.
    """
    current_state = await state.get_state()
    if current_state is None:
        # Если состояния нет, просто показываем главное меню (для текстовой команды)
        if isinstance(callback_or_message, Message):
             await callback_or_message.answer("Нет активной операции для отмены.", reply_markup=main_menu_keyboard())
        else: # Если это CallbackQuery без состояния, просто отвечаем и показываем меню
             await callback_or_message.message.edit_text("Нет активной операции для отмены.", reply_markup=main_menu_keyboard())
             await callback_or_message.answer() # Отвечаем на callback

        return # Выходим, если состояния не было

    logger.info(f"Cancelling FSM process. User ID: {callback_or_message.from_user.id}, State: {current_state}")

    await state.clear() # Очищаем все данные и состояние FSM

    message_text = "Операция отменена."
    reply_markup = main_menu_keyboard()

    if isinstance(callback_or_message, CallbackQuery):
        try:
            # Пытаемся отредактировать сообщение callback'а, если это возможно
            await callback_or_message.message.edit_text(message_text, reply_markup=reply_markup)
        except TelegramAPIError:
             # Если сообщение слишком старое для редактирования, отправляем новое
             await callback_or_message.message.answer(message_text, reply_markup=reply_markup)

        await callback_or_message.answer("Отменено") # Отвечаем на callback

    else: # Это текстовое сообщение "отмена"
        await callback_or_message.answer(message_text, reply_markup=reply_markup)

@router.message(StateFilter("*"))
async def handle_unexpected_message(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state:
        await message.answer(f"Бот ожидает другой ввод в текущем состоянии ({current_state}).\n"
                             "Для отмены текущей операции нажмите '❌ Отмена'.")
    else:
        # Если состояния нет, это просто обычное сообщение, которое бот не понимает
        await message.answer("Извините, я не понял вашу команду. Используйте меню.")
