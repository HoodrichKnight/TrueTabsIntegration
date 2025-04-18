from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery
from keyboards.inline import main_menu_keyboard

router = Router()

@router.message(CommandStart())
async def handle_start(message: Message):
    await message.answer(
        "Привет! Я бот для извлечения и загрузки данных. Выберите действие:",
        reply_markup=main_menu_keyboard()
    )

@router.callback_query(F.data == "main_menu")
async def handle_main_menu_callback(callback: CallbackQuery):
    await callback.message.edit_text(
         "Выберите действие:",
        reply_markup=main_menu_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data == "cancel")
async def handle_cancel_callback(callback: CallbackQuery):
    await callback.message.edit_text(
        "Операция отменена. Выберите действие:",
        reply_markup=main_menu_keyboard()
    )
    await callback.answer()