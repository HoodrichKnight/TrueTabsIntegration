from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery
from ..keyboards.inline import main_menu_keyboard
from .config_handlers import manage_configs_menu
from aiogram.filters.state import StateFilter



router = Router()

@router.message(CommandStart())
async def handle_start(message: Message):
    await message.answer(
        "Привет! Я бот для извлечения и загрузки данных. Выберите действие:",
        reply_markup=main_menu_keyboard()
    )

@router.callback_query(F.data == "main_menu", StateFilter(None))
async def handle_main_menu_callback(callback: CallbackQuery):
    await callback.message.edit_text(
         "Выберите действие:",
        reply_markup=main_menu_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data == "main_menu", ~StateFilter(None))
async def handle_back_to_main_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()

    await callback.answer()

    await callback.message.answer(
         "Выберите действие:",
        reply_markup=main_menu_keyboard()
    )


@router.callback_query(F.data == "manage_configs")
async def handle_manage_configs_button(callback: CallbackQuery):
    await manage_configs_menu(callback)