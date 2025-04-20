from aiogram import Router, F
from aiogram.types import CallbackQuery
from telegram_bot.keyboards import source_selection_keyboard, main_menu_keyboard
from aiogram.fsm.context import FSMContext
from telegram_bot.handlers.upload_handlers import start_upload_process

router = Router()

@router.callback_query(F.data == "select_source")
async def handle_select_source_callback(callback: CallbackQuery):
    await callback.message.edit_text(
        "Выберите источник данных:",
        reply_markup=source_selection_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data.startswith("start_upload_process:"))
async def handle_source_selected_initiate_fsm(callback: CallbackQuery, state: FSMContext):
    await start_upload_process(callback, state)

@router.callback_query(F.data == "main_menu")
async def handle_back_to_main_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
         "Выберите действие:",
        reply_markup=main_menu_keyboard()
    )
    await callback.answer()