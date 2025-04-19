import asyncio
import logging
import sys
import os

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

from handlers import main_router
from config import BOT_TOKEN, TEMP_FILES_DIR
from database.sqlite_db import init_db

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)

storage = MemoryStorage() # Используем хранилище состояний в памяти

dp = Dispatcher(storage=storage) # Передаем хранилище диспетчеру

dp.include_router(main_router)

async def main() -> None:
    await init_db()
    print("База данных SQLite инициализирована.")

    if not os.path.exists(TEMP_FILES_DIR):
        os.makedirs(TEMP_FILES_DIR, exist_ok=True) # Используем exist_ok=True
    print(f"Временная папка для файлов: {TEMP_FILES_DIR}")

    print("Запуск бота...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    if not BOT_TOKEN:
        print("Ошибка: Токен бота не найден. Установите переменную окружения BOT_TOKEN или создайте файл .env с BOT_TOKEN=\"ВАШ_ТОКЕН\"", file=sys.stderr)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("Бот остановлен вручную.")
        except Exception as e:
            print(f"Произошла ошибка при запуске бота: {e}", file=sys.stderr)