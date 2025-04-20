
import asyncio
import logging
import sys
import os
import json
import config
from datetime import datetime
from typing import Optional

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.jobstores.base import JobLookupError

from telegram_bot.handlers import main_router
from telegram_bot.handlers import upload_handlers # Убедитесь, что импортированы
from telegram_bot.handlers import history_handlers # Убедитесь, что импортированы
from telegram_bot.handlers import config_handlers # Убедитесь, что импортированы
# Импортируем новый файл с хэндлерами расписания
from telegram_bot.handlers import scheduled_handlers # НОВЫЙ ИМПОРТ
from telegram_bot.handlers import weather_handlers

from config import BOT_TOKEN, TEMP_FILES_DIR
from telegram_bot.database.sqlite_db import init_db, list_all_scheduled_jobs, delete_scheduled_job, SQLITE_DB_PATH

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('apscheduler').setLevel(logging.INFO)


scheduler: Optional[AsyncIOScheduler] = None

async def main() -> None:
    if not os.path.exists(TEMP_FILES_DIR):
        os.makedirs(TEMP_FILES_DIR, exist_ok=True)
    print(f"Временная папка для файлов: {TEMP_FILES_DIR}")

    if not config.WEATHER_API_KEY or config.WEATHER_API_KEY == "YOUR_OPENWEATHERMAP_API_KEY":
        print("WARNING: WEATHER_API_KEY is not set in config. Weather features will not work.", file=sys.stderr)

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)

    # Подключение роутеров
    dp.include_router(main_router)
    # dp.include_router(weather_handlers.router) # Удалено, чтобы избежать дублирования
    # dp.include_router(upload_handlers.router) # Удалено, чтобы избежать дублирования
    # dp.include_router(history_handlers.router) # Удалено, чтобы избежать дублирования
    # dp.include_router(config_handlers.router) # Удалено, чтобы избежать дублирования
    # Подключаем роутер для UI управления расписанием
    # dp.include_router(scheduled_handlers.router) # Удалено, т.к. уже включен в main_router


    await init_db()
    print("База данных SQLite инициализирована.")

    # --- Настройка и запуск планировщика APScheduler ---

    jobstores = {
        'default': SQLAlchemyJobStore(url=f'sqlite:///{SQLITE_DB_PATH}')
    }

    executors = {
        'default': {'type': 'asyncio'}
    }

    global scheduler
    scheduler = AsyncIOScheduler(jobstores=jobstores, executors=executors)

    logging.info("Загрузка запланированных заданий из базы данных в планировщик...")
    all_jobs_from_db = await list_all_scheduled_jobs()

    for job_data in all_jobs_from_db:
        try:
            trigger_type = job_data['trigger_type']
            trigger_args = json.loads(job_data['trigger_args_json'])
            trigger = None

            if trigger_type == 'interval':
                trigger = IntervalTrigger(**trigger_args)
            elif trigger_type == 'cron':
                trigger = CronTrigger(**trigger_args)
            elif trigger_type == 'date':
                 try:
                     run_date = datetime.fromisoformat(trigger_args.get('run_date'))
                     if run_date < datetime.now(run_date.tzinfo):
                         logging.warning(f"Пропуск загрузки запланированного задания '{job_data['name']}' (ID: {job_data['job_id']}): Дата запуска ({run_date}) в прошлом.")
                         continue
                     trigger = DateTrigger(run_date=run_date)
                 except Exception as date_e:
                      logging.warning(f"Пропуск загрузки запланированного задания '{job_data['name']}' (ID: {job_data['job_id']}): Ошибка при парсинге даты запуска или DateTrigger не полностью поддержан: {date_e}")
                      continue


            if trigger and job_data['enabled']:
                 scheduler.add_job(
                     scheduled_handlers.scheduled_task_executor,
                     trigger=trigger,
                     id=job_data['job_id'],
                     name=job_data['name'],
                     kwargs={
                         'bot': bot, # Передаем экземпляр бота!
                         'chat_id': job_data['chat_id'],
                         'source_config_name': job_data['source_config_name'],
                         'tt_config_name': job_data['tt_config_name'],
                         'action': job_data['action'],
                         'job_name': job_data['name']
                     },
                     replace_existing=True
                 )
                 logging.info(f"Загружено запланированное задание '{job_data['name']}' (ID: {job_data['job_id']}).")
            elif not job_data['enabled']:
                 logging.info(f"Пропуск отключенного запланированного задания '{job_data['name']}' (ID: {job_data['job_id']}).")
            else:
                 logging.error(f"Не удалось создать триггер для запланированного задания '{job_data['name']}' (ID: {job_data['job_id']}). Пропуск.")


        except Exception as e:
            logging.error(f"Критическая ошибка при загрузке запланированного задания '{job_data.get('name', 'Неизвестно')}' (ID: {job_data.get('job_id', 'Неизвестно')}) из БД: {e}", exc_info=True)


    scheduler.start()
    logging.info("Планировщик APScheduler запущен.")

    print("Запуск бота...")
    try:
        await dp.start_polling(bot)
    finally:
        logging.info("Остановка планировщика APScheduler...")
        scheduler.shutdown()
        logging.info("Планировщик остановлен.")


if __name__ == "__main__":
    if not BOT_TOKEN:
        print("Ошибка: Токен бота не найден...", file=sys.stderr)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("Бот остановлен вручную.", file=sys.stderr)
        except Exception as e:
            print(f"Произошла ошибка при запуске или работе бота: {e}", file=sys.stderr)