from aiogram import Router

from . import start_handlers
from . import source_handlers
from . import upload_handlers
from . import config_handlers
from . import history_handlers
from . import scheduled_handlers

main_router = Router()
main_router.include_router(start_handlers.router)
main_router.include_router(source_handlers.router)
main_router.include_router(upload_handlers.router)
main_router.include_router(config_handlers.router)
main_router.include_router(history_handlers.router)
main_router.include_router(scheduled_handlers.router)
