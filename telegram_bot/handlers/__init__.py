from aiogram import Router

from . import start_handlers
from . import source_handlers
from . import params_handlers
from . import history_handlers

main_router = Router()
main_router.include_router(start_handlers.router)
main_router.include_router(source_handlers.router)
main_router.include_router(params_handlers.router)
main_router.include_router(history_handlers.router)