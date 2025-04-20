from aiogram import Router

from . import start_handlers
from . import source_handlers
from . import upload_handlers # Убедитесь, что эта строка есть
from . import config_handlers
from . import history_handlers # Убедитесь, что эта строка есть

main_router = Router()
main_router.include_router(start_handlers.router)
main_router.include_router(source_handlers.router)
main_router.include_router(upload_handlers.router) # Убедитесь, что эта строка есть
main_router.include_router(config_handlers.router)
main_router.include_router(history_handlers.router) # Убедитесь, что эта строка есть