import subprocess
import shlex
import asyncio
import time
import os
import json
from ..config import RUST_EXECUTABLE_PATH
from typing import Dict, Any, Optional
import sys

# Изменена возвращаемая структура
async def execute_rust_command(args: list) -> Dict[str, Any]:
    if not os.path.exists(RUST_EXECUTABLE_PATH):
        # Если исполняемый файл не найден, возвращаем ошибку сразу
        return {
            "status": "ERROR",
            "message": f"Ошибка: Исполняемый файл Rust не найден по пути: {RUST_EXECUTABLE_PATH}. Убедитесь, что проект скомпилирован (cargo build --release) и путь в config.py верен.",
            "duration_seconds": 0.0,
            "process": None, # Добавляем None для process
            "communicate_future": None, # Добавляем None для future
            "file_path": None, "extracted_rows": None, "uploaded_records": None, "datasheet_id": None, # Добавляем другие поля с None
        }

    command = [RUST_EXECUTABLE_PATH] + args
    command_string = shlex.join(command)

    print(f"Выполнение команды Rust: {command_string}", file=sys.stderr)

    start_time = time.time()
    process = None

    try:
        # Запускаем подпроцесс неблокирующим способом
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        # Создаем задачу для communicate(), но НЕ ЖДЕМ ее завершения здесь
        communicate_future = asyncio.create_task(process.communicate())

        # Возвращаем информацию о запущенном процессе, включая сам объект process и future
        return {
            "status": "PROCESS_STARTED", # Новый статус, указывающий, что процесс запущен
            "process": process, # Возвращаем объект процесса
            "communicate_future": communicate_future, # Возвращаем future для communicate
            "start_time": start_time, # Возвращаем время старта для расчета длительности
            "command_string": command_string, # Возвращаем строку команды для логов/отладки
            "message": "Rust process started.", # Начальное сообщение
            "file_path": None, "extracted_rows": None, "uploaded_records": None, "datasheet_id": None, "duration_seconds": 0.0, # Добавляем другие поля с начальными значениями
        }

    except Exception as e:
        # Если произошла ошибка при запуске подпроцесса
        error_message = f"Произошла ошибка при запуске Rust процесса: {e}"
        print(error_message, file=sys.stderr)
        return {
            "status": "ERROR",
            "file_path": None,
            "message": error_message,
            "duration_seconds": time.time() - start_time,
            "process": None,
            "communicate_future": None,
            "extracted_rows": None,
            "uploaded_records": None,
            "datasheet_id": None,
        }