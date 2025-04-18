import subprocess
import shlex
import asyncio
import time
import os
from config import RUST_EXECUTABLE_PATH
from typing import Dict, Any
import sys

async def execute_rust_command(args: list) -> Dict[str, Any]:
    if not os.path.exists(RUST_EXECUTABLE_PATH):
        return {
            "status": "ERROR",
            "file_path": None,
            "message": f"Ошибка: Исполняемый файл Rust не найден по пути: {RUST_EXECUTABLE_PATH}. Убедитесь, что проект скомпилирован (cargo build --release) и путь в config.py верен.",
            "duration": 0.0,
            "return_code": -1,
        }

    command = [RUST_EXECUTABLE_PATH] + args
    command_string = shlex.join(command)

    print(f"Выполнение команды Rust: {command_string}", file=sys.stderr)

    start_time = time.time()
    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        stdout_data, stderr_data = await process.communicate()
        end_time = time.time()
        duration = end_time - start_time

        stdout_str = stdout_data.decode('utf-8', errors='ignore')
        stderr_str = stderr_data.decode('utf-8', errors='ignore')

        print(f"Rust stdout:\n{stdout_str}", file=sys.stderr)
        print(f"Rust stderr:\n{stderr_str}", file=sys.stderr)
        print(f"Rust процесс завершен с кодом: {process.returncode}", file=sys.stderr)


        status = "ERROR"
        file_path = None
        message = stderr_str.strip()

        if process.returncode == 0:
            for line in stdout_str.splitlines():
                if line.strip() == "STATUS: SUCCESS":
                    status = "SUCCESS"
                elif line.startswith("FILE_PATH: "):
                    file_path = line.replace("FILE_PATH: ", "").strip().strip('"\' ')

            if status == "SUCCESS" and file_path is None:
                 status = "ERROR"
                 message = "Rust процесс сообщил об успехе, но не вернул путь к XLSX файлу."
                 print(f"Ошибка парсинга результата Rust: {message}", file=sys.stderr)


        return {
            "status": status,
            "file_path": file_path,
            "message": message,
            "duration": duration,
            "return_code": process.returncode,
        }

    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        error_message = f"Произошла ошибка при выполнении Rust процесса: {e}"
        print(error_message, file=sys.stderr)
        return {
            "status": "ERROR",
            "file_path": None,
            "message": error_message,
            "duration": duration,
            "return_code": process.returncode if process else -1,
        }