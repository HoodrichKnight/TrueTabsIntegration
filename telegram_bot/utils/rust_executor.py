import subprocess
import shlex
import asyncio
import time
import os
import json
from config import RUST_EXECUTABLE_PATH
from typing import Dict, Any, Optional
import sys

async def execute_rust_command(args: list) -> Dict[str, Any]:
    if not os.path.exists(RUST_EXECUTABLE_PATH):
        return {
            "status": "ERROR",
            "file_path": None,
            "message": f"Ошибка: Исполняемый файл Rust не найден по пути: {RUST_EXECUTABLE_PATH}. Убедитесь, что проект скомпилирован (cargo build --release) и путь в config.py верен.",
            "duration_seconds": 0.0,
            "extracted_rows": None,
            "uploaded_records": None,
            "datasheet_id": None,
        }

    command = [RUST_EXECUTABLE_PATH] + args
    command_string = shlex.join(command)

    print(f"Выполнение команды Rust: {command_string}", file=sys.stderr)

    start_time = time.time()
    process = None
    stdout_data = b""
    stderr_data = b""

    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        stdout_data, stderr_data = await process.communicate()
        returncode = process.returncode

        stdout_str = stdout_data.decode('utf-8', errors='ignore')
        stderr_str = stderr_data.decode('utf-8', errors='ignore')

        print(f"Rust stdout:\n{stdout_str}", file=sys.stderr)
        print(f"Rust stderr:\n{stderr_str}", file=sys.stderr)
        print(f"Rust процесс завершен с кодом: {returncode}", file=sys.stderr)

        try:
            json_result: Dict[str, Any] = json.loads(stdout_str)
            if 'status' in json_result:
                 return json_result
            else:
                 return {
                     "status": "ERROR",
                     "message": f"Rust процесс завершился (код {returncode}), но stdout не содержит ожидаемый JSON формат результата. Stdout:\n{stdout_str}\nStderr:\n{stderr_str}",
                     "file_path": None,
                     "duration_seconds": time.time() - start_time,
                     "extracted_rows": None,
                     "uploaded_records": None,
                     "datasheet_id": None,
                 }
        except json.JSONDecodeError:
            return {
                "status": "ERROR",
                "message": f"Rust процесс завершился с кодом {returncode}, но stdout не является валидным JSON. Stderr:\n{stderr_str}\nStdout:\n{stdout_str}",
                "file_path": None,
                "duration_seconds": time.time() - start_time,
                "extracted_rows": None,
                "uploaded_records": None,
                "datasheet_id": None,
            }

    except Exception as e:
        error_message = f"Произошла ошибка при выполнении Rust процесса: {e}"
        print(error_message, file=sys.stderr)
        return {
            "status": "ERROR",
            "file_path": None,
            "message": error_message,
            "duration_seconds": time.time() - start_time,
            "extracted_rows": None,
            "uploaded_records": None,
            "datasheet_id": None,
        }