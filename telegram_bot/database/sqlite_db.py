import aiosqlite
import os
from datetime import datetime
from ..config import SQLITE_DB_PATH # BASE_DIR removed because it is not defined in config.py
from ..utils.encryption import encrypt_data, decrypt_data # Убедитесь, что у вас есть модуль encryption
import json
import sys
from typing import Dict, Any, Optional, List

async def init_db():
    """
    Инициализирует базу данных SQLite: создает директорию, если она не существует,
    и создает все необходимые таблицы, если они еще не существуют.
    """
    # Убедитесь, что директория для БД существует
    db_dir = os.path.dirname(SQLITE_DB_PATH)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        # Таблица для истории загрузок
        await db.execute('''
            CREATE TABLE IF NOT EXISTS uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                source_type TEXT NOT NULL,
                status TEXT NOT NULL, -- 'SUCCESS', 'ERROR', 'CANCELLED'
                file_path TEXT, -- Путь к файлу результата на сервере (если применимо)
                error_message TEXT, -- Сообщение об ошибке или статусное сообщение
                true_tabs_datasheet_id TEXT, -- ID таблицы True Tabs, если применимо
                duration_seconds REAL -- Время выполнения в секундах
            )
        ''')

        # Таблица для сохраненных конфигураций источников данных
        await db.execute('''
            CREATE TABLE IF NOT EXISTS source_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL, -- Уникальное имя конфигурации
                source_type TEXT NOT NULL, -- Тип источника (postgres, mysql, csv и т.д.)
                source_url TEXT, -- URL или путь к файлу источника
                source_user TEXT, -- Пользователь БД
                source_pass TEXT, -- Пароль БД (зашифрован)
                source_query TEXT, -- SQL запрос или другой запрос
                specific_params_json TEXT, -- Другие специфические параметры в JSON (зашифрован)
                is_default BOOLEAN DEFAULT FALSE -- Флаг конфигурации по умолчанию (для данного типа источника)
            )
        ''')

        # Таблица для сохраненных конфигураций True Tabs
        await db.execute('''
            CREATE TABLE IF NOT EXISTS true_tabs_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL, -- Уникальное имя конфигурации
                upload_api_token TEXT, -- API токен (зашифрован)
                upload_datasheet_id TEXT, -- ID таблицы True Tabs
                upload_field_map_json TEXT, -- Сопоставление полей в JSON (зашифрован)
                is_default BOOLEAN DEFAULT FALSE -- Флаг конфигурации по умолчанию (одна на все TT)
            )
        ''')

        # Таблица для запланированных заданий
        await db.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT UNIQUE NOT NULL, -- Уникальный ID задания, используется APScheduler
                name TEXT UNIQUE NOT NULL, -- Понятное пользователю имя задания
                chat_id INTEGER NOT NULL, -- ID чата Telegram, к которому относится задание
                source_config_name TEXT NOT NULL, -- Имя сохраненной конфигурации источника
                tt_config_name TEXT NOT NULL, -- Имя сохраненной конфигурации True Tabs
                action TEXT NOT NULL, -- Действие (например, 'extract')
                trigger_type TEXT NOT NULL, -- Тип триггера APScheduler ('interval', 'cron', 'date')
                trigger_args_json TEXT NOT NULL, -- Параметры триггера в формате JSON строки
                enabled BOOLEAN DEFAULT TRUE, -- Включено ли задание
                created_at TEXT NOT NULL -- Время создания задания (ISO формат)
            )
        ''')

        await db.commit()

# --- Функции для работы с историей загрузок ---

async def get_upload_history_by_id(record_id: int) -> Optional[Dict]:
    """Получает запись истории загрузки по ее ID."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row # Возвращать строки как объекты с доступом по имени колонки
        cursor = await db.cursor()
        await cursor.execute("SELECT * FROM uploads WHERE id = ?", (record_id,))
        row = await cursor.fetchone()
        if row:
            return dict(row) # Преобразовать строку в словарь
        else:
            return None

async def add_upload_record(source_type: str, status: str, file_path: str = None, error_message: str = None, true_tabs_datasheet_id: str = None, duration_seconds: float = None):
    """Добавляет новую запись в историю загрузок."""
    timestamp = datetime.now().isoformat()
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        await db.execute('''
            INSERT INTO uploads (timestamp, source_type, status, file_path, error_message, true_tabs_datasheet_id, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (timestamp, source_type, status, file_path, error_message, true_tabs_datasheet_id, duration_seconds))
        await db.commit()

async def get_upload_history(limit: int = 10, offset: int = 0) -> List[Dict]:
    """Получает записи истории загрузок с пагинацией."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT * FROM uploads
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        ''', (limit, offset))
        records = await cursor.fetchall()
        return [dict(row) for row in records]

async def count_upload_history() -> int:
    """Подсчитывает общее количество записей в истории загрузок."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        cursor = await db.execute('SELECT COUNT(*) FROM uploads')
        row = await cursor.fetchone()
        return row[0] if row else 0

# --- Функции для работы с конфигурациями источников ---

async def add_source_config(name: str, source_type: str, params: Dict[str, Any]) -> bool:
    """Добавляет новую конфигурацию источника в базу данных."""
    source_url = params.get("source_url")
    source_user = params.get("source_user")
    source_pass = params.get("source_pass")
    source_query = params.get("source_query")

    # Собираем остальные параметры, не имеющие отдельных колонок
    specific_params_to_save = {
        k: v for k, v in params.items() if k not in ["source_type", "name", "source_url", "source_user", "source_pass", "source_query"]
    }
    # Шифруем пароль и специфические параметры
    encrypted_pass = encrypt_data(source_pass) if source_pass is not None else None
    encrypted_specific_params = encrypt_data(json.dumps(specific_params_to_save))


    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        try:
            # При добавлении новая конфигурация не является дефолтной
            await db.execute('''
                INSERT INTO source_configs (name, source_type, source_url, source_user, source_pass, source_query, specific_params_json, is_default)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (name, source_type, source_url, source_user, encrypted_pass, source_query, encrypted_specific_params, False))
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            # Конфигурация с таким именем уже существует (UNIQUE constraint failed)
            return False
        except Exception as e:
            print(f"Ошибка при добавлении конфигурации источника: {e}", file=sys.stderr)
            return False


async def get_source_config(name: str) -> Optional[Dict[str, Any]]:
    """Получает конфигурацию источника по ее имени."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM source_configs WHERE name = ?', (name,))
        row = await cursor.fetchone()

        if row:
            config_data = dict(row)

            # Дешифруем пароль, если он есть
            config_data['source_pass'] = decrypt_data(config_data['source_pass']) if config_data['source_pass'] else None
            # Дешифруем и парсим specific_params_json
            specific_params_json_decrypted = decrypt_data(config_data.get('specific_params_json')) # Используем .get() на случай, если поле было null/пустое
            config_data['specific_params'] = json.loads(specific_params_json_decrypted) if specific_params_json_decrypted else {}

            # Собираем все параметры в один словарь для удобства, включая is_default
            full_params = {
                k: v for k, v in config_data.items() if k not in ['id', 'name', 'specific_params_json', 'specific_params']
            }
            full_params.update(config_data['specific_params']) # Добавляем специфические параметры
            full_params['name'] = config_data['name'] # Добавляем имя
            full_params['source_type'] = config_data['source_type'] # Добавляем тип источника
            full_params['is_default'] = bool(config_data.get('is_default', False)) # Добавляем is_default


            return full_params
        return None


async def list_source_configs() -> List[Dict[str, Any]]:
    """Получает список всех сохраненных конфигураций источников (только имя, тип, дефолт)."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Выбираем только нужные поля для списка
        cursor = await db.execute('SELECT id, name, source_type, is_default FROM source_configs ORDER BY name')
        rows = await cursor.fetchall()
        # Преобразуем is_default из 0/1 в True/False
        return [dict(row) | {'is_default': bool(row['is_default'])} for row in rows]


async def delete_source_config(name: str) -> bool:
    """Удаляет конфигурацию источника по ее имени."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        cursor = await db.execute('DELETE FROM source_configs WHERE name = ?', (name,))
        await db.commit()
        return cursor.rowcount > 0

async def update_source_config(name: str, source_type: str, params: Dict[str, Any]) -> bool:
    """Обновляет существующую конфигурацию источника по имени."""
    source_url = params.get("source_url")
    source_user = params.get("source_user")
    source_pass = params.get("source_pass")
    source_query = params.get("source_query")

    # Исключаем is_default при сохранении, оно меняется отдельной функцией
    specific_params_to_save = {
        k: v for k, v in params.items() if k not in ["source_type", "name", "source_url", "source_user", "source_pass", "source_query", "is_default"]
    }

    encrypted_pass = encrypt_data(source_pass) if source_pass is not None else None
    encrypted_specific_params = encrypt_data(json.dumps(specific_params_to_save))

    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        try:
            cursor = await db.execute('''
                UPDATE source_configs
                SET source_type = ?, source_url = ?, source_user = ?, source_pass = ?, source_query = ?, specific_params_json = ?
                WHERE name = ?
            ''', (source_type, source_url, source_user, encrypted_pass, source_query, encrypted_specific_params, name))
            await db.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"Ошибка при обновлении конфигурации источника '{name}': {e}", file=sys.stderr)
            return False

async def set_default_source_config(name: str) -> bool:
    """Устанавливает конфигурацию источника как дефолтную для ее типа, сбрасывая предыдущую дефолтную."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Получаем source_type конфигурации, которую хотим сделать дефолтной
        cursor = await db.execute('SELECT source_type FROM source_configs WHERE name = ?', (name,))
        row = await cursor.fetchone()
        if not row:
            print(f"Конфигурация источника '{name}' не найдена для установки как дефолтной.", file=sys.stderr)
            return False
        source_type = row['source_type']

        try:
            await db.execute("BEGIN") # Начинаем транзакцию
            # Сбрасываем флаг is_default для всех других конфигураций этого типа источника
            await db.execute('UPDATE source_configs SET is_default = FALSE WHERE source_type = ? AND is_default = TRUE', (source_type,))
            # Устанавливаем флаг is_default для указанной конфигурации
            cursor = await db.execute('UPDATE source_configs SET is_default = TRUE WHERE name = ?', (name,))
            await db.execute("COMMIT") # Коммитим транзакцию
            return cursor.rowcount > 0 # Возвращаем True, если указанная конфигурация была успешно обновлена

        except Exception as e:
            await db.execute("ROLLBACK") # Откатываем транзакцию в случае ошибки
            print(f"Ошибка при установке конфигурации источника '{name}' как дефолтной: {e}", file=sys.stderr)
            return False

async def get_default_source_config(source_type: str) -> Optional[Dict[str, Any]]:
    """Получает дефолтную конфигурацию источника для заданного типа источника."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Ищем конфигурацию с флагом is_default = TRUE для данного типа источника
        cursor = await db.execute('SELECT name FROM source_configs WHERE source_type = ? AND is_default = TRUE LIMIT 1', (source_type,))
        row = await cursor.fetchone()
        if row:
            # Используем существующую функцию get_source_config для получения полных данных
            return await get_source_config(row['name'])
        return None


# --- Функции для работы с конфигурациями True Tabs ---

async def add_tt_config(name: str, upload_api_token: str, upload_datasheet_id: str, upload_field_map_json: str) -> bool:
    """Добавляет новую конфигурацию True Tabs в базу данных."""
    # Шифруем токен и сопоставление полей
    encrypted_token = encrypt_data(upload_api_token) if upload_api_token is not None else None
    encrypted_field_map = encrypt_data(upload_field_map_json) if upload_field_map_json is not None else None

    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        try:
            # При добавлении новая конфигурация не является дефолтной
            await db.execute('''
                INSERT INTO true_tabs_configs (name, upload_api_token, upload_datasheet_id, upload_field_map_json, is_default)
                VALUES (?, ?, ?, ?, ?)
            ''', (name, encrypted_token, upload_datasheet_id, encrypted_field_map, False))
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
             # Конфигурация с таким именем уже существует
             return False
        except Exception as e:
            print(f"Ошибка при добавлении конфигурации True Tabs: {e}", file=sys.stderr)
            return False


async def get_tt_config(name: str) -> Optional[Dict[str, str]]:
    """Получение конфигурации True Tabs по имени."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM true_tabs_configs WHERE name = ?', (name,))
        row = await cursor.fetchone()

        if row:
            config_data = dict(row)
            # Дешифруем данные, если они есть
            config_data['upload_api_token'] = decrypt_data(config_data['upload_api_token']) if config_data['upload_api_token'] else None
            config_data['upload_field_map_json'] = decrypt_data(config_data['upload_field_map_json']) if config_data['upload_field_map_json'] else None

            # Убедимся, что возвращаем словарь с ожидаемыми ключами, включая is_default
            return {
                'id': config_data.get('id'),
                'name': config_data.get('name'),
                'upload_api_token': config_data.get('upload_api_token'),
                'upload_datasheet_id': config_data.get('upload_datasheet_id'),
                'upload_field_map_json': config_data.get('upload_field_map_json'),
                'is_default': bool(config_data.get('is_default', False)), # Добавляем is_default
            }
        return None

async def list_tt_configs() -> List[Dict[str, str]]:
    """Получение списка всех сохраненных конфигураций True Tabs (только имя, ID, дефолт)."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Выбираем только нужные поля для списка
        cursor = await db.execute('SELECT id, name, upload_datasheet_id, is_default FROM true_tabs_configs ORDER BY name')
        rows = await cursor.fetchall()
        # Преобразуем is_default из 0/1 в True/False
        return [dict(row) | {'is_default': bool(row['is_default'])} for row in rows]

async def delete_tt_config(name: str) -> bool:
    """Удаление конфигурации True Tabs по имени."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        cursor = await db.execute('DELETE FROM true_tabs_configs WHERE name = ?', (name,))
        await db.commit()
        return cursor.rowcount > 0

async def update_tt_config(name: str, upload_api_token: str, upload_datasheet_id: str, upload_field_map_json: str) -> bool:
    """Обновляет существующую конфигурацию True Tabs по имени."""
    # Шифруем токен и сопоставление полей
    encrypted_token = encrypt_data(upload_api_token) if upload_api_token is not None else None
    encrypted_field_map = encrypt_data(upload_field_map_json) if upload_field_map_json is not None else None

    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        try:
            # is_default не обновляется этой функцией
            cursor = await db.execute('''
                UPDATE true_tabs_configs
                SET upload_api_token = ?, upload_datasheet_id = ?, upload_field_map_json = ?
                WHERE name = ?
            ''', (encrypted_token, upload_datasheet_id, encrypted_field_map, name))
            await db.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"Ошибка при обновлении конфигурации True Tabs '{name}': {e}", file=sys.stderr)
            return False

async def set_default_tt_config(name: str) -> bool:
    """Устанавливает конфигурацию True Tabs как дефолтную, сбрасывая предыдущую дефолтную."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        try:
            await db.execute("BEGIN") # Начинаем транзакцию
            # Сбрасываем флаг is_default для всех других конфигураций True Tabs
            await db.execute('UPDATE true_tabs_configs SET is_default = FALSE WHERE is_default = TRUE')
            # Устанавливаем флаг is_default для указанной конфигурации
            cursor = await db.execute('UPDATE true_tabs_configs SET is_default = TRUE WHERE name = ?', (name,))
            await db.commit() # Коммитим транзакцию
            return cursor.rowcount > 0 # Возвращаем True, если указанная конфигурация была успешно обновлена

        except Exception as e:
            await db.execute("ROLLBACK") # Откатываем транзакцию
            print(f"Ошибка при установке конфигурации True Tabs '{name}' как дефолтной: {e}", file=sys.stderr)
            return False

async def get_default_tt_config() -> Optional[Dict[str, str]]:
    """Получает дефолтную конфигурацию True Tabs."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Ищем конфигурацию с флагом is_default = TRUE
        cursor = await db.execute('SELECT name FROM true_tabs_configs WHERE is_default = TRUE LIMIT 1')
        row = await cursor.fetchone()
        if row:
            # Используем существующую функцию get_tt_config для получения полных данных
            return await get_tt_config(row['name'])
        return None

# --- Функции для работы с запланированными заданиями ---

async def add_scheduled_job(job_id: str, name: str, chat_id: int, source_config_name: str, tt_config_name: str, action: str, trigger_type: str, trigger_args_json: str) -> bool:
    """Добавляет новое запланированное задание в базу данных."""
    created_at = datetime.now().isoformat()
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        try:
            # При добавлении задание включено по умолчанию (enabled = TRUE)
            await db.execute('''
                INSERT INTO scheduled_jobs (job_id, name, chat_id, source_config_name, tt_config_name, action, trigger_type, trigger_args_json, enabled, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (job_id, name, chat_id, source_config_name, tt_config_name, action, trigger_type, trigger_args_json, True, created_at))
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            # Задание с таким job_id или именем уже существует
            print(f"Scheduled job with job_id '{job_id}' or name '{name}' already exists.", file=sys.stderr)
            return False
        except Exception as e:
            print(f"Error adding scheduled job: {e}", file=sys.stderr)
            return False

async def get_scheduled_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Получает запланированное задание по его job_id."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM scheduled_jobs WHERE job_id = ?', (job_id,))
        row = await cursor.fetchone()
        if row:
             # Преобразуем enabled из 0/1 в True/False
             job_data = dict(row)
             job_data['enabled'] = bool(job_data.get('enabled', False))
             return job_data
        return None

async def list_scheduled_jobs(chat_id: int) -> List[Dict[str, Any]]:
    """Получает список всех запланированных заданий для конкретного chat_id."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Сортируем по времени создания, чтобы новые задания были сверху
        cursor = await db.execute('SELECT * FROM scheduled_jobs WHERE chat_id = ? ORDER BY created_at DESC', (chat_id,))
        rows = await cursor.fetchall()
         # Преобразуем enabled из 0/1 в True/False
        return [dict(row) | {'enabled': bool(row.get('enabled', False))} for row in rows]

async def list_all_scheduled_jobs() -> List[Dict[str, Any]]:
    """Получает список всех запланированных заданий из базы данных (для загрузки в scheduler на старте)."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM scheduled_jobs') # Без ORDER BY, т.к. scheduler сам сортирует
        rows = await cursor.fetchall()
        # Преобразуем enabled из 0/1 в True/False
        return [dict(row) | {'enabled': bool(row.get('enabled', False))} for row in rows]

async def delete_scheduled_job(job_id: str) -> bool:
    """Удаляет запланированное задание по его job_id."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        cursor = await db.execute('DELETE FROM scheduled_jobs WHERE job_id = ?', (job_id,))
        await db.commit()
        return cursor.rowcount > 0

async def update_scheduled_job_via_delete_add(job_id: str, name: str, chat_id: int, source_config_name: str, tt_config_name: str, action: str, trigger_type: str, trigger_args_json: str, enabled: bool) -> bool:
    """
    Обновляет запланированное задание путем удаления старого и добавления нового.
    Возвращает True, если обновление прошло успешно, иначе False.
    """
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        try:
            # Удаляем старое задание
            await db.execute('DELETE FROM scheduled_jobs WHERE job_id = ?', (job_id,))
            # Добавляем новое задание с теми же параметрами (enabled учитывается при добавлении)
            await db.execute('''
                INSERT INTO scheduled_jobs (job_id, name, chat_id, source_config_name, tt_config_name, action, trigger_type, trigger_args_json, enabled, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ''', (job_id, name, chat_id, source_config_name, tt_config_name, action, trigger_type, trigger_args_json, enabled))
            await db.commit()
            return True
        except Exception as e:
            print(f"Ошибка при обновлении (удалении и добавлении) запланированного задания '{job_id}': {e}", file=sys.stderr)
            return False

async def get_latest_upload_history_by_job_id(job_id: str):
    """
    Получить последнюю запись истории загрузок по job_id.
    Возвращает словарь с записью или None, если не найдено.
    """
    query = """
    SELECT * FROM uploads
    WHERE job_id = ?
    ORDER BY timestamp DESC
    LIMIT 1
    """
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        async with db.execute(query, (job_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                columns = [column[0] for column in cursor.description]
                return dict(zip(columns, row))
            else:
                return None

async def update_scheduled_job(job_id: str, name: str, chat_id: int, source_config_name: str, tt_config_name: str, action: str, trigger_type: str, trigger_args_json: str, enabled: bool) -> bool:
    """
    Обновляет существующее запланированное задание по job_id.
    Возвращает True, если обновление прошло успешно, иначе False.
    """
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        try:
            cursor = await db.execute('''
                UPDATE scheduled_jobs
                SET name = ?, chat_id = ?, source_config_name = ?, tt_config_name = ?, action = ?, trigger_type = ?, trigger_args_json = ?, enabled = ?
                WHERE job_id = ?
            ''', (name, chat_id, source_config_name, tt_config_name, action, trigger_type, trigger_args_json, enabled, job_id))
            await db.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"Ошибка при обновлении запланированного задания '{job_id}': {e}", file=sys.stderr)
            return False

async def get_last_upload_for_scheduled_job(chat_id: int, action: str) -> Optional[Dict[str, Any]]:
    """
    Получает последнюю запись истории загрузок для заданного chat_id и действия (source_type).
    Возвращает словарь с записью или None, если записей нет.
    """
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Получаем tt_config_name для заданного chat_id и action
        cursor = await db.execute('SELECT tt_config_name FROM scheduled_jobs WHERE chat_id = ? AND action = ? LIMIT 1', (chat_id, action))
        row = await cursor.fetchone()
        if not row:
            return None
        tt_config_name = row['tt_config_name']

        # Получаем последнюю запись из uploads для данного tt_config_name и source_type (action)
        cursor = await db.execute('''
            SELECT * FROM uploads
            WHERE true_tabs_datasheet_id = ? AND source_type = ?
            ORDER BY timestamp DESC
            LIMIT 1
        ''', (tt_config_name, action))
        upload_row = await cursor.fetchone()
        if upload_row:
            return dict(upload_row)
        return None
