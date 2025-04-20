import aiosqlite
import os
from datetime import datetime
from telegram_bot.config import SQLITE_DB_PATH, BASE_DIR
from ..utils.encryption import encrypt_data, decrypt_data
import json
import sys
from typing import Dict, Any, Optional, List

async def init_db():
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
                status TEXT NOT NULL,
                file_path TEXT,
                error_message TEXT,
                true_tabs_datasheet_id TEXT,
                duration_seconds REAL
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS source_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                source_type TEXT NOT NULL,
                source_url TEXT,
                source_user TEXT,
                source_pass TEXT,
                source_query TEXT,
                specific_params_json TEXT,
                is_default BOOLEAN DEFAULT FALSE -- Добавлена колонка is_default
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS true_tabs_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                upload_api_token TEXT,
                upload_datasheet_id TEXT,
                upload_field_map_json TEXT,
                is_default BOOLEAN DEFAULT FALSE -- Добавлена колонка is_default
            )
        ''')


        await db.commit()

async def get_upload_history_by_id(record_id: int) -> Optional[Dict]:
    # Исправлен путь к БД, использовать SQLITE_DB_PATH из config
    async with aiosqlite.connect(SQLITE_DB_PATH) as db: # <-- Использовать SQLITE_DB_PATH
        db.row_factory = aiosqlite.Row # Возвращать строки как объекты с доступом по имени колонки
        cursor = await db.cursor()
        await cursor.execute("SELECT * FROM uploads WHERE id = ?", (record_id,))
        row = await cursor.fetchone()
        if row:
            return dict(row) # Преобразовать строку в словарь
        else:
            return None

async def add_upload_record(source_type: str, status: str, file_path: str = None, error_message: str = None, true_tabs_datasheet_id: str = None, duration_seconds: float = None):
    timestamp = datetime.now().isoformat()
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        await db.execute('''
            INSERT INTO uploads (timestamp, source_type, status, file_path, error_message, true_tabs_datasheet_id, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (timestamp, source_type, status, file_path, error_message, true_tabs_datasheet_id, duration_seconds))
        await db.commit()

async def get_upload_history(limit: int = 10, offset: int = 0):
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT * FROM uploads
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        ''', (limit, offset))
        records = await cursor.fetchall()
        return [dict(row) for row in records]

async def count_upload_history():
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        cursor = await db.execute('SELECT COUNT(*) FROM uploads')
        row = await cursor.fetchone()
        return row[0] if row else 0

async def add_source_config(name: str, source_type: str, params: Dict[str, Any]) -> bool:
    source_url = params.get("source_url")
    source_user = params.get("source_user")
    source_pass = params.get("source_pass")
    source_query = params.get("source_query")

    # Убедимся, что specific_params_json содержит только те параметры, которые не имеют своих колонок
    specific_params_to_save = {
        k: v for k, v in params.items() if k not in ["source_type", "name", "source_url", "source_user", "source_pass", "source_query"]
    }
    encrypted_pass = encrypt_data(source_pass) if source_pass is not None else None
    encrypted_specific_params = encrypt_data(json.dumps(specific_params_to_save))


    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        try:
            # При добавлении новой конфигурации is_default по умолчанию FALSE
            await db.execute('''
                INSERT INTO source_configs (name, source_type, source_url, source_user, source_pass, source_query, specific_params_json, is_default)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (name, source_type, source_url, source_user, encrypted_pass, source_query, encrypted_specific_params, False)) # <-- Передаем False
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False # Конфигурация с таким именем уже существует (для UNIQUE поля)
        except Exception as e:
            print(f"Ошибка при добавлении конфигурации источника: {e}", file=sys.stderr)
            return False


async def get_source_config(name: str) -> Optional[Dict[str, Any]]:
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
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # В список добавляем is_default, чтобы в UI можно было показать, какой конфиг дефолтный
        cursor = await db.execute('SELECT id, name, source_type, is_default FROM source_configs ORDER BY name') # <-- Выбираем is_default
        rows = await cursor.fetchall()
        # Преобразуем is_default из 0/1 в True/False
        return [dict(row) | {'is_default': bool(row['is_default'])} for row in rows]


async def delete_source_config(name: str) -> bool:
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        cursor = await db.execute('DELETE FROM source_configs WHERE name = ?', (name,))
        await db.commit()
        return cursor.rowcount > 0

# --- ФУНКЦИЯ ОБНОВЛЕНИЯ ИСТОЧНИКА (добавлен is_default в UPDATE) ---
async def update_source_config(name: str, source_type: str, params: Dict[str, Any]) -> bool:
    """Updates an existing source configuration by name."""
    source_url = params.get("source_url")
    source_user = params.get("source_user")
    source_pass = params.get("source_pass")
    source_query = params.get("source_query")

    specific_params_to_save = {
        k: v for k, v in params.items() if k not in ["source_type", "name", "source_url", "source_user", "source_pass", "source_query", "is_default"] # Исключаем is_default
    }

    encrypted_pass = encrypt_data(source_pass) if source_pass is not None else None
    encrypted_specific_params = encrypt_data(json.dumps(specific_params_to_save))

    # is_default не обновляется этой функцией, только функциями set_default_*_config
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

# --- НОВЫЕ ФУНКЦИИ ДЛЯ УСТАНОВКИ/ПОЛУЧЕНИЯ ДЕФОЛТНОГО КОНФИГА ИСТОЧНИКА ---
async def set_default_source_config(name: str) -> bool:
    """Sets a source configuration as the default for its type, unsetting previous default."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        # Получаем source_type конфигурации, которую хотим сделать дефолтной
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT source_type FROM source_configs WHERE name = ?', (name,))
        row = await cursor.fetchone()
        if not row:
            print(f"Конфигурация источника '{name}' не найдена для установки как дефолтной.", file=sys.stderr)
            return False
        source_type = row['source_type']

        try:
            # Начинаем транзакцию, чтобы обеспечить атомарность операций
            await db.execute("BEGIN")

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
    """Retrieves the default source configuration for a given source type."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Ищем конфигурацию с флагом is_default = TRUE для данного типа источника
        cursor = await db.execute('SELECT name FROM source_configs WHERE source_type = ? AND is_default = TRUE LIMIT 1', (source_type,))
        row = await cursor.fetchone()
        if row:
            # Используем существующую функцию get_source_config для получения полных данных
            return await get_source_config(row['name'])
        return None


async def add_tt_config(name: str, upload_api_token: str, upload_datasheet_id: str, upload_field_map_json: str) -> bool:
    encrypted_token = encrypt_data(upload_api_token) if upload_api_token is not None else None
    encrypted_field_map = encrypt_data(upload_field_map_json) if upload_field_map_json is not None else None


    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        try:
            # При добавлении новой конфигурации is_default по умолчанию FALSE
            await db.execute('''
                INSERT INTO true_tabs_configs (name, upload_api_token, upload_datasheet_id, upload_field_map_json, is_default)
                VALUES (?, ?, ?, ?, ?)
            ''', (name, encrypted_token, upload_datasheet_id, encrypted_field_map, False)) # <-- Передаем False
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
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
                'is_default': bool(config_data.get('is_default', False)), # <-- Добавляем is_default
            }
        return None

async def list_tt_configs() -> List[Dict[str, str]]:
    """Получение списка всех имен сохраненных конфигураций True Tabs."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # В список добавляем is_default
        cursor = await db.execute('SELECT id, name, upload_datasheet_id, is_default FROM true_tabs_configs ORDER BY name') # <-- Выбираем is_default
        rows = await cursor.fetchall()
        # Преобразуем is_default из 0/1 в True/False
        return [dict(row) | {'is_default': bool(row['is_default'])} for row in rows]

async def delete_tt_config(name: str) -> bool:
    """Удаление конфигурации True Tabs по имени."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        cursor = await db.execute('DELETE FROM true_tabs_configs WHERE name = ?', (name,))
        await db.commit()
        return cursor.rowcount > 0

# --- НОВАЯ ФУНКЦИЯ ОБНОВЛЕНИЯ TRUE TABS (добавлен is_default в UPDATE) ---
async def update_tt_config(name: str, upload_api_token: str, upload_datasheet_id: str, upload_field_map_json: str) -> bool:
    """Updates an existing True Tabs configuration by name."""
    encrypted_token = encrypt_data(upload_api_token) if upload_api_token is not None else None
    encrypted_field_map = encrypt_data(upload_field_map_json) if upload_field_map_json is not None else None

    # is_default не обновляется этой функцией
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        try:
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

# --- НОВЫЕ ФУНКЦИИ ДЛЯ УСТАНОВКИ/ПОЛУЧЕНИЯ ДЕФОЛТНОГО КОНФИГА TRUE TABS ---
async def set_default_tt_config(name: str) -> bool:
    """Sets a True Tabs configuration as the default, unsetting previous default."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        try:
            # Начинаем транзакцию
            await db.execute("BEGIN")

            # Сбрасываем флаг is_default для всех других конфигураций True Tabs
            await db.execute('UPDATE true_tabs_configs SET is_default = FALSE WHERE is_default = TRUE')

            # Устанавливаем флаг is_default для указанной конфигурации
            cursor = await db.execute('UPDATE true_tabs_configs SET is_default = TRUE WHERE name = ?', (name,))

            await db.execute("COMMIT") # Коммитим транзакцию
            return cursor.rowcount > 0 # Возвращаем True, если указанная конфигурация была успешно обновлена

        except Exception as e:
            await db.execute("ROLLBACK") # Откатываем транзакцию
            print(f"Ошибка при установке конфигурации True Tabs '{name}' как дефолтной: {e}", file=sys.stderr)
            return False

async def get_default_tt_config() -> Optional[Dict[str, str]]:
    """Retrieves the default True Tabs configuration."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Ищем конфигурацию с флагом is_default = TRUE
        cursor = await db.execute('SELECT name FROM true_tabs_configs WHERE is_default = TRUE LIMIT 1')
        row = await cursor.fetchone()
        if row:
            # Используем существующую функцию get_tt_config для получения полных данных
            return await get_tt_config(row['name'])
        return None