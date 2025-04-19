import aiosqlite
import os
from datetime import datetime
from config import SQLITE_DB_PATH
from utils.encryption import encrypt_data, decrypt_data
import json

async def init_db():
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
                specific_params_json TEXT
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS true_tabs_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                upload_api_token TEXT,
                upload_datasheet_id TEXT,
                upload_field_map_json TEXT
            )
        ''')


        await db.commit()

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

    encrypted_pass = encrypt_data(source_pass) if source_pass else None
    encrypted_specific_params = encrypt_data(json.dumps({
        k: v for k, v in params.items() if k not in ["source_type", "name", "source_url", "source_user", "source_pass", "source_query"]
    }))

    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        try:
            await db.execute('''
                INSERT INTO source_configs (name, source_type, source_url, source_user, source_pass, source_query, specific_params_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (name, source_type, source_url, source_user, encrypted_pass, source_query, encrypted_specific_params))
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False
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

            config_data['source_pass'] = decrypt_data(config_data['source_pass']) if config_data['source_pass'] else None
            config_data['specific_params_json'] = decrypt_data(config_data['specific_params_json'])
            config_data['specific_params'] = json.loads(config_data['specific_params_json']) if config_data['specific_params_json'] else {}

            full_params = {
                k: v for k, v in config_data.items() if k not in ['id', 'name', 'specific_params_json', 'specific_params']
            }
            full_params.update(config_data['specific_params'])
            full_params['source_type'] = config_data['source_type']

            return full_params
        return None


async def list_source_configs() -> List[Dict[str, Any]]:
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT id, name, source_type FROM source_configs ORDER BY name')
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def delete_source_config(name: str) -> bool:
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        cursor = await db.execute('DELETE FROM source_configs WHERE name = ?', (name,))
        await db.commit()
        return cursor.rowcount > 0

async def add_tt_config(name: str, upload_api_token: str, upload_datasheet_id: str, upload_field_map_json: str) -> bool:
    encrypted_token = encrypt_data(upload_api_token)
    encrypted_field_map = encrypt_data(upload_field_map_json)

    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        try:
            await db.execute('''
                INSERT INTO true_tabs_configs (name, upload_api_token, upload_datasheet_id, upload_field_map_json)
                VALUES (?, ?, ?, ?)
            ''', (name, encrypted_token, upload_datasheet_id, encrypted_field_map))
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
            # Дешифруем данные
            config_data['upload_api_token'] = decrypt_data(config_data['upload_api_token'])
            config_data['upload_field_map_json'] = decrypt_data(config_data['upload_field_map_json'])
            return config_data
        return None

async def list_tt_configs() -> List[Dict[str, str]]:
    """Получение списка всех имен сохраненных конфигураций True Tabs."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT id, name, upload_datasheet_id FROM true_tabs_configs ORDER BY name')
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def delete_tt_config(name: str) -> bool:
    """Удаление конфигурации True Tabs по имени."""
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        cursor = await db.execute('DELETE FROM true_tabs_configs WHERE name = ?', (name,))
        await db.commit()
        return cursor.rowcount > 0