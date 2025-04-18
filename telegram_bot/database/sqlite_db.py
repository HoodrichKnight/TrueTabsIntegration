import aiosqlite
import os
from datetime import datetime
from config import SQLITE_DB_PATH

async def init_db():
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
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