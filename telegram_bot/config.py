import os
from dotenv import load_dotenv
import sys

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

WEATHER_API_KEY = os.environ.get('WEATHER_API_KEY')

RUST_EXECUTABLE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_extractor', 'target', 'release', 'data_extractor')

TEMP_FILES_DIR = os.path.join(os.path.dirname(__file__), 'temp')

SQLITE_DB_PATH = os.path.join(os.path.dirname(__file__), 'database', 'upload_history.db')

TRUE_TABS_DATASHEET_ID = os.getenv("TRUE_TABS_DATASHEET_ID")
TRUE_TABS_API_TOKEN = os.getenv("TRUE_TABS_API_TOKEN")

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")

if not BOT_TOKEN:
    print("Ошибка: Токен бота не найден. Установите переменную окружения BOT_TOKEN или создайте файл .env с BOT_TOKEN=\"ВАШ_ТОКЕН\"", file=sys.stderr)

if not ENCRYPTION_KEY:
    print("Предупреждение: Ключ шифрования (ENCRYPTION_KEY) не найден. Конфигурации не будут шифроваться!", file=sys.stderr)
    # В продакшене лучше прервать выполнение, если ключ отсутствует

if not os.path.exists(RUST_EXECUTABLE_PATH):
     print(f"Предупреждение: Исполняемый файл Rust не найден по пути: {RUST_EXECUTABLE_PATH}. Убедитесь, что Rust проект скомпилирован (cargo build --release).", file=sys.stderr)

os.makedirs(TEMP_FILES_DIR, exist_ok=True)
os.makedirs(os.path.dirname(SQLITE_DB_PATH), exist_ok=True)