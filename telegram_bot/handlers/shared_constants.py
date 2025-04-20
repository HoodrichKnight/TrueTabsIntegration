from typing import Dict, List

# Определяем порядок параметров для каждого типа источника
SOURCE_PARAMS_ORDER: Dict[str, List[str]] = {
    "postgres": ["source_url", "source_user", "source_pass", "source_query"],
    "mysql": ["source_url", "source_user", "source_pass", "source_query"],
    "sqlite": ["source_url", "source_query"], # source_url здесь - путь к файлу .db
    "mongodb": ["source_url", "mongo_db", "mongo_collection"], # source_url здесь - URI
    "redis": ["source_url", "redis_pattern"], # source_url здесь - URL
    "elasticsearch": ["source_url", "es_index", "es_query"], # source_url здесь - URL
    "csv": ["source_url"], # source_url здесь - путь к файлу .csv
    # Removed excel as per user request
}

# Более дружественные названия параметров для отображения пользователю
PARAM_NAMES_FRIENDLY: Dict[str, str] = {
    'source_url': 'URL/путь к источнику',
    'source_user': 'Пользователь',
    'source_pass': 'Пароль',
    'source_query': 'Запрос (SQL/JSON)',
    'mongo_db': 'Имя базы данных MongoDB',
    'mongo_collection': 'Имя коллекции MongoDB',
    'redis_pattern': 'Паттерн ключей Redis',
    'es_index': 'Имя индекса Elasticsearch',
    'es_query': 'JSON запрос Elasticsearch',
    'upload_api_token': 'API токен True Tabs',
    'upload_datasheet_id': 'ID таблицы True Tabs',
    'upload_field_map_json': 'JSON сопоставления полей',
    'source_url_file': 'Путь к файлу',  # Для CSV/Excel
    'record_id': 'ID записи True Tabs',
    'field_updates_json': 'JSON обновлений полей',
}

def get_friendly_param_name(param_key: str) -> str:
    """
    Возвращает более дружественное имя параметра для отображения пользователю.
    Если параметр не найден, возвращает исходное имя.
    """
    return PARAM_NAMES_FRIENDLY.get(param_key, param_key)
