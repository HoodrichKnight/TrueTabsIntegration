from aiogram import Router, F
from aiogram.types import CallbackQuery
from keyboards.inline import source_selection_keyboard
from aiogram.fsm.context import FSMContext
from .params_handlers import UploadProcess
import json

router = Router()

@router.callback_query(F.data == "select_source")
async def handle_select_source_callback(callback: CallbackQuery):
    """Обработчик колбэка кнопки 'Выбрать источник данных'."""
    await callback.message.edit_text(
        "Выберите источник данных:",
        reply_markup=source_selection_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data.startswith("select_source:"))
async def handle_source_selected_and_request_params(callback: CallbackQuery, state: FSMContext):
    """Обработчик колбэка после выбора конкретного источника - просим параметры."""
    source_type = callback.data.split(":")[1]
    await state.update_data(selected_source_type=source_type)
    await state.set_state(UploadProcess.waiting_for_params)

    SOURCE_PARAMS_EXAMPLE = {
        "postgres": ["source_url", "source_query"],
        "mysql": ["source_url", "source_query"],
        "sqlite": ["source_url", "source_query"],
        "mssql": ["source_url", "source_query"],
        "mongodb": ["source_url (URI)", "mongo_db", "mongo_collection"],
        "redis": ["source_url (URL)", "redis_pattern (опционально)"],
        "cassandra": ["source_url (адреса через запятую)", "cassandra_keyspace", "cassandra_query"],
        "clickhouse": ["source_url (URL)", "source_query"],
        "influxdb": ["source_url (URL)", "influx_token", "influx_org", "influx_bucket", "influx_query (Flux запрос)"],
        "elasticsearch": ["source_url (URL)", "es_index", "es_query (JSON, опционально)"],
        "excel": ["source_url (путь к файлу)"],
        "csv": ["source_url (путь к файлу)"],
        "neo4j": ["source_url (URI)", "neo4j_user", "neo4j_pass", "source_query (Cypher запрос)"],
        "couchbase": ["source_url (URL кластера)", "source_user", "source_pass", "couchbase_bucket", "couchbase_query (N1QL запрос)"],
    }

    source_specific_fields = SOURCE_PARAMS_EXAMPLE.get(source_type, [])

    example_params = {
        "source_type": source_type,
        "upload_api_token": "ВАШ_ТОКЕН",
        "upload_datasheet_id": "ВАШ_DATASHEET_ID",
        "upload_field_map_json": "{\"НазваниеКолонкиИзИсточника1\": \"fldID_TrueTabs1\", \"НазваниеКолонкиИзИсточника2\": \"fldID_TrueTabs2\"}",
    }

    for field in source_specific_fields:
        example_params[field] = "..."

    example_json_str = json.dumps(example_params, indent=2, ensure_ascii=False)


    await callback.message.edit_text(
        f"Выбран источник: <b>{source_type}</b>.\n"
        f"Пожалуйста, отправьте сообщение с JSON строкой, содержащей все необходимые параметры для источника и для загрузки в True Tabs.\n\n"
        f"<b>Обязательные поля для {source_type}:</b> {', '.join(source_specific_fields) if source_specific_fields else 'зависят от запроса/типа источника'}.\n"
        f"<b>Обязательные поля для True Tabs:</b> <code>upload_api_token</code>, <code>upload_datasheet_id</code>, <code>upload_field_map_json</code>.\n\n"
        f"Пример формата JSON (заполните только нужные поля):\n"
        f"<pre><code class=\"language-json\">{example_json_str}</code></pre>\n\n"
        f"Используйте кнопку ниже для отмены.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
        ]),
        parse_mode='HTML'
    )

    await callback.answer()