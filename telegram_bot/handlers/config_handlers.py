import json
from pathlib import Path
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import re
import validators

from ..keyboards import (
    main_menu_keyboard,
    source_selection_keyboard,
    manage_configs_menu_keyboard,
    manage_source_configs_keyboard,
    manage_tt_configs_keyboard,
    config_actions_keyboard,
    delete_confirm_keyboard,
    select_input_method_keyboard
)
from ..database.sqlite_db import (
    add_source_config, get_source_config, list_source_configs, delete_source_config, update_source_config,
    add_tt_config, get_tt_config, list_tt_configs, delete_tt_config, update_tt_config,
    set_default_source_config, get_default_source_config, set_default_tt_config, get_default_tt_config
)
from .shared_constants import SOURCE_PARAMS_ORDER, get_friendly_param_name


router = Router()

class ConfigProcess(StatesGroup):
    waiting_config_name = State()
    waiting_source_config_type = State()
    waiting_source_param = State()
    waiting_tt_param = State()


# ... (Остальная часть файла: manage_configs_menu, manage_source_configs_menu, manage_tt_configs_menu, start_add_source_config, start_add_tt_config, start_edit_config_handler, process_config_name, process_source_config_type) ...

# --- Обработка ввода параметров конфигурации источника (с обновленной валидацией) ---
@router.message(ConfigProcess.waiting_source_param)
async def process_source_param(message: Message, state: FSMContext):
    user_input = message.text.strip()

    state_data = await state.get_data()
    mode = state_data.get('mode', 'add')
    param_keys_order = state_data['param_keys_order']
    current_param_index = state_data['current_param_index']
    current_params = state_data['current_params']
    source_type = state_data['source_type']
    config_name = state_data['name']


    current_param_key = param_keys_order[current_param_index]

    if mode == 'edit' and user_input == '-':
        print(f"Редактирование '{config_name}': Параметр '{current_param_key}' - текущее значение сохранено.")
    else:
        validation_error = None
        friendly_param_name = get_friendly_param_name(current_param_key)

        # Базовая валидация на пустоту (для необязательных полей)
        # Удалены 'neo4j_pass', 'couchbase_pass' из списка исключений
        if not user_input and current_param_key not in ['redis_pattern', 'upload_field_map_json']:
             validation_error = f"Параметр '{friendly_param_name}' не может быть пустым."

        # --- УЛУЧШЕННАЯ ВАЛИДАЦИЯ ---
        elif current_param_key == 'source_url':
            # Список поддерживаемых источников для валидации URL/URI
            if source_type in ['postgres', 'mysql', 'redis', 'mongodb', 'elasticsearch']: # Удалены: cassandra, neo4j, couchbase
                is_valid_basic_url = validators.url(user_input, public=True) is True

                if not is_valid_basic_url: # Базовая проверка формата URL для поддерживаемых типов
                     validation_error = f"Неверный формат URL для {source_type} ('{friendly_param_name}')."

                # Дополнительные проверки специфичных схем URL для поддерживаемых типов
                if source_type in ['postgres', 'mysql'] and not re.search(r".+://.+@.+:.+/.+", user_input):
                    validation_error = f"Неверный формат URL для {source_type}. Ожидается формат типа схема://пользователь:пароль@хост:порт/базаданных."
                elif source_type == 'redis' and not user_input.lower().startswith(('redis://', 'rediss://')):
                    validation_error = f"Неверная схема URL для Redis. Ожидается 'redis://' или 'rediss://'."
                elif source_type == 'mongodb' and not user_input.lower().startswith(('mongodb://', 'mongodb+srv://')):
                    validation_error = f"Неверная схема URI для MongoDB. Ожидается 'mongodb://' или 'mongodb+srv://'."
                elif source_type == 'elasticsearch' and not user_input.lower().startswith(('http://', 'https://')):
                    validation_error = f"Неверная схема URL для Elasticsearch. Ожидается 'http://' или 'https://'."

            # Валидация пути к файлу для файловых источников (остается без изменений)
            elif source_type in ['sqlite', 'csv']: # Removed excel
                 if not Path(user_input).is_file():
                      validation_error = f"Файл по пути '{user_input}' не найден или это не файл."
            # Removed excel specific validation
            # elif source_type == 'excel' and not (user_input.lower().endswith('.xlsx') or user_input.lower().endswith('.xls')):
            #      validation_error = f"Файл должен быть в формате .xlsx или .xls."
                 elif source_type == 'csv' and not user_input.lower().endswith('.csv'):
                      validation_error = f"Файл должен быть в формате .csv."


        # Валидация имен БД, коллекций, индексов, организаций, бакетов
        # Удалены: org, bucket из списка специфических проверок, т.к. они больше не используются для удаленных источников
        # Но если org/bucket могут быть параметрами для других источников, нужно вернуть их.
        # Судя по Rust Args, org и bucket используются с Cassandra/Couchbase, которые удалены.
        # Проверка на недопустимые символы в именах остается общей.
        elif current_param_key in ['db_name', 'collection_name', 'index', 'mongo_db', 'mongo_collection', 'es_index']:
             # Удалены: 'org', 'bucket' из этого списка
             if not re.match(r"^[a-zA-Z0-9._-]+$", user_input):
                  validation_error = f"Неверный формат имени для '{friendly_param_name}'. Допускаются латинские буквы, цифры, '.', '-', '_'."

        # Валидация JSON запроса для Elasticsearch (остается без изменений)
        elif current_param_key == 'es_query':
             if not is_valid_json(user_input):
                  validation_error = f"Неверный формат JSON для параметра '{friendly_param_name}'."

        # Валидация паттерна Redis (остается без изменений)
        elif current_param_key == 'redis_pattern':
             if re.search(r"[\x00-\x1F\x7F]", user_input):
                  validation_error = f"Неверный формат паттерна для Redis. Содержит недопустимые символы."

        # Удалены специфические валидации для параметров удаленных источников (cassandra_addresses, neo4j_pass, couchbase_pass и т.д.)
        # Если в дальнейшем добавятся другие источники с этими параметрами, валидацию нужно будет добавить снова.


        if validation_error:
             await message.answer(f"Ошибка валидации: {validation_error}\nПожалуйста, введите параметр '{friendly_param_name}' снова:")
             return

        current_params[current_param_key] = user_input
        await state.update_data(current_params=current_params)


    next_param_index = current_param_index + 1

    if next_param_index < len(param_keys_order):
        await state.update_data(current_param_index=next_param_index)
        next_param_key = param_keys_order[next_param_index]
        next_friendly_name = get_friendly_param_name(next_param_key)

        next_current_value = current_params.get(next_param_key)
        next_current_value_display = "<code>Нет данных</code>"
        if next_current_value is not None and next_current_value != "":
             # Удалены 'neo4j_pass', 'couchbase_pass' из списка скрываемых паролей
             if next_param_key in ['upload_api_token', 'source_pass']:
                  next_current_value_display = "<code>***</code>"
             elif isinstance(next_current_value, (dict, list)):
                 try:
                      next_current_value_display = f"<pre><code class=\"language-json\">{json.dumps(next_current_value, indent=2, ensure_ascii=False)}</code></pre>"
                 except:
                      next_current_value_display = "<code>Некорректный JSON</code>"
             else:
                 next_current_value_display = f"<code>{next_current_value}</code>"


        message_text = f"Введите параметр '{next_friendly_name}':"
        if mode == 'edit':
             message_text = (
                 f"Редактирование конфигурации '{config_name}'.\n"
                 f"Текущее значение параметра '{next_friendly_name}': {next_current_value_display}\n"
                 f"Введите новое значение (или отправьте '-' для сохранения текущего):"
             )


        await state.set_state(ConfigProcess.waiting_source_param)
        await message.answer(message_text, parse_mode='HTML')

    else:
        if mode == 'add':
            success = await add_source_config(config_name, source_type, current_params)
            await state.clear()
            if success:
                 await message.answer(f"Конфигурация источника '{config_name}' ({source_type}) успешно добавлена.", reply_markup=manage_source_configs_keyboard())
            else:
                 await message.answer(f"Ошибка при добавлении конфигурации источника '{config_name}'. Конфигурация с таким именем уже существует?", reply_markup=manage_source_configs_keyboard())

        elif mode == 'edit':
            success = await update_source_config(config_name, source_type, current_params)
            await state.clear()
            if success:
                 await message.answer(f"Конфигурация источника '{config_name}' ({source_type}) успешно обновлена.", reply_markup=manage_source_configs_keyboard())
            else:
                 await message.answer(f"Ошибка при обновлении конфигурации источника '{config_name}'.", reply_markup=manage_source_configs_keyboard())


# ... (Остальная часть файла: process_tt_param и далее - без изменений) ...