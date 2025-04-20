import asyncio
import logging
import sys
import os
import json
import aiohttp # Need aiohttp for async HTTP requests
from datetime import datetime, timedelta # Need timedelta for forecast periods
from typing import Dict, Any, Optional, List

from aiogram import Bot, Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramAPIError
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import StateFilter

from .. import config # Import config for API key
from ..keyboards.inline import (
    main_menu_keyboard, # For returning to main menu
    weather_menu_keyboard, # NEW weather menu keyboard
    select_forecast_period_keyboard, # NEW forecast period keyboard
)
from ..database.sqlite_db import get_latest_upload_history_by_job_id
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.base import JobLookupError

# Assuming scheduler is initialized somewhere globally or imported
try:
    scheduler = AsyncIOScheduler()
    # Do not start scheduler here to avoid RuntimeError: no running event loop
    # scheduler.start()
except Exception as e:
    scheduler = None
    import logging
    logger = logging.getLogger(__name__)
    logger.error(f"Failed to initialize scheduler in weather_handlers: {e}")


# Get the logger for this module
logger = logging.getLogger(__name__)

router = Router()

# Define FSM states for the weather feature
class WeatherProcess(StatesGroup):
    select_input_method = State() # User selects city or coords (Initial state after clicking weather menu)
    waiting_city_name = State() # Waiting for user to type city name
    waiting_coordinates = State() # Waiting for user to type coordinates
    select_forecast_period = State() # After getting weather, ask for forecast period
    # No specific waiting states for forecast period, handled by callback


# --- Helper function to call OpenWeatherMap API ---
async def get_weather_data(api_key: str, city_name: Optional[str] = None, lat: Optional[float] = None, lon: Optional[float] = None, is_forecast: bool = False) -> Optional[Dict[str, Any]]:
    """
    Calls the OpenWeatherMap API to get current weather or forecast.
    Requires either city_name or lat/lon.
    Returns parsed JSON response or None on error.
    """
    base_url = "http://api.openweathermap.org/data/2.5/"
    endpoint = "forecast" if is_forecast else "weather"

    params = {
        "appid": api_key,
        "units": "metric", # Use metric units (Celsius, m/s)
        "lang": "ru",      # Get descriptions in Russian
    }

    if city_name:
        params["q"] = city_name
    elif lat is not None and lon is not None:
        params["lat"] = lat
        params["lon"] = lon
    else:
        logger.error("Both city_name and lat/lon are missing for weather API call.")
        return None

    # For forecast, OpenWeatherMap free API gives 3-hour interval data
    # The 'cnt' parameter can limit the number of intervals, e.g., 8 for 24 hours (8 * 3h).
    # If is_forecast is True, we might add cnt=40 for max 5 days (40 * 3h).
    if is_forecast:
         params['cnt'] = 40 # Max count for free API forecast

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{base_url}{endpoint}", params=params) as response:
                response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
                data = await response.json()
                logger.info(f"OpenWeatherMap API call success for {city_name or f'{lat},{lon}'}. Endpoint: {endpoint}")
                return data
        except aiohttp.ClientResponseError as e:
             logger.error(f"OpenWeatherMap API returned error {e.status} for {city_name or f'{lat},{lon}'}. Endpoint: {endpoint}. Response: {await e.response.text() if e.response else 'N/A'}", exc_info=True)
             return None # Indicate API error
        except aiohttp.ClientConnectorError as e:
             logger.error(f"OpenWeatherMap API connection error for {city_name or f'{lat},{lon}'}: {e}", exc_info=True)
             return None # Indicate connection error
        except Exception as e:
            logger.error(f"Error during OpenWeatherMap API call for {city_name or f'{lat},{lon}'}: {e}", exc_info=True)
            return None # Indicate other errors


# --- Helper function to format weather data ---
def format_weather_data(data: Dict[str, Any], period: str = "now") -> str:
    """Formats weather data from OpenWeatherMap API for Telegram message."""
    if not data:
        return "Не удалось получить данные о погоде."

    # --- Formatting for Current Weather (period='now') ---
    if period == 'now' and 'main' in data and 'weather' in data:
        main_data = data['main']
        weather_desc = data['weather'][0]
        wind_data = data.get('wind', {})
        city_name = data.get('name', 'Неизвестный город')

        text = f"☀️ Погода в городе <b>{city_name}</b> сейчас:\n"
        text += f"Температура: {main_data.get('temp', 'N/A')}°C\n"
        text += f"Ощущается как: {main_data.get('feels_like', 'N/A')}°C\n"
        text += f"Описание: {weather_desc.get('description', 'N/A')}\n"
        text += f"Скорость ветра: {wind_data.get('speed', 'N/A')} м/с\n"
        if 'gust' in wind_data: text += f"Порывы ветра: {wind_data['gust']} м/с\n"
        text += f"Влажность: {main_data.get('humidity', 'N/A')}%\n"
        text += f"Давление: {main_data.get('pressure', 'N/A')} гПа" # OpenWeatherMap uses hPa (hectopascal)

        return text

    # --- Formatting for Forecast ---
    elif period in ['3h', 'today', '1d', '3d', '7d', '30d'] and 'list' in data and 'city' in data:
        forecast_list = data['list']
        city_name = data['city'].get('name', 'Неизвестный город')

        text = f"☀️ Прогноз погоды для города <b>{city_name}</b> ({period}):\n\n"

        now = datetime.now(datetime.now().astimezone().tzinfo) # Get current time with timezone

        if period == '3h':
            # Take the next forecast entry (usually starts soon after 'now')
            if forecast_list:
                 entry = forecast_list[0]
                 dt_txt = entry.get('dt_txt', 'N/A')
                 temp = entry['main'].get('temp', 'N/A') if 'main' in entry else 'N/A'
                 desc = entry['weather'][0].get('description', 'N/A') if 'weather' in entry and entry['weather'] else 'N/A'
                 wind_speed = entry.get('wind', {}).get('speed', 'N/A')
                 text += f"{dt_txt}: {temp}°C, {desc}, Ветер: {wind_speed} м/с"
            else:
                 text += "Прогноз на ближайшие 3 часа недоступен."

        elif period == 'today':
            # Summarize forecast until the end of the current day
            end_of_day = now.replace(hour=23, minute=59, second=59)
            today_forecast = [entry for entry in forecast_list if datetime.fromisoformat(entry['dt_txt']) <= end_of_day]

            if today_forecast:
                # Calculate min/max temp, average wind, summarize conditions
                min_temp = min(entry['main']['temp_min'] for entry in today_forecast if 'main' in entry and 'temp_min' in entry['main']) if today_forecast else 'N/A'
                max_temp = max(entry['main']['temp_max'] for entry in today_forecast if 'main' in entry and 'temp_max' in entry['main']) if today_forecast else 'N/A'
                avg_wind = sum(entry['wind']['speed'] for entry in today_forecast if 'wind' in entry and 'speed' in entry['wind']) / len(today_forecast) if today_forecast and any('wind' in entry and 'speed' in entry['wind'] for entry in today_forecast) else 'N/A'
                # Simple mode summary (take most frequent description)
                descriptions = [entry['weather'][0]['description'] for entry in today_forecast if 'weather' in entry and entry['weather'] and entry['weather'][0].get('description')]
                desc_summary = max(set(descriptions), key=descriptions.count) if descriptions else 'Различно'


                text += f"Минимальная темп.: {min_temp}°C\n"
                text += f"Максимальная темп.: {max_temp}°C\n"
                text += f"Средний ветер: {avg_wind:.2f} м/с\n" if isinstance(avg_wind, (int, float)) else f"Средний ветер: {avg_wind} м/с\n"
                text += f"Условия: {desc_summary}"

            else:
                 text += "Прогноз на сегодня недоступен."


    elif period in ['1d', '3d', '7d', '30d']:
        # Summarize forecast for X days
        days = int(period[:-1]) # Extract number of days (1, 3, 7, 30)
        end_date = now + timedelta(days=days)

        # Filter forecast entries within the requested period
        period_forecast = [entry for entry in forecast_list if datetime.fromisoformat(entry['dt_txt']) <= end_date]

        if not period_forecast:
             text += f"Прогноз на {days} дней недоступен."
             if days > 5: text += " (Бесплатная API предоставляет прогноз максимум на 5 дней)."
        else:
            # Group forecast entries by day
            forecast_by_day: Dict[str, List[Dict[str, Any]]] = {}
            for entry in period_forecast:
                date_str = entry['dt_txt'].split(' ')[0] # 'YYYY-MM-DD'
                if date_str not in forecast_by_day:
                    forecast_by_day[date_str] = []
                forecast_by_day[date_str].append(entry)

            # Summarize each day
            for date_str, daily_entries in forecast_by_day.items():
                try:
                     min_temp = min(entry['main']['temp_min'] for entry in daily_entries if 'main' in entry and 'temp_min' in entry['main']) if daily_entries else 'N/A'
                     max_temp = max(entry['main']['temp_max'] for entry in daily_entries if 'main' in entry and 'temp_max' in entry['main']) if daily_entries else 'N/A'
                     avg_wind = sum(entry['wind']['speed'] for entry in daily_entries if 'wind' in entry and 'speed' in entry['wind']) / len(daily_entries) if daily_entries and any('wind' in entry and 'speed' in entry['wind'] for entry in daily_entries) else 'N/A'
                     descriptions = [entry['weather'][0]['description'] for entry in daily_entries if 'weather' in entry and entry['weather'] and entry['weather'][0].get('description')]
                     desc_summary = max(set(descriptions), key=descriptions.count) if descriptions else 'Различно'

                     text += f"<b>{date_str}</b>: {min_temp}°C ... {max_temp}°C, {desc_summary}, Ветер: {avg_wind:.2f} м/с\n" if isinstance(avg_wind, (int, float)) else f"<b>{date_str}</b>: {min_temp}°C ... {max_temp}°C, {desc_summary}, Ветер: {avg_wind} м/с\n"
                except Exception as e:
                     logger.error(f"Error summarizing daily forecast for {date_str}: {e}", exc_info=True)
                     text += f"<b>{date_str}</b>: Ошибка суммирования данных.\n"
    elif period == '1h':
        # Summarize forecast for next 1 hour (approximate using first forecast entry)
        if forecast_list:
            entry = forecast_list[0]
            dt_txt = entry.get('dt_txt', 'N/A')
            temp = entry['main'].get('temp', 'N/A') if 'main' in entry else 'N/A'
            desc = entry['weather'][0].get('description', 'N/A') if 'weather' in entry and entry['weather'] else 'N/A'
            wind_speed = entry.get('wind', {}).get('speed', 'N/A')
            text += f"Прогноз на следующий час ({dt_txt}): {temp}°C, {desc}, Ветер: {wind_speed} м/с"
        else:
            text += "Прогноз на следующий час недоступен."

        # Add note about API limitations if applicable
        if days > 5 and period in ['7d', '30d']: # Assuming 5-day limit for free API
             text += "\n<i>(Прогноз на >5 дней может быть неточным или недоступен в бесплатной версии API)</i>"


        return text


    else:
        # Handle unexpected data structure
        logger.error(f"Unexpected weather data structure for period '{period}': {data}")
        return "Не удалось обработать данные о погоде."


# --- Handlers for Weather Feature ---

# Handler for "weather_menu" callback
@router.callback_query(F.data == "weather_menu")
async def weather_menu_handler(callback: CallbackQuery, state: FSMContext):
    """Displays the weather menu."""
    # Clear any previous weather state data
    await state.clear()
    await state.set_state(WeatherProcess.select_input_method)

    await callback.message.edit_text(
        "Выберите способ определения местоположения для погоды:",
        reply_markup=weather_menu_keyboard()
    )
    await callback.answer()

# Handler for "weather_by_city" callback
@router.callback_query(F.data == "weather_by_city", WeatherProcess.select_input_method)
async def weather_by_city_handler(callback: CallbackQuery, state: FSMContext):
    """Prompts the user to enter city name."""
    await state.set_state(WeatherProcess.waiting_city_name)
    await callback.message.edit_text("Введите название города:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]) )
    await callback.answer()

# Handler for waiting_city_name state
@router.message(WeatherProcess.waiting_city_name)
async def process_city_name_for_weather(message: Message, state: FSMContext):
    """Processes the entered city name, fetches weather, and asks for forecast period."""
    city_name = message.text.strip()
    if not city_name:
        await message.answer("Название города не может быть пустым. Введите название города:")
        return

    # Fetch current weather data by city name
    weather_data = await get_weather_data(config.WEATHER_API_KEY, city_name=city_name)

    if weather_data is None:
        # Handle API errors or city not found
        # OpenWeatherMap returns 404 for city not found, which get_weather_data handles by returning None
        await message.answer(f"Не удалось найти город '{city_name}' или получить данные о погоде. Проверьте название или попробуйте позже.")
        # Stay in the same state to allow re-entering city name, or go back to menu?
        # Let's go back to menu for simplicity now.
        await state.clear() # Clear state
        await message.answer("Выберите способ определения местоположения:", reply_markup=weather_menu_keyboard())
        return

    # Format and send current weather
    current_weather_text = format_weather_data(weather_data, period='now')
    await message.answer(current_weather_text, parse_mode='HTML')

    # Store location data for forecast step
    # We need city name or lat/lon for forecast API call later
    # If we got weather data, OpenWeatherMap response for city includes lat/lon
    lat = weather_data.get('coord', {}).get('lat')
    lon = weather_data.get('coord', {}).get('lon')
    await state.update_data(weather_location={'city_name': city_name, 'lat': lat, 'lon': lon})


    # Transition to selecting forecast period
    await state.set_state(WeatherProcess.select_forecast_period)
    await message.answer(
        "Выберите период прогноза:",
        reply_markup=select_forecast_period_keyboard()
    )

# Handler for "weather_by_coords" callback
@router.callback_query(F.data == "weather_by_coords", WeatherProcess.select_input_method)
async def weather_by_coords_handler(callback: CallbackQuery, state: FSMContext):
    """Prompts the user to enter coordinates."""
    await state.set_state(WeatherProcess.waiting_coordinates)
    await callback.message.edit_text("Введите координаты в формате `широта,долгота` (например, `51.5074,0.1278`):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]) )
    await callback.answer()

# Handler for waiting_coordinates state
@router.message(WeatherProcess.waiting_coordinates)
async def process_coordinates_for_weather(message: Message, state: FSMContext):
    """Processes the entered coordinates, fetches weather, and asks for forecast period."""
    coords_input = message.text.strip()
    try:
        lat_str, lon_str = coords_input.split(',')
        lat = float(lat_str.strip())
        lon = float(lon_str.strip())

        # Basic validation for lat/lon ranges
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
             raise ValueError("Invalid range")

    except ValueError:
        await message.answer("Неверный формат координат. Введите в формате `широта,долгота` (например, `51.5074,0.1278`):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]) )
        return

    # Fetch current weather data by coordinates
    weather_data = await get_weather_data(config.WEATHER_API_KEY, lat=lat, lon=lon)

    if weather_data is None:
        await message.answer(f"Не удалось получить данные о погоде для координат {lat},{lon}. Попробуйте позже.")
        await state.clear() # Clear state
        await message.answer("Выберите способ определения местоположения:", reply_markup=weather_menu_keyboard())
        return

    # Format and send current weather
    current_weather_text = format_weather_data(weather_data, period='now')
    await message.answer(current_weather_text, parse_mode='HTML')

    # Store location data for forecast step
    await state.update_data(weather_location={'lat': lat, 'lon': lon}) # Don't have city name easily from coords API response


    # Transition to selecting forecast period
    await state.set_state(WeatherProcess.select_forecast_period)
    await message.answer(
        "Выберите период прогноза:",
        reply_markup=select_forecast_period_keyboard()
    )


# Handler for select_forecast_period state (callback from forecast period keyboard)
@router.callback_query(F.data.startswith("weather_period:"), WeatherProcess.select_forecast_period)
async def process_forecast_period(callback: CallbackQuery, state: FSMContext):
    """Processes the selected forecast period and fetches/displays the forecast."""
    period = callback.data.split(":")[1] # 'now', '3h', 'today', '1d', etc.

    # If 'now' was selected again, fetch current weather (should not happen with current keyboard)
    if period == 'now':
        # This case should ideally be handled by the initial city/coords lookup, but for robustness
        # Re-fetch current weather based on stored location
        state_data = await state.get_data()
        location = state_data.get('weather_location')
        if not location:
             await callback.message.edit_text("Ошибка получения местоположения для текущей погоды.", reply_markup=weather_menu_keyboard())
             await state.clear()
             await callback.answer()
             return
        weather_data = await get_weather_data(config.WEATHER_API_KEY, city_name=location.get('city_name'), lat=location.get('lat'), lon=location.get('lon'))
        if weather_data:
            await callback.message.edit_text(format_weather_data(weather_data, period='now'), parse_mode='HTML')
        else:
             await callback.message.edit_text("Не удалось получить текущую погоду.", reply_markup=weather_menu_keyboard())

        # Stay in select_forecast_period state to allow selecting another period
        await callback.answer()
        return


    # For any other period, fetch forecast data
    state_data = await state.get_data()
    location = state_data.get('weather_location')

    if not location:
        await callback.message.edit_text("Ошибка получения местоположения для прогноза.", reply_markup=weather_menu_keyboard())
        await state.clear()
        await callback.answer()
        return

    # Fetch forecast data
    forecast_data = await get_weather_data(config.WEATHER_API_KEY, city_name=location.get('city_name'), lat=location.get('lat'), lon=location.get('lon'), is_forecast=True)

    if forecast_data is None:
        await callback.message.edit_text(f"Не удалось получить прогноз погоды на период {period}.", reply_markup=weather_menu_keyboard())
        await state.clear() # Clear state
        await callback.answer()
        return

    # Format and send forecast
    forecast_text = format_weather_data(forecast_data, period=period)
    await callback.message.edit_text(forecast_text, reply_markup=select_forecast_period_keyboard(), parse_mode='HTML') # Stay in select_forecast_period state
    await callback.answer()


# --- Keep existing scheduled job execution functions ---
# async def scheduled_task_executor(bot: Bot, chat_id: int, ...): ...
# async def run_rust_task_for_scheduled_job(...): ...

# --- Keep existing scheduled job UI handlers (menu, list, add FSM steps) ---
# @router.callback_query(F.data == "manage_schedules") async def manage_schedules_menu_handler(...): ...
# @router.callback_query(F.data == "list_schedules") async def list_schedules_handler(...): ...
# @router.callback_query(F.data == "add_schedule") async def start_add_schedule_handler(...): ...
# @router.message(ScheduleProcess.waiting_schedule_name) async def process_schedule_name(...): ...
# @router.callback_query(F.data.startswith("select_schedule_action:"), ScheduleProcess.select_schedule_action) async def select_schedule_action_handler(...): ...
# @router.callback_query(F.data.startswith("select_config:schedule_source_select:"), ScheduleProcess.select_schedule_source_config) async def process_schedule_source_config_selection(...): ...
# @router.callback_query(F.data.startswith("select_config:schedule_tt_select:"), ScheduleProcess.select_schedule_tt_config) async def process_schedule_tt_config_selection(...): ...
# @router.callback_query(F.data.startswith("select_trigger_type:"), ScheduleProcess.select_schedule_trigger_type) async def select_schedule_trigger_type_handler(...): ...
# @router.message(ScheduleProcess.waiting_interval_args) async def process_interval_args(...): ...
# @router.callback_query(F.data == "confirm_create_schedule", ScheduleProcess.confirm_schedule) async def confirm_schedule_handler(...): ...
# @router.callback_query(F.data.startswith("view_schedule_details:")) async def view_schedule_details_handler(...): ... # To be updated
# @router.callback_query(F.data.startswith("delete_schedule_confirm:")) async def confirm_delete_schedule_handler(...): ... # To be updated
# @router.callback_query(F.data.startswith("delete_schedule:")) async def delete_schedule_handler(...): ... # To be updated
# @router.callback_query(F.data.startswith("toggle_schedule_enabled:")) async def toggle_schedule_enabled_handler(...): ... # To be updated


# --- Update existing scheduled handlers (view, delete, toggle) to use new keyboards and logic ---

@router.callback_query(F.data.startswith("view_schedule_details:"))
async def view_schedule_details_handler(callback: CallbackQuery):
    """Displays details of a specific scheduled job and action buttons."""
    job_id = callback.data.split(":")[1]
    job = await sqlite_db.get_scheduled_job(job_id)

    if not job:
        await callback.message.edit_text("❌ Ошибка: Запланированное задание не найдено.", reply_markup=manage_schedules_menu_keyboard())
        await callback.answer("Задание не найдено.")
        return

    # Format job details for display
    job_name = job.get('name', 'Без имени')
    action = job.get('action', 'Без действия')
    source_config_name = job.get('source_config_name', 'Без источника')
    tt_config_name = job.get('tt_config_name', 'Без TT')
    trigger_type = job.get('trigger_type', 'Неизвестный')
    trigger_args_json = job.get('trigger_args_json', '{}')
    is_enabled = job.get('enabled', False)
    created_at_str = job.get('created_at', 'Неизвестно')

    try:
        trigger_args = json.loads(trigger_args_json)
        formatted_trigger = format_trigger_args(trigger_type, trigger_args) # Use helper
    except Exception as e:
         logger.error(f"Error formatting trigger args for job {job_id}: {e}", exc_info=True)
         formatted_trigger = "Неверный формат триггера"


    # Получаем время следующего запуска из APScheduler
    next_run_time_str = "Неизвестно"
    if scheduler: # Ensure scheduler is initialized
        try:
            aps_job = scheduler.get_job(job_id)
            if aps_job and aps_job.next_run_time:
                # Format datetime nicely, include timezone if possible
                next_run_time_str = aps_job.next_run_time.strftime('%Y-%m-%d %H:%M:%S %Z')
            elif aps_job and not aps_job.next_run_time:
                next_run_time_str = "Нет запланированных запусков (возможно, завершено)"
            else:
                next_run_time_str = "Задание не найдено в планировщике"
        except JobLookupError:
            next_run_time_str = "Задание не найдено в планировщике"
        except Exception as e:
            logger.error(f"Error getting next_run_time for job {job_id}: {e}", exc_info=True)
            next_run_time_str = "Ошибка получения времени"
    else:
        next_run_time_str = "Планировщик недоступен"

    # Получаем информацию о последнем выполнении из истории по job_id
    last_execution = await get_latest_upload_history_by_job_id(job_id)
    if last_execution:
        last_exec_status = "✅ Успех" if last_execution.get('status') == 'SUCCESS' else "❌ Ошибка"
        last_exec_time = last_execution.get('timestamp', 'Неизвестно')
        try:
            last_exec_time = datetime.fromisoformat(last_exec_time).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            pass
        last_exec_info = f"\nПоследнее выполнение: {last_exec_status} в {last_exec_time}"
    else:
        last_exec_info = "\nПоследнее выполнение: Нет данных"

    details_text = (
        f"📅 <b>Детали запланированного задания:</b>\n\n"
        f"Имя: <b>{job_name}</b>\n"
        f"ID задания: <code>{job_id}</code>\n" # Show job ID for debugging/reference
        f"Статус: {'✅ Включено' if is_enabled else '❌ Отключено'}\n"
        f"Действие: <b>{action}</b>\n"
        f"Источник: <b>{source_config_name}</b>\n"
        f"True Tabs: <b>{tt_config_name}</b>\n"
        f"Тип расписания: <b>{trigger_type.capitalize()}</b>\n"
        f"Параметры триггера: <code>{formatted_trigger}</code>\n" # Use formatted trigger
        f"Создано: {created_at_str}\n"
        f"Следующий запуск: {next_run_time_str}"
        f"{last_exec_info}"
    )

    reply_markup = schedule_details_keyboard(job_id, is_enabled) # Pass job_id and status

    await callback.message.edit_text(details_text, reply_markup=reply_markup, parse_mode='HTML')
    await callback.answer()


# Handler for "delete_schedule_confirm:{job_id}" callback (without changes)
# @router.callback_query(F.data.startswith("delete_schedule_confirm:")) async def confirm_delete_schedule_handler(...): ...

# Handler for "delete_schedule:{job_id}" callback (without changes)
# @router.callback_query(F.data.startswith("delete_schedule:")) async def delete_schedule_handler(...): ...

# Handler for "toggle_schedule_enabled:{job_id}" callback (without changes)
# @router.callback_query(F.data.startswith("toggle_schedule_enabled:")) async def toggle_schedule_enabled_handler(...): ...