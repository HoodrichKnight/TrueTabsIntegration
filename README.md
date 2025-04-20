# TrueTabs Integration Bot

This repository contains a Telegram bot and a Rust-based data extraction script that work together to automate data extraction, processing, and scheduling tasks.

---

## README Language Switch

You can switch between English and Russian sections by clicking the language buttons below:

- [English](#english-version)
- [Русский](#русская-версия)

---

## English Version

### Overview

The TrueTabs Integration Bot is a Telegram bot that allows users to manage data extraction tasks, configure data sources, schedule automated jobs, and view upload history. It integrates with a Rust-based data extraction script built with Cargo.

### Features

- Manage data sources (PostgreSQL, MySQL, SQLite, MongoDB, Redis, Elasticsearch, CSV)
- Schedule automated extraction or update jobs with flexible triggers (interval, cron, date)
- View upload history and status
- Configure True Tabs settings
- Get data from True Tabs
- Interactive Telegram UI with inline keyboards
- Weather information integration

### Installation and Setup

#### Prerequisites

- Python 3.8+
- Rust toolchain (Cargo)
- Telegram Bot API token

#### Build Rust Data Extractor

1. Navigate to the `data_extractor` directory:

```bash
# Linux/macOS (bash/zsh)
cd data_extractor
# Windows (PowerShell)
cd data_extractor
# Windows (CMD)
cd data_extractor
```

2. Build the Rust project using Cargo:

```bash
# Linux/macOS (bash/zsh)
cargo build --release
# Windows (PowerShell)
cargo build --release
# Windows (CMD)
cargo build --release
```

The compiled binary will be located in `target/release/`.

#### Setup Python Environment

1. Create and activate a Python virtual environment:

```bash
# Linux/macOS (bash/zsh)
python3 -m venv .venv
source .venv/bin/activate

# Windows (PowerShell)
python -m venv .venv
.venv\Scripts\Activate.ps1

# Windows (CMD)
python -m venv .venv
.venv\Scripts\activate.bat
```

2. Install Python dependencies:

```bash
# Linux/macOS (bash/zsh)
pip install -r telegram_bot/requirements.txt

# Windows (PowerShell)
pip install -r telegram_bot/requirements.txt

# Windows (CMD)
pip install -r telegram_bot/requirements.txt
```

#### Configure Environment Variables

All sensitive and configuration data must be added to a `.env` file in the root directory of the project. This file is used to securely store environment variables that the bot and scripts will use.

To create the `.env` file:

1. In the root directory of the project, create a new file named `.env`.

2. Open the `.env` file in a text editor and add the following variables:

```
BOT_TOKEN="TELEGRAM_BOT_TOKEN"
TRUE_TABS_DATASHEET_ID="TRUE_TABS_DATASHEET_ID" # start with 'dst'
TRUE_TABS_API_TOKEN="TRUE_TABS_API_TOKEN"
ENCRYPTION_KEY="ENCRYPTION_KEY
WEATHER_API_KEY="YOUR_WEATHER_API_KEY"
```

- `BOT_TOKEN`: Your Telegram Bot API token. Obtain it by creating a bot via [BotFather](https://t.me/BotFather) on Telegram.
- `TRUE_TABS_DATASHEET_ID`: Your True Tabs datasheet ID. Obtain this from your True Tabs account or configuration.
- `TRUE_TABS_API_TOKEN`: Your True Tabs API token. Obtain this from your True Tabs account.
- `ENCRYPTION_KEY`: A base64-encoded encryption key used for securing sensitive data. You can generate one using a secure random generator. For example, in Python:

  ```python
  import base64
  import os

  key = base64.urlsafe_b64encode(os.urandom(32))
  print(key.decode())
  ```

- `WEATHER_API_KEY`: API key for the weather service used by the bot. Obtain it from your chosen weather API provider.

Make sure the `.env` file is included in your `.gitignore` to avoid committing sensitive data to version control.

#### Configure the Bot

1. Set your Telegram Bot API token and other configurations in `telegram_bot/config.py` if needed.

2. Ensure the Rust binary path is correctly set in the bot configuration if not using `.env`.

#### Run the Bot

From the project root directory, run:

```bash
# Linux/macOS (bash/zsh)
.venv/bin/python telegram_bot/bot.py

# Windows (PowerShell)
.venv\Scripts\python.exe telegram_bot\bot.py

# Windows (CMD)
.venv\Scripts\python.exe telegram_bot\bot.py
```

The bot will start and connect to Telegram.

---

## Русская версия

### Обзор

TrueTabs Integration Bot — это Telegram-бот, который позволяет управлять задачами извлечения данных, настраивать источники данных, планировать автоматические задания и просматривать историю загрузок. Он интегрируется с Rust-скриптом для извлечения данных, собранным с помощью Cargo.

### Функции

- Управление источниками данных (PostgreSQL, MySQL, SQLite, MongoDB, Redis, Elasticsearch, CSV)
- Планирование автоматических заданий извлечения или обновления с гибкими триггерами (интервал, cron, дата)
- Просмотр истории загрузок и статусов
- Настройка параметров True Tabs
- Получение данных из True Tabs
- Интерактивный Telegram UI с inline-клавиатурами
- Интеграция информации о погоде

### Установка и настройка

#### Требования

- Python 3.8+
- Rust toolchain (Cargo)
- Токен Telegram Bot API

#### Сборка Rust-скрипта

1. Перейдите в директорию `data_extractor`:

```bash
# Linux/macOS (bash/zsh)
cd data_extractor
# Windows (PowerShell)
cd data_extractor
# Windows (CMD)
cd data_extractor
```

2. Соберите проект Rust с помощью Cargo:

```bash
# Linux/macOS (bash/zsh)
cargo build --release
# Windows (PowerShell)
cargo build --release
# Windows (CMD)
cargo build --release
```

Скомпилированный бинарный файл будет находиться в `target/release/`.

#### Настройка Python окружения

1. Создайте и активируйте виртуальное окружение Python:

```bash
# Linux/macOS (bash/zsh)
python3 -m venv .venv
source .venv/bin/activate

# Windows (PowerShell)
python -m venv .venv
.venv\Scripts\Activate.ps1

# Windows (CMD)
python -m venv .venv
.venv\Scripts\activate.bat
```

2. Установите зависимости Python:

```bash
# Linux/macOS (bash/zsh)
pip install -r telegram_bot/requirements.txt

# Windows (PowerShell)
pip install -r telegram_bot/requirements.txt

# Windows (CMD)
pip install -r telegram_bot/requirements.txt
```

#### Настройка переменных окружения

Все конфиденциальные и конфигурационные данные необходимо добавить в файл `.env` в корне проекта. Этот файл используется для безопасного хранения переменных окружения, которые будут использоваться ботом и скриптами.

Чтобы создать файл `.env`:

1. В корневой директории проекта создайте новый файл с именем `.env`.

2. Откройте файл `.env` в текстовом редакторе и добавьте следующие переменные:

```
BOT_TOKEN="TELEGRAM_BOT_TOKEN"
TRUE_TABS_DATASHEET_ID="TRUE_TABS_DATASHEET_ID" # start with 'dst'
TRUE_TABS_API_TOKEN="TRUE_TABS_API_TOKEN"
ENCRYPTION_KEY="ENCRYPTION_KEY
WEATHER_API_KEY="YOUR_WEATHER_API_KEY"
```

- `BOT_TOKEN`: Токен Telegram-бота. Получите его, создав бота через [BotFather](https://t.me/BotFather) в Telegram.
- `TRUE_TABS_DATASHEET_ID`: ID таблицы True Tabs. Получите в вашем аккаунте True Tabs. Начинается с 'dst'
- `TRUE_TABS_API_TOKEN`: API токен True Tabs. Получите в вашем аккаунте True Tabs.
- `ENCRYPTION_KEY`: Ключ шифрования в формате base64 для защиты конфиденциальных данных. Можно сгенерировать с помощью безопасного генератора случайных чисел. Например, в Python:

  ```python
  import base64
  import os

  key = base64.urlsafe_b64encode(os.urandom(32))
  print(key.decode())
  ```

- `WEATHER_API_KEY`: API ключ для сервиса погоды, используемого ботом. Получите у выбранного провайдера погодных данных.

Убедитесь, что файл `.env` добавлен в `.gitignore`, чтобы избежать попадания конфиденциальных данных в систему контроля версий.

#### Настройка бота

1. Установите токен Telegram Bot API и другие параметры в файле `telegram_bot/config.py`, если это необходимо.

2. Убедитесь, что путь к Rust бинарнику правильно указан в конфигурации бота, если не используете `.env`.

#### Запуск бота

Из корневой директории проекта выполните:

```bash
# Linux/macOS (bash/zsh)
.venv/bin/python telegram_bot/bot.py

# Windows (PowerShell)
.venv\Scripts\python.exe telegram_bot\bot.py

# Windows (CMD)
.venv\Scripts\python.exe telegram_bot\bot.py
```

Бот запустится и подключится к Telegram.

---

If you have any questions or need assistance, please open an issue or contact the maintainer.
