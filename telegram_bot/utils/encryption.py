# telegram_bot/utils/encryption.py
from cryptography.fernet import Fernet
import base64
import logging
from ..config import ENCRYPTION_KEY

try:
    fernet = Fernet(ENCRYPTION_KEY) if ENCRYPTION_KEY else None
    if not fernet and ENCRYPTION_KEY: # Проверка на случай, если ключ есть, но невалиден
        logging.error("Не удалось инициализировать Fernet с предоставленным ключом. Шифрование недоступно.")
except Exception as e:
    fernet = None
    logging.error(f"Ошибка инициализации Fernet: {e}. Шифрование недоступно.")


def encrypt_data(data: str) -> str:
    if not fernet:
        logging.warning("Шифрование недоступно (нет ключа или ошибка инициализации). Данные сохраняются без шифрования.")
        return data
    try:
        encrypted_bytes = fernet.encrypt(data.encode())
        return encrypted_bytes.decode()
    except Exception as e:
        logging.error(f"Ошибка шифрования данных: {e}")
        return data


def decrypt_data(encrypted_data: str) -> str:
    if not fernet:
        logging.warning("Дешифрование недоступно (нет ключа или ошибка инициализации). Возвращаются исходные данные (ожидается, что они не были зашифрованы).")
        return encrypted_data
    try:
        decrypted_bytes = fernet.decrypt(encrypted_data.encode())
        return decrypted_bytes.decode()
    except Exception as e:
        logging.error(f"Ошибка дешифрования данных: {e}. Возможно, данные были зашифрованы другим ключом или повреждены.")
        return encrypted_data