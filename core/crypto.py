"""
Шифрование пользовательских секретов (приватный ключ, CLOB-креды).

Ключи хранятся в БД ТОЛЬКО в зашифрованном виде. Ключ шифрования берётся
из переменной окружения SECRETS_ENCRYPTION_KEY и в базе не хранится — то
есть дамп базы сам по себе бесполезен без доступа к окружению.

ВАЖНО про модель угроз: это защита от утечки дампа БД, а НЕ от компромета-
ции сервера. Если атакующий получил доступ к работающему процессу, он
получит и ключ шифрования из окружения. Полноценная защита требует
внешнего KMS (AWS KMS, HashiCorp Vault) с подписью на стороне хранилища.
Для self-hosted (каждый клиент разворачивает бота у себя) этого достаточно.
"""
import base64
import hashlib
import os

from cryptography.fernet import Fernet, InvalidToken
from loguru import logger


def _derive_key() -> bytes | None:
    raw = os.getenv("SECRETS_ENCRYPTION_KEY", "").strip()
    if not raw:
        return None
    # Принимаем как готовый Fernet-ключ, так и произвольную парольную фразу
    try:
        key = raw.encode()
        Fernet(key)
        return key
    except Exception:
        digest = hashlib.sha256(raw.encode()).digest()
        return base64.urlsafe_b64encode(digest)


_KEY = _derive_key()
_fernet = Fernet(_KEY) if _KEY else None

if _fernet is None:
    logger.warning(
        "SECRETS_ENCRYPTION_KEY не задан — секреты пользователей НЕ будут "
        "шифроваться. Задайте его перед приёмом чужих ключей."
    )


def is_enabled() -> bool:
    return _fernet is not None


def encrypt(plaintext: str | None) -> str | None:
    if not plaintext:
        return None
    if _fernet is None:
        raise RuntimeError(
            "SECRETS_ENCRYPTION_KEY не задан — отказываюсь сохранять "
            "секрет в открытом виде"
        )
    return _fernet.encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str | None) -> str | None:
    if not ciphertext:
        return None
    if _fernet is None:
        return None
    try:
        return _fernet.decrypt(ciphertext.encode()).decode()
    except InvalidToken:
        logger.error(
            "Не удалось расшифровать секрет — сменился "
            "SECRETS_ENCRYPTION_KEY? Пользователю нужно ввести данные заново."
        )
        return None


def generate_key() -> str:
    """Сгенерировать новый ключ шифрования (для первичной настройки)."""
    return Fernet.generate_key().decode()