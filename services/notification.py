"""Простой мост для отправки уведомлений из фоновых сервисов в Telegram."""
import asyncio
from loguru import logger

_bot = None
# Держим ссылки на фоновые отправки, иначе сборщик мусора может
# оборвать задачу до того, как сообщение уйдёт
_pending: set[asyncio.Task] = set()


def set_bot(bot_instance):
    global _bot
    _bot = bot_instance


async def _send(user_id: int, text: str):
    # Логируем И успех тоже. Раньше при успешной отправке не писалось
    # ничего, и когда пользователи сказали "уведомления не приходили",
    # по логу нельзя было отличить "отправили, но не дошло" от
    # "вообще не пытались отправить" — в логе просто пусто.
    head = text.split("\n", 1)[0][:60]
    try:
        await _bot.send_message(user_id, text, parse_mode="HTML")
        logger.info(f"notify -> {user_id}: {head}")
    except Exception as e:
        logger.error(
            f"notify FAILED -> {user_id} ({type(e).__name__}: {e}) | {head}"
        )


def notify_user_bg(user_id: int, text: str) -> None:
    """
    Отправить уведомление ФОНОМ, не блокируя вызывающий код.

    Зачем: Telegram у вас регулярно недоступен (в логах сплошные
    "Request timeout error" / "Cannot connect to host api.telegram.org").
    Раньше notify_user ждали через await прямо внутри копирования —
    внутри открытой сессии БД и под локом (user, token). Пока
    Telegram отваливался по таймауту (5+ секунд), эта сделка держала
    и соединение с базой, и лок, тормозя следующие сделки по тому же
    рынку. Отправка результата пользователю не должна замедлять
    торговлю, поэтому уходит в отдельную задачу.
    """
    if _bot is None:
        logger.error(
            f"notify НЕ ОТПРАВЛЕНО -> {user_id}: бот ещё не "
            f"инициализирован (set_bot не вызван)"
        )
        return
    task = asyncio.create_task(_send(user_id, text))
    _pending.add(task)
    task.add_done_callback(_pending.discard)


async def notify_user(user_id: int, text: str):
    """Совместимость со старыми вызовами: тоже не блокирует."""
    notify_user_bg(user_id, text)