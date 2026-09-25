"""Простой мост для отправки уведомлений из фоновых сервисов в Telegram."""
import asyncio
import time
from loguru import logger

_bot = None
# Держим ссылки на фоновые отправки, иначе сборщик мусора может
# оборвать задачу до того, как сообщение уйдёт
_pending: set[asyncio.Task] = set()


def set_bot(bot_instance):
    global _bot
    _bot = bot_instance


# Ограничения Telegram: около 1 сообщения в секунду в ОДИН чат и около
# 30 в секунду на бота в целом. Если их превысить, Telegram притормаживает
# весь бот — включая получение обновлений. В логе это выглядело как
# "Flood control exceeded on method 'GetUpdates'": серия сделок трейдера
# порождала пачку уведомлений, отправленных одновременно, и бот уходил в
# ограничение целиком.
_PER_CHAT_INTERVAL = 1.05      # секунд между сообщениями в один чат
_GLOBAL_INTERVAL = 0.05        # ~20 сообщений в секунду на весь бот
_MAX_ATTEMPTS = 4

_chat_locks: dict[int, asyncio.Lock] = {}
_chat_last_sent: dict[int, float] = {}
_global_lock = asyncio.Lock()
_global_last_sent = 0.0


async def _pace(user_id: int):
    """Выдержать паузы: и в этот чат, и по боту в целом."""
    global _global_last_sent
    last = _chat_last_sent.get(user_id, 0.0)
    wait = last + _PER_CHAT_INTERVAL - time.monotonic()
    if wait > 0:
        await asyncio.sleep(wait)
    async with _global_lock:
        gwait = _global_last_sent + _GLOBAL_INTERVAL - time.monotonic()
        if gwait > 0:
            await asyncio.sleep(gwait)
        _global_last_sent = time.monotonic()


async def _send(user_id: int, text: str):
    # Логируем И успех тоже: иначе по логу не отличить "отправили, но
    # не дошло" от "вообще не пытались отправить".
    head = text.split("\n", 1)[0][:60]

    # Сообщения в ОДИН чат идут строго по очереди — сохраняется порядок
    # (сначала "скопирована сделка", потом "стоп-лосс") и темп.
    lock = _chat_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _chat_locks[user_id] = lock

    async with lock:
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            await _pace(user_id)
            try:
                await _bot.send_message(user_id, text, parse_mode="HTML")
                _chat_last_sent[user_id] = time.monotonic()
                logger.info(f"notify -> {user_id}: {head}")
                return
            except Exception as e:
                name = type(e).__name__
                retry_after = getattr(e, "retry_after", None)

                if name == "TelegramRetryAfter" and retry_after:
                    # Telegram прямо говорит, сколько ждать
                    logger.warning(
                        f"notify -> {user_id}: Telegram просит подождать "
                        f"{retry_after}с (попытка {attempt})"
                    )
                    await asyncio.sleep(float(retry_after) + 0.5)
                    continue

                if name in ("TelegramServerError", "TelegramNetworkError") \
                        and attempt < _MAX_ATTEMPTS:
                    # Bad Gateway и сетевые сбои — временные, стоит повторить
                    await asyncio.sleep(1.5 * attempt)
                    continue

                # Прочее (пользователь заблокировал бота, неверный HTML
                # и т.п.) повтором не лечится
                logger.error(
                    f"notify FAILED -> {user_id} ({name}: {e}) | {head}"
                )
                return

        logger.error(
            f"notify FAILED -> {user_id}: не удалось за {_MAX_ATTEMPTS} "
            f"попытки | {head}"
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