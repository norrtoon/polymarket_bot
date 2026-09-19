import asyncio
import signal
import sys

# uvloop — замена стандартного цикла событий на реализацию поверх
# libuv. Бот почти целиком состоит из ожидания сети (Redis, Postgres,
# HTTP к бирже, вебсокеты), а именно на таких нагрузках uvloop даёт
# заметный выигрыш по сравнению со стандартным asyncio. Ставится
# прозрачно: если пакет не установлен, всё работает как раньше.
try:
    import uvloop
    uvloop.install()
    _UVLOOP = True
except ImportError:
    _UVLOOP = False
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from loguru import logger

# Пишем логи через очередь в отдельном потоке.
#
# По умолчанию loguru пишет в stdout СИНХРОННО, а в Docker поток
# уходит через лог-драйвер контейнера. Каждый logger.info() в горячем
# пути (обнаружение сделки, тайминги, уведомления) блокировал цикл
# событий на время записи. При копировании это время добавлялось
# прямо к задержке.
logger.remove()
logger.add(
    sys.stderr,
    enqueue=True,           # запись в отдельном потоке, не блокирует loop
    backtrace=False,
    diagnose=False,
    level="INFO",
)

from core.config import settings
from core.database import init_models, async_session
from core.redis_client import redis_client
from bot.middlewares.auth import AuthMiddleware
from bot.middlewares.ensure_user import EnsureUserMiddleware
from bot.handlers import (
    start, wallet, settings as settings_h, positions, onboarding
)
from services.trader import trader_service
from services.tp_sl_monitor import tp_sl_monitor
from services.reconciliation import reconciliation_service
from services.notification import set_bot
from poly.geoblock import geoblock_checker
from poly.client import polymarket_client
import poly.ws_user as ws_user_module


def _handle_task_exception(task: asyncio.Task) -> None:
    """Логируем исключения фоновых задач вместо тихого падения"""
    if task.cancelled():
        return
    exc = task.exception()
    if exc:
        logger.error(f"Background task '{task.get_name()}' failed: {exc}")


async def shutdown(
    bot: Bot,
    tasks: list[asyncio.Task],
) -> None:
    """Корректное завершение всех соединений и задач"""
    logger.info("Завершение работы бота...")

    # Отменяем фоновые задачи
    for task in tasks:
        if not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    # Закрываем User WS если был запущен
    if ws_user_module.user_ws_manager:
        try:
            await ws_user_module.user_ws_manager.stop()
        except Exception as e:
            logger.warning(f"user_ws_manager stop error: {e}")

    # Закрываем Market WS
    try:
        from poly.ws_market import market_ws_manager
        market_ws_manager._started = False
        if market_ws_manager._ws:
            await market_ws_manager._ws.close()
    except Exception as e:
        logger.warning(f"market_ws_manager stop error: {e}")

    # Закрываем aiohttp сессии polymarket клиента
    try:
        await polymarket_client.close()
        logger.info("polymarket_client closed")
    except Exception as e:
        logger.warning(f"polymarket_client close error: {e}")

    # Закрываем Redis
    try:
        await redis_client.aclose()
        logger.info("Redis closed")
    except Exception as e:
        logger.warning(f"Redis close error: {e}")

    # Закрываем бота
    try:
        await bot.session.close()
        logger.info("Bot session closed")
    except Exception as e:
        logger.warning(f"Bot session close error: {e}")

    logger.info("Shutdown complete ✅")


async def check_redis_connection() -> bool:
    """Проверка доступности Redis при старте"""
    try:
        await redis_client.ping()
        logger.info("✅ Redis подключен")
        return True
    except Exception as e:
        logger.warning(
            f"⚠️ Redis недоступен при старте: {e}. "
            f"Copy trading не будет работать пока Redis не запустится. "
            f"Запусти: redis-server --daemonize yes"
        )
        return False


async def main():
    # Инициализация БД
    await init_models()

    # Проверка Redis (не блокирует запуск бота)
    await check_redis_connection()

    # Гео-проверка
    try:
        status = await geoblock_checker.check()
        if status:
            if status.blocked:
                logger.critical(
                    f"⛔ Сервер заблокирован для торговли. "
                    f"Country={status.country}"
                )
            else:
                logger.info(f"✅ Гео-проверка пройдена: {status.country}")
    except Exception as e:
        logger.warning(f"geoblock check failed: {e}")

    # Инициализация бота
    bot = Bot(token=settings.telegram_bot_token)
    set_bot(bot)
    dp = Dispatcher(storage=MemoryStorage())

    # Middleware
    dp.message.middleware(AuthMiddleware())
    dp.callback_query.middleware(AuthMiddleware())
    # ПОСЛЕ авторизации: создаёт запись пользователя, если её нет,
    # чтобы ни один хендлер не получил None из session.get(User, ...)
    dp.message.middleware(EnsureUserMiddleware())
    dp.callback_query.middleware(EnsureUserMiddleware())

    # Роутеры
    dp.include_router(onboarding.router)
    dp.include_router(start.router)
    dp.include_router(wallet.router)
    dp.include_router(settings_h.router)
    dp.include_router(positions.router)

    # User WebSocket (только в реальном режиме с API ключами)
    if not settings.simulation_mode and settings.clob_api_key:
        try:
            ws_user_module.user_ws_manager = ws_user_module.UserWebSocketManager(
                api_key=settings.clob_api_key,
                secret=settings.clob_api_secret,
                passphrase=settings.clob_api_passphrase,
            )
            await ws_user_module.user_ws_manager.start()
            logger.info("User WS started")
        except Exception as e:
            logger.warning(f"User WS start failed: {e}")

    # Reconciliation при старте
    try:
        await reconciliation_service.run_on_startup()
    except Exception as e:
        logger.warning(f"reconciliation startup error: {e}")

    # Фоновые задачи
    background_tasks = [
        asyncio.create_task(
            trader_service.start(),
            name="trader_service"
        ),
        asyncio.create_task(
            tp_sl_monitor.start(),
            name="tp_sl_monitor"
        ),
    ]

    # Прогреваем всё, за что иначе заплатит первая сделка:
    # геоблок, соединение с БД, TLS до биржи.
    try:
        await trader_service.prewarm_at_startup()
    except Exception as e:
        logger.warning(f"прогрев пропущен: {e}")

    # Держим соединение с биржей горячим: сделки приходят редко, а
    # простаивающее TCP-соединение рвётся молча, и первый же ордер
    # платит полный DNS+TCP+TLS до Лондона.
    # Прогреваем клиентов биржи, чтобы первая сделка после старта не
    # платила за деривацию CLOB-кредов в критическом пути.
    try:
        await polymarket_client.prewarm_user_clients()
    except Exception as e:
        logger.warning(f"prewarm клиентов пропущен: {e}")

    try:
        await polymarket_client.start_connection_keepalive()
    except Exception as e:
        logger.warning(f"keepalive не запущен: {e}")

    # ✅ Логируем исключения фоновых задач вместо тихого падения
    for task in background_tasks:
        task.add_done_callback(_handle_task_exception)

    # Восстанавливаем слежку для активных пользователей.
    # Раньше этого не было: после перезапуска контейнера бот показывал
    # статус "активен", но фактически ничего не копировал, пока
    # вручную не нажать Стоп -> Старт.
    try:
        from sqlalchemy import select as _select
        from models.user import User as _User
        from services.watcher import wallet_watcher as _ww
        async with async_session() as _s:
            _rows = (await _s.execute(
                _select(_User).where(_User.is_active.is_(True))
            )).scalars().all()
            _restore = [(u.id, u.target_wallet) for u in _rows]
        for _uid, _wallet in _restore:
            if _wallet:
                await _ww.start_watching(_uid, _wallet)
                logger.info(f"Слежка восстановлена после рестарта: user={_uid}")
    except Exception as _e:
        logger.error(f"Не удалось восстановить слежку: {_e}")

    # Диагностика производительности: обе эти библиотеки влияют на
    # скорость напрямую, а их отсутствие ничего не ломает — код просто
    # молча работает в разы медленнее. Лучше видеть это в логе.
    try:
        from eth_keys.backends import get_default_backend_class
        _ecc = get_default_backend_class().rsplit(".", 1)[-1]
    except Exception:
        _ecc = "неизвестен"

    if "CoinCurve" not in _ecc:
        logger.warning(
            f"Подпись ордеров идёт через {_ecc} (чистый Python). "
            f"Установите coincurve — подпись ускорится на порядки. "
            f"Сейчас каждый ордер тратит на подпись десятки-сотни мс."
        )

    # Печатаем ключевые настройки, которые РЕАЛЬНО применились.
    # Иначе невозможно отличить "правка не работает" от "контейнер
    # собран со старым кодом или .env перебивает значение".
    logger.info(
        "Настройки: "
        f"ставка={settings.default_bet_amount} "
        f"({settings.default_bet_mode}), "
        f"авто-подъём={settings.auto_bump_to_min_order}, "
        f"окно схлопывания={settings.copy_dedup_window_seconds}с, "
        f"макс.позиций={settings.max_open_positions}, "
        f"экспозиция={settings.max_total_exposure}, "
        f"опрос={settings.poll_interval_seconds}с, "
        f"симуляция={settings.simulation_mode}"
    )

    logger.info(
        f"Bot started (uvloop: {'да' if _UVLOOP else 'НЕТ'}, "
        f"подпись: {_ecc})"
    )

    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types()
        )
    except (KeyboardInterrupt, SystemExit):
        logger.info("Получен сигнал остановки")
    except Exception as e:
        logger.error(f"Polling error: {e}")
    finally:
        await shutdown(bot, background_tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass