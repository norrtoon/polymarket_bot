import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from loguru import logger
from sqlalchemy import select, func

from core.database import async_session
from core.config import settings
from core.redis_client import redis_client
from models.user import User
from models.position import Position
from models.trade_log import TradeLog
from poly.client import polymarket_client
from poly.geoblock import geoblock_checker
import poly.ws_user as ws_user_module
from services.notification import notify_user, notify_user_bg


def _format_speed(elapsed_ms: float) -> str:
    """Форматировать скорость копирования с оценкой качества"""
    if elapsed_ms < 500:
        grade = "🟢 Отлично"
    elif elapsed_ms < 1500:
        grade = "🟡 Хорошо"
    elif elapsed_ms < 3000:
        grade = "🟠 Удовлетворительно"
    else:
        grade = "🔴 Медленно"

    if elapsed_ms < 1000:
        return f"{elapsed_ms:.0f}мс {grade}"
    else:
        return f"{elapsed_ms / 1000:.2f}с {grade}"


def _now_str() -> str:
    """
    Время постановки ставки для уведомления.

    ВАЖНО: берётся системное время процесса. Если часы контейнера
    съехали (у Docker Desktop на macOS это бывает после сна Mac —
    см. предупреждение watcher о расхождении), время в уведомлении
    будет соответственно неверным. Смещение задаётся настройкой
    display_timezone_offset_hours (по умолчанию +3, Хельсинки).
    """
    tz = timezone(timedelta(hours=settings.display_timezone_offset_hours))
    # Время ПО ЧАСАМ POLYMARKET, а не сервера. Раньше показывалось время
    # сервера, и если его часы уезжали (у тебя они расходились на час),
    # в уведомлениях было неверное время.
    try:
        ts = polymarket_client.polymarket_now()
    except Exception:
        ts = time.time()
    return datetime.fromtimestamp(ts, tz).strftime("%H:%M:%S %d.%m.%Y")


def _ts_str(epoch: float | int | None) -> str:
    """Метка времени блока Polymarket в читаемом виде."""
    if not epoch:
        return "—"
    tz = timezone(timedelta(hours=settings.display_timezone_offset_hours))
    return datetime.fromtimestamp(float(epoch), tz).strftime("%H:%M:%S")


def _build_result_message(
    pos,
    reason: str,
    exit_price: Decimal | None,
    won: bool | None = None,
    trigger_elapsed_ms: float | None = None,
    trigger_price: Decimal | None = None,
) -> str:
    """
    Итог по сыгравшей ставке: выиграли или проиграли и на сколько.

    reason: "tp" | "sl" | "sell" (трейдер закрылся) | "resolved"
    exit_price — цена выхода; для разрешившегося рынка не нужна
    (выигравший токен гасится по 1.0, проигравший по 0).
    """
    entry = Decimal(str(pos.entry_price or 0))
    invested = Decimal(str(pos.amount_usdc or 0))

    # Куплено долей. shares_bought может быть не заполнен —
    # тогда считаем из вложенной суммы и цены входа.
    shares = Decimal(str(pos.shares_bought or 0))
    if shares <= 0 and entry > 0:
        shares = invested / entry

    if reason == "resolved":
        # Выигравший исход гасится по 1 USDC за долю, проигравший — по 0
        payout = shares if won else Decimal("0")
    else:
        px = Decimal(str(exit_price or pos.current_price or entry))
        payout = shares * px

    pnl = payout - invested
    pct = (pnl / invested * 100) if invested > 0 else Decimal("0")
    is_win = pnl > 0

    # Защита от абсурдных значений.
    #
    # На рынках вероятностей большие проценты бывают законно: вход по
    # 0.01 при выигрыше даёт почти +9900%, это правильная математика.
    # Но такой же результат получается и при ОШИБКЕ в данных — именно
    # так выглядел баг, когда проигравшая позиция объявлялась
    # выигрышной. Логируем исходные числа, чтобы отличить одно от
    # другого по логу, а не по ощущениям.
    if abs(pct) > 500:
        logger.warning(
            f"Необычный результат по позиции {getattr(pos, 'id', '?')}: "
            f"{pct:.0f}% (вложено {invested}, цена входа {entry}, "
            f"долей {shares}, выплата {payout}, причина {reason}, "
            f"won={won}). Проверьте позицию на polymarket.com."
        )

    if reason == "resolved":
        head = "🏆 <b>СТАВКА СЫГРАЛА — ВЫИГРЫШ</b>" if won \
            else "💀 <b>СТАВКА СЫГРАЛА — ПРОИГРЫШ</b>"
        how = "Рынок разрешился"
    elif reason == "tp":
        head = "🎯 <b>ТЕЙК-ПРОФИТ</b>"
        how = "Сработал take-profit"
    elif reason == "sl":
        head = "🛑 <b>СТОП-ЛОСС</b>"
        how = "Сработал stop-loss"
    else:
        head = "✅ <b>ПОЗИЦИЯ ЗАКРЫТА</b>" if is_win \
            else "📕 <b>ПОЗИЦИЯ ЗАКРЫТА</b>"
        how = "Трейдер закрыл позицию"

    verdict = "🟢 В ПЛЮСЕ" if is_win else (
        "🔴 В МИНУСЕ" if pnl < 0 else "⚪️ В НОЛЬ"
    )
    sign = "+" if pnl > 0 else ""

    lines = [
        head,
        f"📊 Рынок: Market {str(pos.token_id)[:8]}...",
        f"ℹ️ {how}",
        f"💵 Вложено: {invested:.2f} USDC",
    ]
    if reason != "resolved":
        px = Decimal(str(exit_price or pos.current_price or entry))
        lines.append(f"📈 Вход: {entry:.4f} → выход: {px:.4f}")
    else:
        lines.append(f"📈 Цена входа: {entry:.4f}")
    if trigger_elapsed_ms is not None:
        if trigger_elapsed_ms < 300:
            mark = "🟢 Отлично"
        elif trigger_elapsed_ms < 1000:
            mark = "🟡 Хорошо"
        else:
            mark = "🔴 Медленно"
        level = (
            pos.tp_price if reason == "tp"
            else pos.sl_price if reason == "sl" else None
        )
        if level and trigger_price:
            lvl = Decimal(str(level))
            trg = Decimal(str(trigger_price))
            fill = Decimal(str(exit_price)) if exit_price else None
            line = (
                f"📉 Уровень {lvl:.4f} → сработал по {trg:.4f}"
            )
            if fill is not None and fill != trg:
                line += f" → продано по {fill:.4f}"
            # Показываем разрыв: если цена перепрыгнула уровень, убыток
            # окажется больше заданного процента, и это должно быть
            # видно явно, а не выглядеть как ошибка расчёта.
            gap = abs(trg - lvl) / lvl * 100 if lvl > 0 else Decimal("0")
            if gap >= 5:
                line += f"\n⚠️ Цена перепрыгнула уровень на {gap:.0f}%"
            lines.append(line)
        lines.append(
            f"⚡ Скорость срабатывания: "
            f"{trigger_elapsed_ms:.0f}мс {mark}"
        )

    lines += [
        f"💰 Получено: {payout:.2f} USDC",
        f"{verdict}: {sign}{pnl:.2f} USDC ({sign}{pct:.1f}%)",
        f"🕒 Время: {_now_str()}",
    ]
    return "\n".join(lines)


class _UserSnapshot:
    """
    Отсоединённая копия полей пользователя.

    Нужна там, где данные пользователя используются ПАРАЛЛЕЛЬНО с
    другими запросами к той же сессии: AsyncSession не допускает
    конкурентных операций, а обращение к полю просроченного ORM-объекта
    само по себе порождает запрос к БД.
    """

    __slots__ = (
        "id", "private_key_enc", "clob_api_key_enc",
        "clob_api_secret_enc", "clob_api_passphrase_enc",
        "proxy_wallet", "proxy_wallet_type",
    )

    def __init__(self, user):
        for field in self.__slots__:
            setattr(self, field, getattr(user, field, None))


class TraderService:
    def __init__(self):
        self._pubsub_task: asyncio.Task | None = None
        # Задачи копирования "в полёте" — держим ссылки, иначе GC может
        # оборвать таску до завершения
        self._inflight: set[asyncio.Task] = set()
        # Кэш геоблока — проверяем раз в 5 минут
        self._geoblock_ok: bool | None = None
        self._geoblock_checked_at: float = 0
        self._geoblock_cache_ttl: float = 300.0
        # Retry настройки для Redis
        self._redis_retry_delay: float = 5.0
        self._redis_retry_max_delay: float = 60.0
        # Кэш equity: user_id -> (значение, время истечения по monotonic)
        self._equity_cache: dict[int, tuple[Decimal, float]] = {}
        # Ограничения рынков (min_order_size, tick_size) — не меняются
        self._constraints_cache: dict[str, dict] = {}
        # Цена из стакана, полученная в проверках — передаём её в
        # ордер, чтобы SDK не запрашивал стакан ещё раз
        self._last_book_price: dict[str, tuple] = {}
        # Предзапросы стакана, запущенные до основных проверок
        self._book_inflight: dict[str, asyncio.Task] = {}
        # Фоновые задачи прогрева, запущенные при обнаружении сделки
        self._prewarm_tasks: set[asyncio.Task] = set()
        self._timings: list[float] = []
        # Суммы, поднятые до минимума рынка в проверках
        self._bumped_amount: dict[str, Decimal] = {}
        # Позиции, о неудачном закрытии которых уже сообщили
        self._close_retry_notified: set[int] = set()
        # Активные задачи повторного закрытия, по одной на позицию
        self._close_retry_tasks: dict[int, asyncio.Task] = {}
        # Фоновый прогрев капитала: задачи по user_id и период
        self._equity_tasks: dict[int, asyncio.Task] = {}
        self._equity_refresh_interval: float = 30.0

        # Локи на (user_id, token_id) — защита от гонки при
        # конкурентной обработке нескольких сделок по одному токену
        self._position_locks: dict[tuple[int, str], asyncio.Lock] = {}

    async def start(self):
        self._pubsub_task = asyncio.create_task(self._listen())

    async def _listen(self):
        """
        Слушаем Redis pub/sub с автоматическим переподключением.
        Если Redis недоступен — ждём и пробуем снова,
        не роняя весь процесс.
        """
        retry_delay = self._redis_retry_delay

        while True:
            try:
                pubsub = redis_client.pubsub()
                await pubsub.psubscribe("new_trade:*")
                logger.info("TraderService subscribed to new_trade:*")

                # Успешное подключение — сбрасываем задержку
                retry_delay = self._redis_retry_delay

                async for message in pubsub.listen():
                    if message["type"] != "pmessage":
                        continue
                    try:
                        user_id = int(message["channel"].split(":")[1])
                        data = json.loads(message["data"])
                    except Exception as e:
                        logger.error(f"trader listen parse error: {e}")
                        continue

                    # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: не await, а create_task.
                    # Иначе каждая следующая сделка ждёт, пока полностью
                    # отработает предыдущая (ордер + БД + Telegram + WS-
                    # подписка TP/SL) — это и давало линейный рост задержки.
                    task = asyncio.create_task(
                        self._safe_execute(user_id, data)
                    )
                    self._inflight.add(task)
                    task.add_done_callback(self._inflight.discard)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    f"Redis connection lost in TraderService: {e}, "
                    f"retry in {retry_delay:.0f}s"
                )
                await asyncio.sleep(retry_delay)
                # Экспоненциальный backoff с максимумом
                retry_delay = min(
                    retry_delay * 2, self._redis_retry_max_delay
                )

    async def _safe_execute(self, user_id: int, trade_data: dict):
        """Обёртка с изоляцией ошибок — падение одной сделки не должно
        затрагивать остальные конкурентные задачи."""
        try:
            await self.execute_copy_trade(user_id, trade_data)
        except Exception as e:
            logger.error(
                f"execute_copy_trade crashed user={user_id}: {e}"
            )

    async def _check_geoblock(self, user_id: int) -> bool:
        """Проверка геоблока с кэшированием на 5 минут"""
        now = time.monotonic()
        # Успешную проверку держим 5 минут, а ЗАПРЕТ — всего 30 секунд.
        #
        # Раньше отрицательный результат кэшировался на те же 5 минут.
        # Если проверка не прошла один раз (например, сеть ещё не
        # прогрета сразу после запуска), торговля вставала на пять
        # минут, а сделки при этом исчезали БЕЗ ЕДИНОЙ СТРОКИ в логе —
        # выглядело как "бот перестал видеть ставки".
        ttl = (
            self._geoblock_cache_ttl if self._geoblock_ok
            else 30.0
        )
        if (self._geoblock_ok is not None and
                now - self._geoblock_checked_at < ttl):
            return self._geoblock_ok

        try:
            status = await geoblock_checker.check()

            if status.blocked:
                logger.error(f"Geo blocked: {status.country}")
                notify_user_bg(
                    user_id,
                    f"⛔ Торговля недоступна из региона {status.country}"
                )
                self._geoblock_ok = False

            elif status.unknown:
                # Проверка не дала ответа. Раньше такой случай молча
                # трактовался как "регион разрешён": check() ловил
                # исключение внутри себя и возвращал None, ветка
                # except здесь не срабатывала НИКОГДА, и защита
                # fail-closed фактически не работала.
                #
                # В боевом режиме не торгуем без подтверждения региона,
                # в симуляции безопасно продолжить.
                self._geoblock_ok = bool(settings.simulation_mode)
                if self._geoblock_ok:
                    logger.warning(
                        "geoblock: статус неизвестен — продолжаем "
                        "(симуляция)"
                    )
                else:
                    logger.error(
                        "geoblock: статус региона НЕИЗВЕСТЕН — торговля "
                        "заблокирована до успешной проверки. Если регион "
                        "точно разрешён, поставьте "
                        "GEOBLOCK_FAIL_OPEN=true в .env."
                    )
                    notify_user_bg(
                        user_id,
                        "⚠️ Не удалось проверить регион — торговля "
                        "приостановлена. Сделки не копируются."
                    )
            else:
                self._geoblock_ok = True

            # Аварийный переключатель: если проверка стабильно не
            # работает, а регион заведомо разрешён.
            if not self._geoblock_ok and settings.geoblock_fail_open \
                    and not status.blocked:
                logger.warning(
                    "GEOBLOCK_FAIL_OPEN=true — торгуем без подтверждения "
                    "региона (под вашу ответственность)"
                )
                self._geoblock_ok = True

        except Exception as e:
            self._geoblock_ok = bool(
                settings.simulation_mode or settings.geoblock_fail_open
            )
            logger.error(
                f"geoblock unexpected error: {type(e).__name__}: {e} — "
                f"{'продолжаем' if self._geoblock_ok else 'ТОРГОВЛЯ ЗАБЛОКИРОВАНА'}"
            )

        self._geoblock_checked_at = now
        return self._geoblock_ok

    async def _refresh_equity(self, user_id: int, wallet: str) -> Decimal:
        """
        ПРИНУДИТЕЛЬНО перечитать капитал и обновить кэш, не спрашивая
        кэш о свежести. Именно этого не хватало раньше: фоновый цикл
        дёргал _get_equity_cached, тот видел ещё живой кэш и ничего не
        обновлял — в итоге реальное обновление происходило только раз
        в ~90с, а в окне между истечением TTL и следующим тиком любая
        сделка платила за поход в сеть (в логах это 300-940мс).
        """
        try:
            equity = await polymarket_client.get_total_equity(wallet)
        except Exception as e:
            logger.debug(
                f"_refresh_equity user={user_id}: "
                f"{type(e).__name__}: {e}"
            )
            return self._equity_cache.get(user_id, (Decimal("0"), 0.0))[0]

        self._equity_cache[user_id] = (equity, 0.0)
        return equity

    async def warm_equity_cache(self, user_id: int, wallet: str):
        """Разовый прогрев при нажатии Старт, чтобы percent-режим знал
        капитал и не запрашивал его во время копирования сделки."""
        await self._refresh_equity(user_id, wallet)

    async def start_equity_refresher(self, user_id: int, wallet: str):
        """
        Фоновое обновление капитала каждые 30 секунд.

        Обновляет ПРИНУДИТЕЛЬНО (через _refresh_equity, а не через
        кэшированный геттер) — иначе, как было раньше, цикл видел ещё
        живой кэш и ничего не обновлял, реальное обновление случалось
        раз в ~90с, и сделка, попавшая в окно между истечением TTL и
        следующим тиком, платила 300-900мс за поход в сеть.
        """
        old_task = self._equity_tasks.get(user_id)
        if old_task and not old_task.done():
            old_task.cancel()

        async def loop():
            while True:
                await self._refresh_equity(user_id, wallet)
                await asyncio.sleep(self._equity_refresh_interval)

        self._equity_tasks[user_id] = asyncio.create_task(loop())

    async def stop_equity_refresher(self, user_id: int):
        task = self._equity_tasks.pop(user_id, None)
        if task and not task.done():
            task.cancel()

    def get_equity_cached(self, user_id: int) -> Decimal:
        """
        Последний известный капитал. В СЕТЬ НЕ ХОДИТ НИКОГДА.

        Значение поддерживает свежим фоновый прогрев (каждые 30с) и
        нажатие кнопки "Баланс". Критический путь копирования сети не
        касается вообще — именно это убирает всплески до 900мс.

        Если значения ещё нет (самая первая сделка до первого
        прогрева), вернём 0, и _calculate_amount откатится на
        фиксированную сумму вместо похода в сеть.
        """
        cached = self._equity_cache.get(user_id)
        return cached[0] if cached else Decimal("0")

    def set_equity_cache(self, user_id: int, equity: Decimal) -> None:
        """Положить свежий капитал в кэш. Вызывается из хендлера
        кнопки "Баланс" — значение освежается ровно тогда, когда
        пользователь сам его смотрит."""
        self._equity_cache[user_id] = (equity, 0.0)

    async def _calculate_amount(self, user: User) -> Decimal:
        if user.bet_mode == "fixed":
            return Decimal(str(user.bet_amount))

        if user.bet_mode == "percent":
            wallet = (
                getattr(user, "proxy_wallet", None)
                or settings.my_proxy_wallet_address
                or settings.my_wallet_address
            )

            if not wallet:
                logger.warning(
                    f"percent mode w/o proxy_wallet user={user.id}, "
                    f"fallback fixed"
                )
                return Decimal(str(user.bet_amount))

            total_equity = self.get_equity_cached(user.id)

            if total_equity <= 0:
                logger.warning(
                    f"total_equity=0 user={user.id} wallet={wallet}, "
                    f"fallback fixed"
                )
                return Decimal(str(user.bet_amount))

            amount = (
                total_equity
                * Decimal(str(user.bet_percent))
                / Decimal("100")
            )
            result = min(amount, Decimal(str(settings.max_bet_amount)))
            logger.debug(
                f"percent mode user={user.id}: equity={total_equity} "
                f"bet_percent={user.bet_percent}% amount={result}"
            )
            return result

        logger.warning(
            f"unknown bet_mode={user.bet_mode} user={user.id}, fallback fixed"
        )
        return Decimal(str(user.bet_amount))

    def _calculate_tp_sl_prices(
        self,
        entry_price: Decimal,
        user: User
    ) -> tuple[Decimal | None, Decimal | None]:
        tp_price = (
            entry_price * (1 + Decimal(str(user.tp_percent)) / 100)
            if user.tp_percent else None
        )
        sl_price = (
            entry_price * (1 - Decimal(str(user.sl_percent)) / 100)
            if user.sl_percent else None
        )

        if tp_price is not None:
            tp_price = min(tp_price, Decimal("0.999"))
        if sl_price is not None:
            sl_price = max(sl_price, Decimal("0.001"))

        return tp_price, sl_price

    def _prune_locks(self, limit: int = 500) -> None:
        """Словарь локов растёт с каждым новым рынком и никогда не
        очищался. За долгую сессию это тысячи объектов Lock в памяти.
        Выбрасываем свободные, когда накопилось слишком много."""
        if len(self._position_locks) <= limit:
            return
        for key in [
            k for k, v in self._position_locks.items() if not v.locked()
        ]:
            self._position_locks.pop(key, None)

    def _position_lock(self, user_id: int, token_id: str) -> asyncio.Lock:
        """
        Лок на пару (пользователь, токен). Лок именно на ПАРУ, а не на
        токен: два разных пользователя, копирующих один рынок, не
        должны блокировать друг друга.
        """
        self._prune_locks()
        key = (user_id, token_id)
        lock = self._position_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._position_locks[key] = lock
        return lock

    async def _today_realized_loss(self, session, user_id: int) -> Decimal:
        """
        Сколько реально потеряно за сегодня (только закрытые позиции).

        Считаем по фактически закрытым позициям: вложено минус
        получено. Открытые не учитываем — их результат ещё не
        определён.
        """
        try:
            start_of_day = datetime.utcnow().replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            stmt = select(Position).where(
                Position.user_id == user_id,
                Position.status != "open",
                Position.closed_at >= start_of_day,
            )
            rows = (await session.execute(stmt)).scalars().all()

            total = Decimal("0")
            for p_ in rows:
                invested = Decimal(str(p_.amount_usdc or 0))
                shares = Decimal(str(p_.shares_bought or 0))
                exit_px = Decimal(str(p_.current_price or 0))
                status = p_.status or ""

                if status == "resolved_won":
                    payout = shares
                elif status == "resolved_lost":
                    payout = Decimal("0")
                elif status == "failed":
                    payout = invested        # сделка не состоялась
                else:
                    payout = shares * exit_px

                total += invested - payout   # положительное = убыток

            return max(total, Decimal("0"))
        except Exception as e:
            logger.warning(f"_today_realized_loss: {e}")
            return Decimal("0")

    async def _preflight_checks(
        self, session, user, token_id: str, amount: Decimal,
        expected_price: Decimal | None, side: str,
    ) -> str | None:
        """
        Проверки ПЕРЕД отправкой боевого ордера.
        Возвращает текст причины отказа или None, если всё в порядке.

        Ни одной из этих проверок раньше не было: бот отправлял ордер
        вслепую — без учёта баланса и общей экспозиции.
        """
        if settings.simulation_mode:
            return None

        # ОДИН снимок стакана на все проверки ниже: и цена для
        # ордера (limit_price), и минимальный размер. Раньше это были
        # два отдельных сетевых вызова в критическом пути.
        t_book = time.monotonic()

        # Всё, что можно сделать ПАРАЛЛЕЛЬНО — делаем параллельно.
        # Раньше это были последовательные ожидания: сначала стакан,
        # потом запрос экспозиции в БД, а метаданные рынка вообще
        # тянулись позже внутри place_market_order. Три ожидания
        # подряд вместо одного.
        exposure_stmt = select(
            func.count(), func.coalesce(func.sum(Position.amount_usdc), 0)
        ).where(
            Position.user_id == user.id, Position.status == "open"
        )

        # ВАЖНО: до gather вытаскиваем всё, что нужно от ORM-объекта.
        #
        # AsyncSession НЕ допускает конкурентных операций. Если внутри
        # gather обратиться к полю user, а объект окажется просроченным,
        # SQLAlchemy отправит SELECT одновременно с exposure_stmt и
        # упадёт с InvalidRequestError ("concurrent operations are not
        # permitted"). Снимок полей делает эту гонку невозможной.
        user_snapshot = _UserSnapshot(user)

        book, exposure_row, _ = await asyncio.gather(
            self._get_book(token_id),
            session.execute(exposure_stmt),
            # Прогрев кэша SDK: убирает сетевой запрос из момента
            # подписи ордера (см. warm_order_metadata).
            polymarket_client.warm_order_metadata(user_snapshot, token_id),
            return_exceptions=False,
        )
        logger.info(
            f"  этап: стакан+БД+прогрев = "
            f"{(time.monotonic() - t_book) * 1000:.0f}мс"
        )

        # Цену из стакана запоминаем: она передаётся в ордер как
        # limit_price, чтобы SDK не запрашивал стакан ещё раз.
        self._last_book_price[token_id] = (
            book.get("best_ask") if side == "BUY" else book.get("best_bid"),
            book.get("tick_size") or Decimal("0.01"),
            side,
            time.monotonic(),
        )

        # ДНЕВНОЙ ЛИМИТ УБЫТКА — последний рубеж.
        #
        # Все остальные проверки защищают от конкретных сценариев. Этот
        # не зависит ни от какого сценария: сколько бы ошибок ни было в
        # коде, за сутки нельзя потерять больше заданной суммы. Дальше
        # копирование останавливается до ручного вмешательства.
        limit = Decimal(str(getattr(settings, "daily_loss_limit", 0)))
        if limit > 0:
            lost = await self._today_realized_loss(session, user.id)
            if lost >= limit:
                return (
                    f"достигнут дневной лимит убытка: потеряно "
                    f"{lost:.2f} USDC при лимите {limit} USDC. "
                    f"Копирование остановлено до завтра или до "
                    f"изменения DAILY_LOSS_LIMIT."
                )

        # ---- Дальше идут проверки, осмысленные ТОЛЬКО для покупки ----
        #
        # Раньше они выполнялись и для продажи, из-за чего бот не мог
        # закрывать позиции вслед за трейдером:
        #   * лимит открытых позиций отклонял продажу, которая как раз
        #     освободила бы слот (при MAX_OPEN_POSITIONS=1 блокировались
        #     вообще все сделки после первой, включая перезаходы);
        #   * проверка баланса требовала свободных USDC, хотя при
        #     продаже мы деньги ПОЛУЧАЕМ, а не тратим;
        #   * минимум в долях считался как ставка/цена, хотя продаём мы
        #     shares_bought из позиции — другое число.
        #
        if side != "BUY":
            return None

        # 2a. Минимум площадки в долларах для рыночной покупки.
        # Биржа отклоняет marketable BUY меньше 1 USDC. Проверяем сами,
        # чтобы пользователь видел понятную причину, а не отказ биржи.
        if amount < Decimal(str(
            getattr(settings, "min_order_usdc", "1.0")
        )):
            return (
                f"ставка {amount:.2f} USDC меньше минимума площадки "
                f"({getattr(settings, 'min_order_usdc', 1)} USDC "
                f"для рыночной покупки)"
            )

        # 2b. Минимальный размер ордера в долях.
        #
        # Polymarket меряет минимум в ДОЛЯХ (обычно 5), а не в
        # долларах. Стоимость этого минимума зависит от цены: 5 долей
        # по 0.10 — это $0.50, а по 0.90 — уже $4.50. Без этой
        # проверки мелкая ставка на дорогом рынке отклонялась биржей
        # с ошибкой "Size (...) lower than the minimum: 5", а
        # пользователь видел лишь "Ордер не исполнен".
        if expected_price and expected_price > 0:
            min_shares = book.get("min_order_size") or Decimal("0")
            if min_shares > 0:
                our_shares = amount / expected_price
                if our_shares < min_shares:
                    need = (min_shares * expected_price).quantize(
                        Decimal("0.01"), rounding="ROUND_UP"
                    )
                    cap = Decimal(str(settings.max_bet_amount))

                    # Минимум измеряется в ДОЛЯХ и фиксирован, а в
                    # долларах зависит от цены: 5 долей по 0.10 — это
                    # $0.50, по 0.90 — уже $4.50. Поэтому одна сумма
                    # ставки физически не подходит ко всем рынкам, и
                    # часть сделок отсекалась просто из-за цены.
                    #
                    # Слегка добавляем до минимума, если разрешено и
                    # это не выходит за лимит ставки.
                    auto_bump = getattr(
                        settings, "auto_bump_to_min_order", True
                    )
                    if auto_bump and need <= cap:
                        logger.info(
                            f"user={user.id}: ставка поднята "
                            f"{amount:.2f} -> {need:.2f} USDC, чтобы "
                            f"пройти минимум {min_shares} долей "
                            f"при цене {expected_price:.4f}"
                        )
                        self._bumped_amount[token_id] = need
                        return None

                    return (
                        f"ставка {amount:.2f} USDC даёт "
                        f"{our_shares:.2f} долей, а рынок требует "
                        f"минимум {min_shares} — нужно хотя бы "
                        f"{need:.2f} USDC при цене {expected_price:.4f}"
                        + (f" (лимит ставки {cap} USDC)"
                           if need > cap else "")
                    )

        # 3. Хватает ли свободных средств
        if user.proxy_wallet:
            # Берём из кэша, который и так обновляется фоном каждые 30с.
            # Свежий RPC-запрос здесь добавлял ~0.5-1с прямо в
            # критический путь копирования.
            free = self.get_equity_cached(user.id)
            if free > 0 and free < amount:
                return (
                    f"недостаточно средств: нужно {amount:.2f}, "
                    f"свободно {free:.2f}"
                )

        # 4. Ограничение общей экспозиции
        row = exposure_row.one()
        open_count, open_sum = int(row[0] or 0), Decimal(str(row[1] or 0))

        if open_count >= settings.max_open_positions:
            return (
                f"достигнут лимит открытых позиций "
                f"({open_count}/{settings.max_open_positions})"
            )
        if open_sum + amount > Decimal(str(settings.max_total_exposure)):
            return (
                f"лимит суммарной экспозиции: в рынке {open_sum:.2f}, "
                f"максимум {settings.max_total_exposure}"
            )
        return None

    async def _settle_resolved_position(self, position_id: int) -> None:
        """
        Закрыть позицию на разрешившемся рынке.

        Смотрим, есть ли доли на кошельке и подлежат ли они погашению.
        Если да — гасим (выигрыш придёт на кошелёк). Если долей нет
        или они ничего не стоят — помечаем позицию проигранной.
        """
        try:
            async with async_session() as session:
                pos = await session.get(Position, position_id)
                if not pos or pos.status != "open":
                    return
                owner = await session.get(User, pos.user_id)
                if not owner or not owner.proxy_wallet:
                    return

                won = False
                try:
                    redeemable = await polymarket_client.get_positions(
                        owner.proxy_wallet, redeemable=True
                    )
                    won = any(
                        p.asset == pos.token_id and (p.size or 0) > 0
                        for p in redeemable
                    )
                except Exception as e:
                    logger.warning(f"проверка погашаемости: {e}")

            # redeem_resolved_position сам запишет статус и уведомит
            async with async_session() as session:
                pos = await session.get(Position, position_id)
                if pos and pos.status == "open":
                    await self.redeem_resolved_position(pos, won=won)

        except Exception as e:
            logger.error(
                f"_settle_resolved_position {position_id}: "
                f"{type(e).__name__}: {e}"
            )

    def _resting_sell_price(self, token_id: str) -> Decimal | None:
        """
        Цена для лимитной продажи: чуть ниже последнего известного
        бида, чтобы встать первыми в очередь и исполниться сразу при
        появлении покупателя.
        """
        entry = self._last_book_price.get(token_id)
        if not entry:
            return None
        price, tick, _side, _ts = entry
        if not price:
            return None
        tick = Decimal(str(tick or "0.01"))
        price = Decimal(str(price))
        limit = price - tick
        if limit < tick:
            limit = tick
        return limit

    def _schedule_close_retry(
        self, position_id: int, reason: str, attempt: int = 1
    ) -> None:
        """Повторить закрытие позиции через паузу, до 5 попыток."""
        if attempt > 5:
            logger.error(
                f"Position {position_id}: закрыть не удалось за 5 попыток "
                f"({reason}). Позиция остаётся открытой — закройте "
                f"вручную на polymarket.com."
            )
            self._close_retry_notified.discard(position_id)

            async def _notify_giveup():
                try:
                    async with async_session() as session:
                        pos = await session.get(Position, position_id)
                        if pos and pos.status == "open":
                            notify_user_bg(
                                pos.user_id,
                                f"❌ Позицию не удалось закрыть за 5 "
                                f"попыток ({reason}). Закройте её "
                                f"вручную на polymarket.com."
                            )
                except Exception:
                    pass

            task = asyncio.create_task(_notify_giveup())
            self._prewarm_tasks.add(task)
            task.add_done_callback(self._prewarm_tasks.discard)
            return

        # Не плодим параллельные попытки по одной позиции.
        #
        # Из-за сброса счётчика запускалось по новой задаче на каждую
        # неудачу, и они накладывались друг на друга — в логе это
        # выглядело как непрерывный поток попыток раз в секунду.
        existing = self._close_retry_tasks.get(position_id)
        if existing is not None and not existing.done():
            return

        # Первые повторы — БЫСТРЫЕ: ликвидность на тонком рынке
        # появляется и исчезает за секунды, а прежние 15с гарантировали
        # продажу по уже уехавшей цене.
        delays = [1, 3, 8, 20, 45]
        delay = delays[min(attempt - 1, len(delays) - 1)]

        async def retry():
            await asyncio.sleep(delay)
            try:
                async with async_session() as session:
                    pos = await session.get(Position, position_id)
                    if not pos or pos.status != "open":
                        return  # уже закрылась
                logger.info(
                    f"Position {position_id}: повторная попытка "
                    f"закрытия ({reason}), попытка {attempt + 1}"
                )
                await self.close_position(
                    pos, reason=reason, attempt=attempt
                )
            except Exception as e:
                logger.error(
                    f"Повтор закрытия {position_id}: "
                    f"{type(e).__name__}: {e}"
                )
                self._schedule_close_retry(position_id, reason, attempt + 1)

        task = asyncio.create_task(retry())
        self._prewarm_tasks.add(task)
        task.add_done_callback(self._prewarm_tasks.discard)

    async def prewarm_at_startup(self) -> None:
        """
        Прогреть ВСЁ, что первая сделка иначе оплатит из своего времени.

        В логах видно наглядно: первая сделка после запуска — проверки
        1152мс, следующие — 53-69мс. Разница в двадцать раз, и вся она
        приходится на разовые операции: TLS-рукопожатия, создание
        клиента биржи, первый запрос геоблока.

        Геоблок здесь особенно важен: стартовая проверка в main.py
        кэшируется ОТДЕЛЬНО от кэша внутри трейдера, поэтому первая
        же сделка всё равно делала собственный сетевой запрос.
        """
        # 1) Геоблок — заполняем именно трейдерский кэш
        if not settings.simulation_mode:
            try:
                async with async_session() as session:
                    first = (await session.execute(
                        select(User).where(User.is_active.is_(True)).limit(1)
                    )).scalar_one_or_none()
                if first:
                    await self._check_geoblock(first.id)
                    if self._geoblock_ok:
                        logger.info("Кэш геоблока прогрет: регион разрешён")
                    else:
                        # Прогрев идёт в самый холодный момент: сеть
                        # ещё не установилась, DNS и TLS не прогреты.
                        # Неудача здесь НЕ должна запирать торговлю —
                        # сбрасываем кэш, чтобы первая реальная сделка
                        # проверила заново.
                        self._geoblock_ok = None
                        logger.warning(
                            "Прогрев геоблока не удался (сеть ещё не "
                            "готова) — проверим при первой сделке"
                        )
            except Exception as e:
                logger.debug(f"прогрев геоблока: {e}")

        # 2) Соединение с БД: первый запрос иначе платит за коннект
        try:
            async with async_session() as session:
                await session.execute(select(func.count()).select_from(Position))
        except Exception as e:
            logger.debug(f"прогрев БД: {e}")

        # 3) Соединение со стаканом: TLS до clob.polymarket.com
        try:
            await polymarket_client.get_book_snapshot("warmup")
        except Exception:
            pass

        logger.info("Прогрев завершён — первая сделка не будет холодной")

    def _schedule_position_verification(self, position_id: int) -> None:
        """
        Через несколько секунд сверить позицию с биржей.

        Уведомление "Скопирована сделка" отправляется по ответу биржи
        на ордер. Но ответ может сообщать о принятии, а не об
        исполнении — тогда в базе появляется позиция, которой на
        кошельке нет. Пользователь видит подтверждение, а на
        polymarket.com ордера нет: перезаход "только на словах".

        Проверять синхронно нельзя — это добавило бы задержку в
        критический путь, а данные на бирже появляются не мгновенно.
        Поэтому сверяем отложенно и, если долей нет, честно помечаем
        позицию несостоявшейся.
        """
        if settings.simulation_mode:
            return

        async def verify():
            # getattr с запасным значением: если core/config.py
            # окажется старее services/trader.py (например, при
            # частичном обновлении репозитория), задача не должна
            # падать с AttributeError и молча терять сверку позиции.
            await asyncio.sleep(
                getattr(settings, "position_verify_delay_seconds", 12)
            )
            try:
                async with async_session() as session:
                    pos = await session.get(Position, position_id)
                    if not pos or pos.status != "open":
                        return
                    owner = await session.get(User, pos.user_id)
                    if not owner or not owner.proxy_wallet:
                        return

                    live = await polymarket_client.get_positions(
                        owner.proxy_wallet
                    )
                    on_chain = Decimal("0")
                    for lp in live:
                        if lp.asset == pos.token_id:
                            on_chain = Decimal(str(lp.size or 0))
                            break

                    if on_chain > 0:
                        # Позиция реальна. Заодно уточняем количество:
                        # фактическое исполнение может отличаться от
                        # того, что вернул ответ на ордер.
                        recorded = Decimal(str(pos.shares_bought or 0))
                        if recorded > 0 and abs(on_chain - recorded) > \
                                recorded * Decimal("0.02"):
                            logger.info(
                                f"Position {pos.id}: количество уточнено "
                                f"по бирже {recorded} -> {on_chain}"
                            )
                            pos.shares_bought = on_chain
                            await session.commit()
                        return

                    # Долей нет — ордер не исполнился
                    logger.error(
                        f"Position {pos.id}: НЕ ПОДТВЕРЖДЕНА биржей "
                        f"(долей по {pos.token_id[:16]}... на кошельке "
                        f"нет). Ордер не исполнился, помечаем как "
                        f"несостоявшуюся."
                    )
                    pos.status = "failed"
                    pos.closed_at = datetime.utcnow()
                    await session.commit()

                    notify_user_bg(
                        pos.user_id,
                        f"❌ <b>Сделка НЕ состоялась</b>\n"
                        f"📊 Рынок: {pos.token_id[:10]}...\n"
                        f"Ордер не исполнился на бирже — позиции нет. "
                        f"Предыдущее подтверждение было преждевременным, "
                        f"деньги не списаны."
                    )

                    try:
                        from services.tp_sl_monitor import tp_sl_monitor
                        await tp_sl_monitor.stop_watching_position(pos.id)
                    except Exception:
                        pass

            except Exception as e:
                logger.warning(
                    f"сверка позиции {position_id}: "
                    f"{type(e).__name__}: {e}"
                )

        task = asyncio.create_task(verify())
        self._prewarm_tasks.add(task)
        task.add_done_callback(self._prewarm_tasks.discard)

    def _record_timing(self, elapsed_ms: float) -> None:
        """
        Копим статистику скорости и раз в 10 сделок печатаем сводку.

        Одна цифра в уведомлении не показывает разброс, а именно
        разброс и мешает: важно видеть не только среднее, но и
        худшие случаи.
        """
        self._timings.append(elapsed_ms)
        if len(self._timings) < 10:
            return
        vals = sorted(self._timings)
        n = len(vals)
        logger.info(
            f"СКОРОСТЬ за {n} сделок: "
            f"мин {vals[0]:.0f}мс | "
            f"медиана {vals[n // 2]:.0f}мс | "
            f"худшая {vals[-1]:.0f}мс"
        )
        self._timings.clear()

    def prewarm_for_trade(self, user_id: int, token_id: str) -> None:
        """
        Начать всю сетевую подготовку СРАЗУ при обнаружении сделки,
        не дожидаясь, пока она дойдёт до исполнения.

        Вызывается из вотчера перед публикацией. К моменту, когда
        сделка пройдёт через Redis, подписку и БД, стакан и метаданные
        рынка уже будут получены.
        """
        if settings.simulation_mode:
            return

        self._prefetch_book(token_id)

        # Метаданные рынка нужны SDK для подписи ордера. Без прогрева
        # он тянет их сам, синхронно, прямо перед отправкой.
        async def _warm_meta():
            try:
                async with async_session() as session:
                    user = await session.get(User, user_id)
                    if not user or not user.private_key_enc:
                        return
                    snapshot = _UserSnapshot(user)
                await polymarket_client.warm_order_metadata(
                    snapshot, token_id
                )
            except Exception as e:
                logger.debug(f"prewarm meta: {type(e).__name__}: {e}")

        task = asyncio.create_task(_warm_meta())
        self._prewarm_tasks.add(task)
        task.add_done_callback(self._prewarm_tasks.discard)

    def _prefetch_book(self, token_id: str) -> None:
        """Начать тянуть стакан заранее, не дожидаясь результата."""
        existing = self._book_inflight.get(token_id)
        if existing is not None and not existing.done():
            return
        self._book_inflight[token_id] = asyncio.create_task(
            polymarket_client.get_book_snapshot(token_id)
        )

    async def _get_book(self, token_id: str) -> dict:
        """Забрать результат предзапроса или сделать запрос сейчас."""
        task = self._book_inflight.pop(token_id, None)
        if task is not None:
            try:
                return await task
            except Exception:
                pass
        return await polymarket_client.get_book_snapshot(token_id)

    def _book_price_for(self, token_id: str) -> Decimal | None:
        """
        Предельная цена для ордера — с ШИРОКИМ запасом.

        Раньше сюда шла ровно текущая лучшая цена из стакана. Это жёсткий
        лимит без допуска: стакан сдвигается на тик между нашим запросом
        и приходом ордера — и биржа отвечает "No resting liquidity" /
        "no orders found to match with FAK order". Именно это мешало
        закрывать позиции по стоп-лоссу.

        Лимит нужен не для контроля цены (защита от проскальзывания
        отключена), а только чтобы SDK не ходил за стаканом второй раз.
        Поэтому берём заведомо широкий диапазон: он не блокирует
        исполнение, но позволяет SDK подписать ордер сразу.
        """
        entry = self._last_book_price.get(token_id)
        if not entry:
            return None
        price, tick, side, taken_at = entry
        if price is None or time.monotonic() - taken_at > 15:
            return None

        tick = Decimal(str(tick or "0.01"))
        price = Decimal(str(price))

        if side == "BUY":
            # Готовы заплатить сильно дороже текущего аска
            limit = min(price * Decimal("2"), Decimal("0.99"))
            limit = (limit / tick).to_integral_value(rounding="ROUND_FLOOR") * tick
            floor_ = price
            if limit < floor_:
                limit = min(floor_, Decimal("0.99"))
        else:
            # Готовы продать сильно дешевле текущего бида
            limit = max(price / Decimal("2"), tick)
            limit = (limit / tick).to_integral_value(rounding="ROUND_CEILING") * tick
            if limit > price:
                limit = price

        if limit <= 0 or limit >= 1:
            return None
        return limit

    async def _open_shares_for_token(
        self, session, user_id: int, token_id: str
    ) -> Decimal:
        """
        Сколько долей по этому токену у нас реально открыто.

        Суммируем ВСЕ открытые позиции: перезаходы создают несколько
        позиций по одному рынку, и когда трейдер выходит, закрывать
        нужно весь объём. Если shares_bought где-то не сохранилось,
        восстанавливаем из вложенной суммы и цены входа.
        """
        stmt = select(Position).where(
            Position.user_id == user_id,
            Position.token_id == token_id,
            Position.status == "open",
        )
        positions = (await session.execute(stmt)).scalars().all()

        total = Decimal("0")
        for pos in positions:
            shares = Decimal(str(pos.shares_bought or 0))

            if shares <= 0:
                entry = Decimal(str(pos.entry_price or 0))
                if entry > 0:
                    shares = Decimal(str(pos.amount_usdc or 0)) / entry
            total += shares
        return total

    async def _count_open_positions(
        self, session, user_id: int, token_id: str
    ) -> int:
        """Сколько открытых позиций у пользователя по этому токену."""
        stmt = select(func.count()).select_from(Position).where(
            Position.user_id == user_id,
            Position.token_id == token_id,
            Position.status == "open",
        )
        return int((await session.execute(stmt)).scalar() or 0)

    async def execute_copy_trade(self, user_id: int, trade_data: dict):
        # Засекаем время — используем detected_at от watcher
        detected_at = trade_data.get("detected_at")
        copy_start_time = (
            float(detected_at) if detected_at else time.monotonic()
        )

        # Геоблок с кэшированием (не тормозит каждый раз)
        if not settings.simulation_mode:
            if not await self._check_geoblock(user_id):
                logger.error(
                    f"user={user_id}: сделка {side} НЕ СКОПИРОВАНА — "
                    f"проверка региона не пройдена. Это главная причина, "
                    f"по которой бот может молча перестать копировать."
                )
                return

        side = trade_data.get("side", "BUY")
        token_id = trade_data.get("token_id", "")

        if not token_id:
            logger.error(
                f"execute_copy_trade: пустой token_id user={user_id}"
            )
            return

        # Запускаем запрос стакана СРАЗУ, ещё до похода в БД за
        # пользователем и до ожидания лока. Раньше он стартовал только
        # внутри проверок — то есть после нескольких последовательных
        # операций. Теперь сетевой запрос летит параллельно с ними, и к
        # моменту проверок ответ уже готов.
        if not settings.simulation_mode:
            self._prefetch_book(token_id)

        # Лок НЕ держим на весь путь.
        #
        # Раньше он охватывал всё исполнение целиком, включая отправку
        # ордера. Из-за этого две сделки по ОДНОМУ рынку, пришедшие
        # одновременно (например, вход и сразу перезаход трейдера),
        # исполнялись строго по очереди: вторая ждала, пока первая
        # полностью отработает вместе с сетевым запросом к бирже. В
        # логах это выглядело как 2.2с и 2.5с на двух сделках одного
        # рынка, хотя каждая сама по себе занимает доли секунды.
        #
        # Лок нужен только для корректной нумерации перезаходов, а это
        # короткая операция с БД — её и защищаем, ниже по коду.
        await self._execute_copy_trade_locked(
            user_id, trade_data, side, token_id, copy_start_time
        )

    async def _execute_copy_trade_locked(
        self,
        user_id: int,
        trade_data: dict,
        side: str,
        token_id: str,
        copy_start_time: float,
    ):
        async with async_session() as session:
            user = await session.get(User, user_id)
            if not user:
                logger.warning(
                    f"user={user_id}: сделка {side} не скопирована — "
                    f"пользователя нет в базе"
                )
                return
            if not user.is_active:
                logger.warning(
                    f"user={user_id}: сделка {side} не скопирована — "
                    f"копирование выключено (нажмите Старт)"
                )
                return

            # Перезаходы КОПИРУЮТСЯ (это осознанное поведение — бот
            # повторяет все новые сделки трейдера). Считаем, какой это
            # по счёту вход в данный рынок, чтобы пометить его в
            # уведомлении.
            # Номер перезахода считается позже — непосредственно перед
            # записью позиции, под коротким локом. Здесь он не нужен.
            entry_number = 1

            amount = await self._calculate_amount(user)
            if amount <= 0:
                logger.warning(f"amount=0 user={user_id}, skip trade")
                return

            logger.info(
                f"Executing copy trade user={user_id} side={side} "
                f"token={token_id[:16]}... amount={amount}"
                + (f" ПЕРЕЗАХОД #{entry_number}"
                   if entry_number > 1 else "")
            )

            # Цена копируемой сделки — уже есть в данных от watcher,
            # передаём её, чтобы в симуляции не ходить за ценой в сеть.
            try:
                price_hint = Decimal(str(trade_data.get("price", "0")))
            except Exception:
                price_hint = None

            # Разбивка по этапам.
            #
            # ms_wait — время от обнаружения сделки вотчером до начала
            # проверок: очередь Redis, создание задачи, запрос
            # пользователя из БД. Если тормозит здесь, оптимизировать
            # надо не сеть, а путь доставки.
            ms_wait = (time.monotonic() - copy_start_time) * 1000
            t_phase = time.monotonic()

            # Боевые проверки (в симуляции пропускаются, чтобы не
            # замедлять критический путь)
            if not settings.simulation_mode:
                reason = await self._preflight_checks(
                    session, user, token_id, amount, price_hint, side
                )
                if reason:
                    logger.warning(
                        f"user={user_id}: сделка пропущена — {reason}"
                    )
                    notify_user_bg(
                        user_id, f"⏭ Сделка пропущена: {reason}"
                    )
                    return

            # Если проверки подняли сумму до минимума рынка — берём её
            bumped = self._bumped_amount.pop(token_id, None)
            if bumped is not None and bumped > amount:
                amount = bumped

            ms_preflight = (time.monotonic() - t_phase) * 1000
            t_phase = time.monotonic()

            # Для продажи нужно КОЛИЧЕСТВО ДОЛЕЙ, а не сумма.
            #
            # Раньше shares не передавался вовсе, и копирование продажи
            # трейдера падало в place_market_order с "SELL без
            # количества долей" — бот покупал вслед за кошельком, но
            # закрыть позицию не мог. Берём фактический объём из своих
            # открытых позиций по этому токену.
            sell_shares = None
            if side == "SELL":
                sell_shares = await self._open_shares_for_token(
                    session, user_id, token_id
                )
                if not sell_shares or sell_shares <= 0:
                    logger.warning(
                        f"user={user_id}: трейдер ПРОДАЛ "
                        f"{token_id[:16]}..., но открытой позиции по "
                        f"этому рынку у нас нет — копировать нечего. "
                        f"Обычно значит, что вход не состоялся или "
                        f"позицию уже закрыл TP/SL."
                    )
                    notify_user_bg(
                        user_id,
                        f"ℹ️ Трейдер вышел из рынка "
                        f"{token_id[:10]}..., но у нас там нет открытой "
                        f"позиции — закрывать нечего."
                    )
                    return

            # СНАЧАЛА исполняем ордер — это критический путь
            result = await polymarket_client.place_market_order(
                token_id=token_id,
                side=side,
                amount_usdc=amount,
                price_hint=price_hint,
                user=user,
                shares=sell_shares,
                # Цена из стакана, уже полученная в проверках. Убирает
                # повторный запрос стакана внутри SDK и служит защитой
                # цены на стороне биржи.
                limit_price=self._book_price_for(token_id),
            )
            ms_order = (time.monotonic() - t_phase) * 1000
            t_phase = time.monotonic()

            # Считаем скорость СРАЗУ после исполнения ордера
            copy_elapsed_ms = (time.monotonic() - copy_start_time) * 1000
            speed_text = _format_speed(copy_elapsed_ms)

            # User WS fallback для delayed ордеров
            if (result.success
                    and result.filled_price is None
                    and ws_user_module.user_ws_manager
                    and result.tx_hash):
                try:
                    ws_event = await ws_user_module.user_ws_manager.wait_for_fill(
                        result.tx_hash, timeout=5.0
                    )
                    if ws_event:
                        result.filled_price = Decimal(
                            str(ws_event.get("price", "0"))
                        )
                        result.filled_size = Decimal(
                            str(ws_event.get("size", "0"))
                        )
                        result.tx_hash = (
                            ws_event.get("transaction_hash") or result.tx_hash
                        )
                except Exception as e:
                    logger.warning(f"wait_for_fill error: {e}")

            # Получаем название рынка ПОСЛЕ ордера
            # Не влияет на скорость копирования
            question = trade_data.get("question", "")
            outcome = (
                trade_data.get("outcome", "")
                or trade_data.get("outcome_id", "")
            )

            # Раньше здесь вызывался polymarket_client.get_market_info(),
            # которого в клиенте не существует — вызов падал на КАЖДОЙ
            # сделке ('PolymarketClient' object has no attribute
            # 'get_market_info') и просто засорял лог. Название рынка
            # берём из данных сделки, иначе — короткий fallback ниже.

            # Финальный fallback для названия
            if not question:
                question = f"Market {token_id[:8]}..."

            session.add(TradeLog(
                user_id=user_id,
                action="order_placed" if result.success else "order_failed",
                details={
                    "trade": trade_data,
                    "amount": str(amount),
                    "error": result.error,
                    "copy_elapsed_ms": round(copy_elapsed_ms, 1),
                    "question": question,
                    "outcome": outcome,
                },
            ))

            if not result.success:
                await session.commit()
                # Переводим машинные ответы биржи на человеческий
                err = result.error or "неизвестная причина"
                low = err.lower()
                if "no orders found to match" in low:
                    human = (
                        "в стакане не нашлось встречных заявок — "
                        "по этому рынку сейчас нет ликвидности по "
                        "приемлемой цене. Это состояние рынка, а не "
                        "сбой бота."
                    )
                elif "min size" in low or "invalid amount" in low:
                    human = (
                        f"сумма ставки ниже минимума площадки. "
                        f"Увеличьте ставку (сейчас {amount:.2f} USDC)."
                    )
                elif "not enough balance" in low or "insufficient" in low:
                    human = "недостаточно средств на кошельке."
                elif "allowance" in low:
                    human = (
                        "не выданы разрешения контрактам биржи — "
                        "выполните /approve."
                    )
                else:
                    human = err

                notify_user_bg(
                    user_id,
                    f"❌ Ордер не исполнен: {human}"
                )
                return

            entry_price = result.filled_price or Decimal(
                str(trade_data.get("price", "0.5"))
            )

            if side == "BUY":
                tp_price, sl_price = self._calculate_tp_sl_prices(
                    entry_price, user
                )
                # Страховка от фантомных позиций.
                #
                # Позиция без реального количества долей потом не
                # закрывается: биржа отвечает "balance: 0", стоп-лосс
                # уходит в бесконечные повторы, а средства числятся
                # вложенными. Лучше не открыть позицию вовсе, чем
                # завести запись, которой не соответствуют доли.
                filled = Decimal(str(result.filled_size or 0))
                if filled <= 0:
                    logger.error(
                        f"user={user_id}: ордер вернул нулевое "
                        f"количество долей — позиция НЕ создана "
                        f"(tx={result.tx_hash})"
                    )
                    notify_user_bg(
                        user_id,
                        "⚠️ Ордер не дал исполнения — позиция не открыта."
                    )
                    return

                # Короткий лок: только подсчёт номера входа и запись.
                # Сетевые операции уже позади, здесь лишь два запроса
                # к локальной БД — сериализация почти ничего не стоит.
                async with self._position_lock(user_id, token_id):
                    entry_number = await self._count_open_positions(
                        session, user_id, token_id
                    ) + 1

                position = Position(
                    user_id=user_id,
                    market_id=trade_data.get("market_id", ""),
                    outcome_id=outcome,
                    token_id=token_id,
                    entry_price=entry_price,
                    amount_usdc=amount,
                    shares_bought=result.filled_size or Decimal("0"),
                    tx_hash_copy=trade_data.get("tx_hash"),
                    tx_hash_ours=result.tx_hash,
                    status="open",
                    tp_price=tp_price,
                    sl_price=sl_price,
                    current_price=entry_price,
                )
                session.add(position)
                await session.commit()
                await session.refresh(position)

                try:
                    from services.tp_sl_monitor import tp_sl_monitor
                    # create_task, а не await: подписка на market WS не
                    # должна задерживать возврат из execute_copy_trade
                    asyncio.create_task(
                        tp_sl_monitor.watch_position(position)
                    )
                except Exception as e:
                    logger.error(f"tp_sl_monitor.watch_position error: {e}")

                tp_text = f"{tp_price:.4f}" if tp_price else "нет"
                sl_text = f"{sl_price:.4f}" if sl_price else "нет"

                # Пометка перезахода: показываем, что это не первая
                # позиция по этому рынку, и какая по счёту.
                if entry_number > 1:
                    header = (
                        f"🔁 <b>ПЕРЕЗАХОД #{entry_number}</b> — "
                        f"сделка BUY"
                    )
                    reentry_line = (
                        f"↩️ Вход №{entry_number} в этот рынок "
                        f"(предыдущие ещё открыты)\n"
                    )
                else:
                    header = "✅ <b>Скопирована сделка BUY</b>"
                    reentry_line = ""

                notify_user_bg(
                    user_id,
                    f"{header}\n"
                    f"📊 Рынок: {question}\n"
                    f"🎲 Исход: <b>{outcome}</b>\n"
                    f"{reentry_line}"
                    f"💰 Сумма: {amount:.2f} USDC\n"
                    f"📈 Цена входа: {entry_price:.4f}\n"
                    f"🎯 TP: {tp_text} | 🛑 SL: {sl_text}\n"
                    f"🕒 Трейдер: {_ts_str(trade_data.get('timestamp'))} · "
                    f"бот: {_now_str()}\n"
                    f"⚡ Скорость: {speed_text}\n"
                    # Разбивка прямо в уведомлении: логи до вас
                    # регулярно не доходят, а по этим трём числам
                    # сразу видно, где именно уходит время.
                    f"⚙️ ожидание {ms_wait:.0f}мс · "
                    f"проверки {ms_preflight:.0f}мс · "
                    f"ордер {ms_order:.0f}мс",
                )

                self._record_timing(copy_elapsed_ms)
                logger.info(
                    f"ТАЙМИНГ user={user_id}: "
                    f"ожидание {ms_wait:.0f}мс, "
                    f"проверки {ms_preflight:.0f}мс, "
                    f"ордер {ms_order:.0f}мс, "
                    f"запись {(time.monotonic() - t_phase) * 1000:.0f}мс"
                )
                # Отложенная сверка с биржей: ловит случаи, когда
                # ордер приняли, но он не исполнился, и позиция
                # оказалась только в нашей базе.
                self._schedule_position_verification(position.id)

                logger.info(
                    f"Position opened id={position.id} user={user_id} "
                    f"entry={entry_price} tp={tp_price} sl={sl_price} "
                    f"entry_number={entry_number} "
                    f"copy_time={copy_elapsed_ms:.0f}ms"
                )

            else:
                try:
                    # scalar_one_or_none() здесь БРОСАЛ
                    # MultipleResultsFound, если по токену накопилось
                    # больше одной открытой позиции (а из-за отсутствия
                    # проверки выше это происходило регулярно). То есть
                    # дубли не просто плодились — они ещё и ломали
                    # закрытие: SELL трейдера падал с исключением, и
                    # позиции оставались висеть открытыми навсегда.
                    # Закрываем ВСЕ открытые позиции по этому токену.
                    stmt = select(Position).where(
                        Position.user_id == user_id,
                        Position.token_id == token_id,
                        Position.status == "open"
                    )
                    positions = (
                        await session.execute(stmt)
                    ).scalars().all()
                    for pos in positions:
                        pos.status = "closed"
                        pos.current_price = entry_price
                        pos.closed_at = datetime.utcnow()
                        logger.info(
                            f"Position {pos.id} closed by SELL "
                            f"price={entry_price}"
                        )
                        # Раньше подписка на WS для этого токена не
                        # снималась НИКОГДА, если позиция закрывалась
                        # через SELL от трейдера (а не через TP/SL или
                        # resolution) — market_ws_manager._subscribed
                        # только рос за всю сессию. При реконнекте весь
                        # накопленный список уходит одним сообщением —
                        # именно это и приводило к "1008 invalid
                        # subscription payload".
                        notify_user_bg(
                            pos.user_id,
                            _build_result_message(
                                pos, "sell", exit_price=entry_price
                            ),
                        )
                        try:
                            from services.tp_sl_monitor import (
                                tp_sl_monitor,
                            )
                            # Отписываем именно ЭТУ позицию: подписка на
                            # токен останется, если по нему есть другие
                            # позиции — другого пользователя или
                            # перезаходы этого же.
                            await tp_sl_monitor.stop_watching_position(
                                pos.id
                            )
                        except Exception as e:
                            logger.warning(
                                f"unsubscribe on SELL close error: {e}"
                            )
                except Exception as e:
                    logger.error(f"close position on SELL error: {e}")

                await session.commit()

                notify_user_bg(
                    user_id,
                    f"✅ <b>Скопирована сделка SELL</b>\n"
                    f"📊 Рынок: {question}\n"
                    f"🎲 Исход: <b>{outcome}</b>\n"
                    f"💵 Цена: {entry_price:.4f}\n"
                    f"🕒 Время: {_now_str()}\n"
                    f"⚡ Скорость: {speed_text}",
                )

    async def close_position(
        self, position: Position, reason: str,
        trigger_elapsed_ms: float | None = None,
        trigger_price: Decimal | None = None,
        attempt: int = 1,
    ):
        # Лок на (пользователь, рынок): обработчики TP/SL у разных
        # позиций одного рынка срабатывают почти одновременно. Без
        # лока они успевали войти в закрытие параллельно, ещё до того
        # как первый пометит позиции закрытыми — и групповое закрытие
        # не спасало бы от конкуренции за стакан.
        async with self._position_lock(position.user_id, position.token_id):
            await self._close_position_locked(
                position, reason, trigger_elapsed_ms,
                trigger_price, attempt,
            )

    async def _close_position_locked(
        self, position: Position, reason: str,
        trigger_elapsed_ms: float | None = None,
        trigger_price: Decimal | None = None,
        attempt: int = 1,
    ):
        async with async_session() as session:
            pos = await session.get(Position, position.id)
            if not pos or pos.status != "open":
                return

            owner = await session.get(User, pos.user_id)

            # Восстановление количества долей.
            #
            # Если shares_bought по какой-то причине не сохранилось
            # (так было из-за неверного разбора ответа биржи), позиция
            # становилась НЕЗАКРЫВАЕМОЙ: SELL отклонялся с "SELL без
            # количества долей", а деньги оставались в рынке. Считаем
            # доли из вложенной суммы и цены входа, а если есть связь
            # с биржей — уточняем по фактической позиции.
            # Закрываем ВСЕ позиции по этому рынку, у которых сработал
            # тот же уровень, ОДНИМ ордером.
            #
            # Раньше каждая позиция закрывалась отдельно. При
            # перезаходах их по рынку несколько, обработчики TP/SL
            # срабатывают почти одновременно — и бот слал несколько
            # ордеров на продажу в ОДИН стакан. Первые выедали
            # доступные заявки, последнему не доставалось ничего:
            # "no orders found to match". Бот конкурировал сам с собой.
            siblings = [pos]
            if trigger_price is not None:
                try:
                    stmt = select(Position).where(
                        Position.user_id == pos.user_id,
                        Position.token_id == pos.token_id,
                        Position.status == "open",
                        Position.id != pos.id,
                    )
                    others = (await session.execute(stmt)).scalars().all()
                    px = Decimal(str(trigger_price))
                    for other in others:
                        # Берём только те, у которых уровень пробит
                        # этой же ценой — иначе закроем позицию,
                        # которая ещё не должна закрываться.
                        if reason == "tp" and other.tp_price and \
                                px >= Decimal(str(other.tp_price)):
                            siblings.append(other)
                        elif reason == "sl" and other.sl_price and \
                                px <= Decimal(str(other.sl_price)):
                            siblings.append(other)
                except Exception as e:
                    logger.warning(f"поиск позиций того же рынка: {e}")

            if len(siblings) > 1:
                logger.info(
                    f"Position {pos.id}: по рынку {pos.token_id[:12]}... "
                    f"сработал {reason} сразу у {len(siblings)} позиций "
                    f"— закрываем одним ордером"
                )

            shares = sum(
                (Decimal(str(p_.shares_bought or 0)) for p_ in siblings),
                Decimal("0"),
            )
            if shares <= 0:
                entry = Decimal(str(pos.entry_price or 0))
                if entry > 0:
                    shares = Decimal(str(pos.amount_usdc or 0)) / entry
                    logger.warning(
                        f"Position {pos.id}: shares_bought пусто, "
                        f"восстановлено из суммы и цены входа: {shares}"
                    )
                if owner and owner.proxy_wallet:
                    try:
                        live = await polymarket_client.get_positions(
                            owner.proxy_wallet
                        )
                        for lp in live:
                            if lp.asset == pos.token_id and lp.size > 0:
                                shares = Decimal(str(lp.size))
                                logger.info(
                                    f"Position {pos.id}: количество "
                                    f"уточнено по бирже: {shares}"
                                )
                                break
                    except Exception as e:
                        logger.warning(
                            f"не удалось уточнить позицию на бирже: {e}"
                        )
                if shares > 0:
                    pos.shares_bought = shares
                    await session.commit()

            # ПЕРЕД ПОВТОРНОЙ попыткой сверяемся с биржей.
            #
            # Раньше эта сверка была вложена в ветку "количество долей
            # потерялось" и при нормально записанном количестве не
            # выполнялась вовсе. Из-за этого бот продолжал слать
            # продажи по позиции, которой на бирже уже нет: лимитный
            # ордер мог исполниться сам, или позиция была закрыта
            # вручную, а в базе она оставалась открытой.
            #
            # На ПЕРВОЙ попытке проверку пропускаем: там важна скорость
            # (сработал TP/SL), а позиция почти наверняка ещё на месте.
            if attempt > 1 and owner and owner.proxy_wallet \
                    and not settings.simulation_mode:
                try:
                    live = await polymarket_client.get_positions(
                        owner.proxy_wallet
                    )
                    on_chain = Decimal("0")
                    for lp in live:
                        if lp.asset == pos.token_id:
                            on_chain = Decimal(str(lp.size or 0))
                            break

                    if on_chain <= 0:
                        logger.info(
                            f"Position {pos.id}: на бирже позиции уже "
                            f"нет — закрылась сама. Помечаем закрытой, "
                            f"повторы прекращаем."
                        )
                        pos.status = (
                            f"{reason}_hit" if reason in ("tp", "sl")
                            else "closed"
                        )
                        pos.closed_at = datetime.utcnow()
                        await session.commit()
                        self._close_retry_notified.discard(pos.id)
                        notify_user_bg(
                            pos.user_id,
                            "✅ Позиция закрылась на бирже — "
                            "лимитный ордер исполнился. Повторы "
                            "остановлены."
                        )
                        try:
                            from services.tp_sl_monitor import (
                                tp_sl_monitor,
                            )
                            await tp_sl_monitor.stop_watching_position(
                                pos.id
                            )
                        except Exception:
                            pass
                        return

                    if on_chain != shares:
                        logger.info(
                            f"Position {pos.id}: количество уточнено "
                            f"по бирже {shares} -> {on_chain}"
                        )
                        shares = on_chain
                except Exception as e:
                    logger.warning(f"сверка перед повтором: {e}")

            result = await polymarket_client.place_market_order(
                token_id=pos.token_id,
                side="SELL",
                amount_usdc=pos.amount_usdc,
                shares=shares,   # ПРОДАЁМ ДОЛИ, не доллары
                user=owner,
            )

            # Раньше позиция помечалась закрытой БЕЗУСЛОВНО. Если продажа
            # проваливалась (нет ликвидности, сеть, отказ биржи), в базе
            # она числилась закрытой, а доли оставались у пользователя:
            # мониторинг снимался, TP/SL больше не срабатывал, при
            # разрешении рынка позицию не гасили. Теперь при неудаче
            # позиция остаётся открытой и будет закрыта следующей
            # попыткой.
            if not result.success:
                logger.error(
                    f"Position {pos.id}: SELL НЕ исполнен ({result.error}) "
                    f"— позиция остаётся открытой"
                )
                session.add(TradeLog(
                    user_id=pos.user_id, position_id=pos.id,
                    action=f"{reason}_failed",
                    details={"error": result.error},
                ))
                await session.commit()
                # Уведомляем только о ПЕРВОЙ неудаче: повторов до пяти,
                # и десять одинаковых сообщений подряд только мешают.
                if pos.id not in self._close_retry_notified:
                    self._close_retry_notified.add(pos.id)
                    notify_user_bg(
                        pos.user_id,
                        f"⚠️ Не удалось закрыть позицию ({reason}): "
                        f"{result.error}. Пробую повторно."
                    )

                # Нет встречных заявок — выставляем ЛИМИТНУЮ продажу.
                #
                # Рыночный ордер (FAK) исполняется только тем, что
                # стоит в стакане прямо сейчас. На тонком рынке его
                # просто уничтожает, и повтор через время продаёт уже
                # по худшей цене — именно это и портило исход.
                #
                # Лимитный ордер остаётся в стакане и исполнится сам,
                # как только появится покупатель. Цену берём чуть ниже
                # текущего бида, чтобы встать первыми в очередь.
                err = (result.error or "").lower()

                # Стакана больше нет = рынок РАЗРЕШИЛСЯ.
                #
                # Продать там невозможно в принципе: торговля
                # закончена, позиция теперь либо гасится (если
                # выиграла), либо обнуляется. Раньше бот этого не
                # различал и бесконечно пытался продать — в логе шли
                # десятки "No orderbook exists" подряд.
                if "no orderbook exists" in err or "market is closed" in err:
                    logger.info(
                        f"Position {pos.id}: рынок разрешился, продажа "
                        f"невозможна — переходим к погашению"
                    )
                    await session.commit()
                    await self._settle_resolved_position(pos.id)
                    return

                no_liquidity = (
                    "no orders found" in err
                    or "no resting liquidity" in err
                )
                if no_liquidity and shares > 0:
                    limit = self._resting_sell_price(pos.token_id)
                    if limit:
                        rest = await polymarket_client.place_resting_sell(
                            token_id=pos.token_id, shares=shares,
                            price=limit, user=owner,
                        )
                        if rest.success:
                            logger.info(
                                f"Position {pos.id}: выставлена лимитная "
                                f"продажа {shares} долей по {limit} — "
                                f"исполнится при появлении покупателя"
                            )
                            # Запоминаем, что по позиции уже стоит
                            # заявка в стакане. Повтор рыночной продажи
                            # при живой лимитной приводил к тому, что
                            # бот слал ордера по позиции, которая вот-вот
                            # закроется сама.
                            pos.tx_hash_ours = rest.tx_hash or pos.tx_hash_ours
                            await session.commit()
                            notify_user_bg(
                                pos.user_id,
                                f"📋 В стакане нет встречных заявок. "
                                f"Выставил лимитную продажу по {limit:.4f} "
                                f"— она исполнится, как только появится "
                                f"покупатель."
                            )
                            self._schedule_close_retry(
                                pos.id, reason, attempt + 1
                            )
                            return

                # Обычный повтор, если лимитную выставить не вышло.
                # ВАЖНО: передаём attempt + 1. Раньше здесь вызывался
                # планировщик с умолчанием attempt=1, поэтому счётчик
                # никогда не рос: в логе вечно повторялось "попытка 2",
                # и бот долбил продажу бесконечно.
                self._schedule_close_retry(pos.id, reason, attempt + 1)
                return

            new_status = (
                f"{reason}_hit" if reason in ("tp", "sl") else "closed"
            )
            for p_ in siblings:
                p_.status = new_status
                p_.closed_at = datetime.utcnow()
                p_.tx_hash_ours = result.tx_hash or p_.tx_hash_ours
                if result.filled_price:
                    p_.current_price = result.filled_price

            session.add(TradeLog(
                user_id=pos.user_id,
                position_id=pos.id,
                action=f"{reason}_triggered",
                details={
                    "tx": result.tx_hash,
                    "success": result.success
                },
            ))
            await session.commit()

            # Цена выхода: фактический филл, иначе последняя известная
            exit_price = result.filled_price or pos.current_price
            if len(siblings) > 1:
                notify_user_bg(
                    pos.user_id,
                    f"ℹ️ По рынку {pos.token_id[:10]}... закрыто "
                    f"{len(siblings)} позиций одним ордером "
                    f"({shares} долей) — иначе они конкурировали бы "
                    f"за один стакан."
                )
            notify_user_bg(
                pos.user_id,
                _build_result_message(
                    pos, reason, exit_price,
                    trigger_elapsed_ms=trigger_elapsed_ms,
                    trigger_price=trigger_price,
                ),
            )
            logger.info(f"Position {pos.id} closed reason={reason}")

    async def redeem_resolved_position(self, position: Position, won: bool):
        async with async_session() as session:
            pos = await session.get(Position, position.id)
            if not pos or pos.status != "open":
                return

            tx_hash = None
            if won:
                try:
                    owner = await session.get(User, pos.user_id)
                    result = await polymarket_client.redeem_positions(
                        pos.market_id, user=owner
                    )
                    tx_hash = result.tx_hash if result.success else None
                    if not result.success:
                        logger.error(
                            f"Position {pos.id}: погашение НЕ прошло "
                            f"({result.error}). Выигрыш остался в "
                            f"токенах — погасите вручную на "
                            f"polymarket.com."
                        )
                        notify_user_bg(
                            pos.user_id,
                            f"⚠️ Выигрыш не удалось получить "
                            f"автоматически: {result.error}. "
                            f"Погасите позицию вручную на "
                            f"polymarket.com."
                        )
                except Exception as e:
                    logger.error(f"redeem_positions error: {e}")

            pos.status = "resolved_won" if won else "resolved_lost"
            pos.closed_at = datetime.utcnow()
            pos.tx_hash_ours = tx_hash or pos.tx_hash_ours

            session.add(TradeLog(
                user_id=pos.user_id,
                position_id=pos.id,
                action="market_resolved",
                details={"won": won, "tx_hash": tx_hash},
            ))
            await session.commit()

            notify_user_bg(
                pos.user_id,
                _build_result_message(
                    pos, "resolved", exit_price=None, won=won
                ),
            )
            logger.info(f"Position {pos.id} resolved won={won}")


trader_service = TraderService()