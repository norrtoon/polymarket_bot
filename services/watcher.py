"""
Слежка за кошельками трейдеров.

АРХИТЕКТУРА: один цикл опроса на КОШЕЛЁК, а не на пользователя.

Раньше каждый пользователь запускал собственный цикл опроса Data API.
Нагрузка росла линейно с числом клиентов, а лимит Cloudflare считается
ПО IP на весь сервер — при двух пользователях 429 шли уже потоком, при
десяти бот захлебнулся бы. Вдобавок, если несколько человек копируют
одного трейдера, это были одинаковые запросы за одними и теми же
данными.

Теперь: кошелёк опрашивается ОДИН раз, результат раздаётся всем
подписанным на него пользователям. Каждый пользователь сохраняет свою
точку отсчёта, своё окно дедупликации и своё поколение — то есть
подключение и отключение одного не влияет на остальных.
"""
import asyncio
import json
import random
import time
from datetime import datetime, timezone
from loguru import logger

from core.config import settings
from core.redis_client import redis_client
from poly.client import polymarket_client, RateLimited

POLL_LIMIT = 12
BASELINE_LIMIT = 10
SEEN_WINDOW = 200


def _trade_key(t) -> str:
    """
    Ключ сделки для дедупликации.

    Data API не даёт уникального ID филла — только transactionHash,
    asset, side, size, price, timestamp. Один ордер трейдера
    исполняется НЕСКОЛЬКИМИ транзакциями (по одной на контрагента),
    поэтому price/size в ключ не входят: иначе каждый филл копировался
    бы отдельной ставкой.
    """
    return f"{t.tx_hash}:{t.token_id}:{t.side}"


def _fmt_ts(ts: float) -> str:
    try:
        return datetime.fromtimestamp(
            float(ts), tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ts)


class _Subscriber:
    """Состояние одного пользователя, следящего за кошельком."""

    __slots__ = ("user_id", "generation", "baseline_ts", "baseline_pending")

    def __init__(self, user_id: int, generation: int):
        self.user_id = user_id
        self.generation = generation
        self.baseline_ts: float = 0.0
        self.baseline_pending: bool = True


class WalletWatcher:
    def __init__(self):
        # кошелёк -> задача опроса
        self._wallet_tasks: dict[str, asyncio.Task] = {}
        # кошелёк -> {user_id: _Subscriber}
        self._subscribers: dict[str, dict[int, _Subscriber]] = {}
        # user_id -> кошелёк (чтобы знать, откуда отписывать)
        self._user_wallet: dict[int, str] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Публичный интерфейс
    # ------------------------------------------------------------------

    async def start_watching(self, user_id: int, wallet: str):
        wallet = wallet.lower().strip()
        async with self._lock:
            await self._detach_locked(user_id)

            generation = await redis_client.incr(f"watch_gen:{user_id}")
            sub = _Subscriber(user_id, generation)

            # Точка отсчёта в шкале времени API: помечаем текущие сделки
            # кошелька виденными, чтобы не копировать историю. Локальные
            # часы не используются — они могут расходиться с сервером
            # (у Docker Desktop на macOS это обычное дело).
            ok = await self._snapshot_baseline(sub, wallet)
            sub.baseline_pending = not ok

            self._subscribers.setdefault(wallet, {})[user_id] = sub
            self._user_wallet[user_id] = wallet

            if wallet not in self._wallet_tasks or \
                    self._wallet_tasks[wallet].done():
                task = asyncio.create_task(self._poll_wallet(wallet))
                self._wallet_tasks[wallet] = task
                logger.info(f"Запущен опрос кошелька {wallet[:12]}...")

            logger.info(
                f"Watcher started user={user_id} wallet={wallet[:12]}... "
                f"gen={generation} подписчиков на кошельке="
                f"{len(self._subscribers[wallet])}"
            )

        await self._start_equity_refresher(user_id)

    async def stop_watching(self, user_id: int):
        async with self._lock:
            await self._detach_locked(user_id)
            await redis_client.incr(f"watch_gen:{user_id}")
        try:
            from services.trader import trader_service
            await trader_service.stop_equity_refresher(user_id)
        except Exception as e:
            logger.debug(f"equity refresher stop skipped: {e}")

    async def _detach_locked(self, user_id: int):
        wallet = self._user_wallet.pop(user_id, None)
        if not wallet:
            return
        subs = self._subscribers.get(wallet, {})
        subs.pop(user_id, None)
        logger.info(f"Watcher stopped user={user_id}")

        # Кошелёк больше никому не нужен — гасим его цикл опроса,
        # чтобы не тратить квоту Data API впустую.
        if not subs:
            self._subscribers.pop(wallet, None)
            task = self._wallet_tasks.pop(wallet, None)
            if task and not task.done():
                task.cancel()
            logger.info(
                f"Опрос кошелька {wallet[:12]}... остановлен "
                f"(подписчиков не осталось)"
            )

    async def _start_equity_refresher(self, user_id: int):
        try:
            from services.trader import trader_service
            from core.database import async_session
            from models.user import User
            async with async_session() as sess:
                u = await sess.get(User, user_id)
                own_wallet = u.proxy_wallet if u else None
            if own_wallet:
                await trader_service.start_equity_refresher(
                    user_id, own_wallet
                )
        except Exception as e:
            logger.debug(f"equity refresher start skipped: {e}")

    # ------------------------------------------------------------------
    # Точка отсчёта
    # ------------------------------------------------------------------

    async def _snapshot_baseline(self, sub: _Subscriber, wallet: str) -> bool:
        cache_key = f"recent_trade_keys:{sub.user_id}"
        for attempt in range(3):
            try:
                trades = await polymarket_client.get_wallet_trades(
                    wallet, limit=BASELINE_LIMIT
                )
            except RateLimited:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            except Exception as e:
                logger.warning(f"baseline user={sub.user_id}: {e}")
                return False

            if not trades:
                sub.baseline_ts = 0.0
                return True

            sub.baseline_ts = max(t.timestamp for t in trades)

            # Кэш дедупликации НЕ стираем, а дополняем.
            #
            # Раньше он очищался при каждом Старте — и после Стоп ->
            # Старт бот заново копировал сделки последних минут, потому
            # что Data API отдаёт данные с задержкой и новая точка
            # отсчёта оказывалась в прошлом. Позиции открывались по
            # устаревшим ценам и мгновенно закрывались по TP/SL.
            existing = await redis_client.get(cache_key)
            seen = json.loads(existing) if existing else []
            for t in trades:
                k = _trade_key(t)
                if k not in seen:
                    seen.append(k)
            await redis_client.set(
                cache_key, json.dumps(seen[-SEEN_WINDOW:])
            )

            skew = time.time() - sub.baseline_ts
            if abs(skew) > 300:
                logger.warning(
                    f"⚠️ Часы контейнера расходятся с временем сделок "
                    f"Polymarket на {skew / 60:.0f} мин. На копирование "
                    f"это не влияет (сравниваем только метки API), но "
                    f"время в уведомлениях будет неверным."
                )

            logger.info(
                f"baseline user={sub.user_id}: рубеж "
                f"{_fmt_ts(sub.baseline_ts)}, копируем только более "
                f"поздние сделки"
            )
            return True
        return False

    # ------------------------------------------------------------------
    # Опрос кошелька
    # ------------------------------------------------------------------

    async def _poll_wallet(self, wallet: str):
        """
        ОДИН цикл на кошелёк. Результат раздаётся всем подписчикам.
        """
        consecutive_rate_limits = 0
        effective_interval = settings.poll_interval_seconds
        last_rl = polymarket_client.rate_limit_events
        last_heartbeat = time.monotonic()
        polls = 0

        while True:
            subs = dict(self._subscribers.get(wallet, {}))
            if not subs:
                logger.info(
                    f"Опрос {wallet[:12]}...: подписчиков нет, выходим"
                )
                return

            # Интервал масштабируется по числу ОПРАШИВАЕМЫХ КОШЕЛЬКОВ,
            # а не пользователей: несколько человек на одном трейдере
            # обслуживаются одним запросом и нагрузку не увеличивают.
            base = settings.poll_interval_seconds * max(
                1, len(self._wallet_tasks)
            )
            if effective_interval < base:
                effective_interval = base

            sleep_for = effective_interval * random.uniform(0.85, 1.15)

            try:
                trades = await polymarket_client.get_wallet_trades(
                    wallet, limit=POLL_LIMIT
                )
                consecutive_rate_limits = max(
                    0, consecutive_rate_limits - 1
                )

                rl_now = polymarket_client.rate_limit_events
                if rl_now > last_rl:
                    effective_interval = min(effective_interval * 1.4, 8.0)
                else:
                    effective_interval = max(effective_interval * 0.9, base)
                last_rl = rl_now

                polls += 1
                if time.monotonic() - last_heartbeat >= 300:
                    logger.info(
                        f"опрос жив: {wallet[:12]}... подписчиков="
                        f"{len(subs)} опросов за 5 мин={polls} "
                        f"интервал={effective_interval:.2f}s"
                    )
                    last_heartbeat = time.monotonic()
                    polls = 0

                if trades:
                    for sub in subs.values():
                        try:
                            await self._dispatch(sub, trades)
                        except Exception as e:
                            logger.error(
                                f"dispatch user={sub.user_id}: "
                                f"{type(e).__name__}: {e}"
                            )

            except asyncio.CancelledError:
                raise
            except RateLimited:
                consecutive_rate_limits += 1
                sleep_for = min(3.0 * consecutive_rate_limits, 60.0)
                logger.warning(
                    f"rate limit на {wallet[:12]}..., "
                    f"backoff {sleep_for:.1f}s "
                    f"(подряд: {consecutive_rate_limits})"
                )
            except Exception as e:
                logger.error(
                    f"poll {wallet[:12]}...: {type(e).__name__}: {e}"
                )

            await asyncio.sleep(sleep_for)

    async def _dispatch(self, sub: _Subscriber, trades: list):
        """Отдать сделки ОДНОМУ подписчику с его дедупликацией."""
        user_id = sub.user_id
        cache_key = f"recent_trade_keys:{user_id}"

        # Пользователь мог нажать Стоп/Старт — тогда его поколение
        # выросло, и эта подписка уже неактуальна.
        current_gen = await redis_client.get(f"watch_gen:{user_id}")
        if current_gen is None or int(current_gen) != sub.generation:
            return

        if sub.baseline_pending:
            sub.baseline_ts = max(t.timestamp for t in trades)
            await redis_client.set(
                cache_key,
                json.dumps([_trade_key(t) for t in trades][-SEEN_WINDOW:]),
            )
            sub.baseline_pending = False
            logger.info(
                f"user={user_id}: рубеж снят с задержкой — "
                f"{_fmt_ts(sub.baseline_ts)}"
            )
            return

        seen_raw = await redis_client.get(cache_key)
        seen_list = json.loads(seen_raw) if seen_raw else []
        seen_set = set(seen_list)

        batch_newest = max(t.timestamp for t in trades)

        for t in reversed(trades):  # от старых к новым
            key = _trade_key(t)
            if key in seen_set:
                continue

            skip_reason = None
            if t.timestamp < sub.baseline_ts:
                skip_reason = "старее рубежа"
            elif settings.max_trade_age_seconds and (
                batch_newest - t.timestamp
            ) > settings.max_trade_age_seconds:
                # Протухшая сделка: цена рынка уже ушла, позиция
                # открылась бы по исторической цене и мгновенно
                # закрылась по TP/SL.
                skip_reason = (
                    f"устарела на {batch_newest - t.timestamp}с"
                )

            if skip_reason:
                logger.info(
                    f"user={user_id}: {t.tx_hash[:18]}... пропущена "
                    f"({skip_reason})"
                )
            elif not await self._claim_market_entry(user_id, t):
                # Частая причина "перезаходы не работают": окно
                # схлопывания ещё не истекло. Оно нужно, чтобы один
                # ордер трейдера, разбитый на несколько транзакций, не
                # копировался несколько раз — но если трейдер реально
                # перезаходит быстрее этого окна, вход будет пропущен.
                logger.info(
                    f"user={user_id}: {t.tx_hash[:18]}... "
                    f"({t.side} {t.token_id[:12]}...) пропущена — окно "
                    f"схлопывания {settings.copy_dedup_window_seconds}с "
                    f"ещё не истекло. Если это был настоящий перезаход, "
                    f"уменьшите COPY_DEDUP_WINDOW_SECONDS."
                )
            else:
                # Прогрев начинаем ПРЯМО СЕЙЧАС, до публикации.
                #
                # Раньше подготовка (стакан, метаданные рынка в SDK)
                # стартовала только когда сделка доходила до трейдера —
                # то есть после публикации в Redis, подписки, создания
                # задачи и похода в БД за пользователем. Всё это время
                # сетевые запросы просто не были начаты, и их полная
                # стоимость ложилась на критический путь.
                #
                # Теперь они летят параллельно с доставкой сделки: к
                # моменту, когда трейдер до неё доберётся, ответы уже
                # готовы или почти готовы. Это же убирает разброс —
                # именно холодный старт давал скачки до 2 секунд.
                try:
                    from services.trader import trader_service
                    trader_service.prewarm_for_trade(user_id, t.token_id)
                except Exception as e:
                    logger.debug(f"prewarm пропущен: {e}")

                await redis_client.publish(
                    f"new_trade:{user_id}",
                    json.dumps({
                        "tx_hash": t.tx_hash,
                        "market_id": t.market_id,
                        "outcome_id": t.outcome_id,
                        "token_id": t.token_id,
                        "side": t.side,
                        "price": str(t.price),
                        "size": str(t.size),
                        "usdc_amount": str(t.usdc_amount),
                        "timestamp": t.timestamp,
                        "question": "",
                        "outcome": t.outcome_id,
                        "detected_at": time.monotonic(),
                    })
                )
                logger.info(
                    f"New trade detected user={user_id}: {t.tx_hash} "
                    f"side={t.side} trade_time={_fmt_ts(t.timestamp)}"
                )

            seen_set.add(key)
            seen_list.append(key)
            seen_list = seen_list[-SEEN_WINDOW:]
            await redis_client.set(cache_key, json.dumps(seen_list))

    async def _claim_market_entry(self, user_id: int, t) -> bool:
        """
        Занять право скопировать вход в рынок.

        Один ордер трейдера Polymarket проводит несколькими
        транзакциями подряд. Первая занимает ключ на
        copy_dedup_window_seconds, остальные видят занятый ключ и
        копию не создают.
        """
        key = f"copied_entry:{user_id}:{t.token_id}:{t.side}"
        try:
            claimed = await redis_client.set(
                key, t.tx_hash, nx=True,
                ex=settings.copy_dedup_window_seconds,
            )
            return bool(claimed)
        except Exception as e:
            logger.warning(f"_claim_market_entry: {e}")
            return True


wallet_watcher = WalletWatcher()