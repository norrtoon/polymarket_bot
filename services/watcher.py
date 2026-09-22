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
# Постраничная выборка: размер страницы и потолок страниц.
# 50 x 10 = до 500 сделок за опрос — с запасом покрывает
# любую серию и простой при перезапуске.
PAGE_SIZE = 50
MAX_PAGES = 10
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


def _cursor_key(user_id: int, wallet: str) -> str:
    """Курсор копирования: метка времени последней обработанной сделки."""
    return f"copy_cursor:{user_id}:{(wallet or '').lower()}"


def _cache_key(user_id: int, wallet: str) -> str:
    """
    Ключ окна дедупликации — отдельный на КАЖДЫЙ кошелёк.

    Привязка только к пользователю приводила к тому, что при смене
    отслеживаемого кошелька бот видел непустой кэш от прежнего и
    трактовал запуск как перезапуск: рубеж не выставлялся, и вся
    недавняя история нового кошелька копировалась разом.
    """
    return f"recent_trade_keys:{user_id}:{(wallet or '').lower()}"


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

    async def start_watching(
        self, user_id: int, wallet: str, resume: bool = False
    ):
        """
        resume=False — пользователь нажал Старт: копируем строго с
        этого момента, всё сделанное раньше пропускаем.
        resume=True — автовосстановление после перезапуска бота:
        догоняем сделки, сделанные за время простоя.
        """
        wallet = wallet.lower().strip()
        async with self._lock:
            await self._detach_locked(user_id)

            generation = await redis_client.incr(f"watch_gen:{user_id}")
            sub = _Subscriber(user_id, generation)

            # Точка отсчёта в шкале времени API: помечаем текущие сделки
            # кошелька виденными, чтобы не копировать историю. Локальные
            # часы не используются — они могут расходиться с сервером
            # (у Docker Desktop на macOS это обычное дело).
            ok = await self._snapshot_baseline(sub, wallet, resume)
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

    async def _snapshot_baseline(
        self, sub: _Subscriber, wallet: str, resume: bool = False
    ) -> bool:
        """
        Выставить курсор: с какой сделки начинать копирование.

        Курсор — это метка времени последней обработанной сделки. Бот
        копирует ВСЁ, что новее курсора, и после обработки сдвигает
        его вперёд. Хранится в Redis и переживает перезапуск.

        Раньше вместо курсора был фильтр по возрасту: сделка старше N
        секунд относительно самой свежей в ответе отбрасывалась. Из-за
        этого терялись настоящие сделки — при задержке Data API, при
        перезапуске, при серии быстрых входов. Курсор не теряет ничего:
        всё, что трейдер сделал после Старта, будет скопировано.
        """
        cursor_key = _cursor_key(sub.user_id, wallet)

        for attempt in range(3):
            try:
                trades = await polymarket_client.get_wallet_trades(
                    wallet, limit=BASELINE_LIMIT
                )
                break
            except RateLimited:
                await asyncio.sleep(1.5 * (attempt + 1))
            except Exception as e:
                logger.warning(f"baseline user={sub.user_id}: {e}")
                return False
        else:
            return False

        newest = max((t.timestamp for t in trades), default=0)
        stored_raw = await redis_client.get(cursor_key)
        stored = float(stored_raw) if stored_raw else None

        if resume and stored is not None:
            # АВТОВОССТАНОВЛЕНИЕ после перезапуска: догоняем простой.
            #
            # Но не бесконечно назад. Если бот лежал часами, сделки
            # того времени уже неактуальны: трейдер мог давно из них
            # выйти, цены ушли. Догоняем только разумное окно.
            catchup = getattr(settings, "resume_catchup_seconds", 600)
            floor = newest - catchup if newest else stored
            cursor = max(stored, floor)
            if cursor > stored:
                logger.warning(
                    f"user={sub.user_id}: бот простаивал дольше "
                    f"{catchup}с — сделки старше {_fmt_ts(cursor)} "
                    f"пропущены как неактуальные"
                )
            logger.info(
                f"user={sub.user_id}: восстановление — копируем всё "
                f"новее {_fmt_ts(cursor)}"
            )
        else:
            # ЯВНЫЙ СТАРТ (или первый запуск для кошелька): копируем
            # строго с этого момента. Всё сделанное до нажатия Старта
            # — история, её не трогаем.
            cursor = newest
            cache_key = _cache_key(sub.user_id, wallet)
            seen = [_trade_key(t) for t in trades]
            await redis_client.set(
                cache_key, json.dumps(seen[-SEEN_WINDOW:])
            )
            logger.info(
                f"user={sub.user_id}: СТАРТ — копируем все сделки "
                f"новее {_fmt_ts(cursor)}"
            )

        sub.baseline_ts = cursor
        await redis_client.set(cursor_key, str(cursor))
        return True

    async def _fetch_new_trades(self, wallet: str, subs: dict) -> list:
        """
        Забрать ВСЕ сделки новее курсора, листая страницы.

        Раньше брались только последние POLL_LIMIT сделок. Если трейдер
        делал больше между опросами (серия быстрых входов, перезапуск
        бота, задержка Data API), всё, что не влезло в эту выборку,
        терялось безвозвратно — бот "не видел" часть ставок.

        Листаем, пока не дойдём до сделок старше самого раннего курсора
        среди подписчиков. Потолок страниц — защита от бесконечного
        листания, если что-то пошло не так.
        """
        min_cursor = min(
            (sub.baseline_ts for sub in subs.values()), default=0.0
        )

        collected = []
        offset = 0
        for _page in range(MAX_PAGES):
            page = await polymarket_client.get_wallet_trades(
                wallet, limit=PAGE_SIZE, offset=offset
            )
            if not page:
                break
            collected.extend(page)

            # Сделки идут от новых к старым. Если самая старая на
            # странице уже не новее курсора — дальше листать незачем.
            oldest = min(t.timestamp for t in page)
            if oldest <= min_cursor or len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        else:
            logger.warning(
                f"{wallet[:12]}...: достигнут потолок в {MAX_PAGES} "
                f"страниц — возможно, часть сделок не выбрана"
            )

        return collected

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
                trades = await self._fetch_new_trades(wallet, subs)
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
                            await self._dispatch(sub, trades, wallet)
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

    async def _dispatch(
        self, sub: _Subscriber, trades: list, wallet: str
    ):
        """Отдать сделки ОДНОМУ подписчику с его дедупликацией."""
        user_id = sub.user_id
        cache_key = _cache_key(user_id, wallet)

        # Пользователь мог нажать Стоп/Старт — тогда его поколение
        # выросло, и эта подписка уже неактуальна.
        current_gen = await redis_client.get(f"watch_gen:{user_id}")
        if current_gen is None or int(current_gen) != sub.generation:
            # Раньше здесь был ТИХИЙ выход: если поколение разъехалось,
            # сделки молча выбрасывались, и в логе не было ни строки.
            # Выглядело как "бот ничего не копирует" без объяснений.
            logger.warning(
                f"user={user_id}: сделки отброшены — поколение "
                f"подписки {sub.generation}, текущее {current_gen}. "
                f"Нажмите Стоп и Старт, чтобы пересоздать слежку."
            )
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

        # ПРЕДОХРАНИТЕЛЬ на размер пачки.
        #
        # Сегодня из-за ошибки в ключе кэша бот скопировал всю недавнюю
        # историю кошелька разом — полтора десятка сделок в одну
        # секунду, включая уже разрешившиеся рынки, и это съело весь
        # баланс. Нормальный трейдер не совершает столько сделок
        # мгновенно: такая пачка почти всегда означает сбой, а не
        # реальную активность.
        #
        # Копируем не больше разрешённого за один опрос. Остальные
        # помечаем виденными, чтобы они не хлынули на следующем цикле.
        publish_budget = settings.max_copies_per_poll

        # Что реально вернул API — до всякой фильтрации.
        # Без этого невозможно отличить "бот отфильтровал продажу" от
        # "трейдер её не совершал" или "API её не отдаёт".
        sides = {}
        for t in trades:
            sides[t.side] = sides.get(t.side, 0) + 1
        new_count = sum(
            1 for t in trades if _trade_key(t) not in seen_set
        )
        if new_count:
            logger.info(
                f"user={user_id}: от API получено {len(trades)} сделок "
                f"({', '.join(f'{k}: {v}' for k, v in sides.items())}), "
                f"новых для нас: {new_count}"
            )

        for t in reversed(trades):  # от старых к новым
            key = _trade_key(t)
            if key in seen_set:
                continue

            skip_reason = None
            # Единственный временной фильтр — курсор.
            #
            # Сделки новее курсора копируются ВСЕ. Раньше здесь был ещё
            # фильтр по возрасту относительно самой свежей сделки в
            # ответе: он отбрасывал настоящие сделки при задержке Data
            # API, при серии быстрых входов и после перезапуска. Курсор
            # сам по себе гарантирует, что история до Старта не
            # скопируется, а всё после — скопируется.
            #
            # Строго меньше: сделки с той же секундой, что и курсор,
            # отсекаются окном уже виденных (seen_set), а не временем —
            # иначе в одной секунде можно потерять вторую сделку.
            if t.timestamp < sub.baseline_ts:
                skip_reason = "до Старта"
            elif t.timestamp == sub.baseline_ts and key in seen_set:
                skip_reason = "уже обработана"


            if skip_reason:
                # WARNING, а не INFO: пропуск сделки — это то, что
                # пользователь замечает как "бот не видит ставки".
                # Такие строки должны быть заметны в логе.
                logger.warning(
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
            elif settings.max_copies_per_poll and publish_budget <= 0:
                logger.warning(
                    f"user={user_id}: {t.tx_hash[:18]}... НЕ скопирована "
                    f"— за один опрос уже скопировано "
                    f"{settings.max_copies_per_poll} сделок. Похоже на "
                    f"аномальную пачку; если это нормальная активность "
                    f"трейдера, поднимите MAX_COPIES_PER_POLL."
                )
            else:
                publish_budget -= 1
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
                        # Насколько поздно бот УВИДЕЛ сделку: время
                        # биржи сейчас минус время блока сделки. Это
                        # задержка Data API + интервал опроса — то, что
                        # не видно в "скорости копирования" (та меряется
                        # от момента обнаружения).
                        "detect_lag_s": round(
                            polymarket_client.polymarket_now()
                            - float(t.timestamp), 1
                        ),
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

            # Сдвигаем курсор: эта сделка обработана. Хранится в Redis
            # и переживает перезапуск — после рестарта бот продолжит
            # ровно с этого места, не потеряв и не повторив ничего.
            if t.timestamp > sub.baseline_ts:
                sub.baseline_ts = float(t.timestamp)
                await redis_client.set(
                    _cursor_key(user_id, wallet), str(sub.baseline_ts)
                )

    async def _claim_market_entry(self, user_id: int, t) -> bool:
        """
        Отличить транзакции ОДНОГО ордера от настоящего перезахода.

        Раньше ключ ставился в Redis с TTL по НАСТЕННЫМ ЧАСАМ бота, то
        есть окно отсчитывалось от момента ОБНАРУЖЕНИЯ сделки. А
        обнаружение зависит от интервала опроса, задержек Data API и
        бэкоффа при 429 — поэтому окно вело себя непредсказуемо:
          * трейдер разбил ордер на две транзакции за 2 секунды, а бот
            получил их с разницей в 20 — они НЕ схлопывались;
          * трейдер реально перезашёл через 10 секунд — попадал в окно
            и ПРОПАДАЛ.
        Менять размер окна не помогало, потому что дело было не в
        размере, а в точке отсчёта.

        Теперь сравниваются метки времени САМИХ СДЕЛОК (шкала API).
        Транзакции одного ордера всегда лежат в пределах пары секунд
        друг от друга, а перезаход отличается заметно сильнее — и это
        не зависит от того, когда бот успел их увидеть.
        """
        key = f"copied_entry:{user_id}:{t.token_id}:{t.side}"
        window = max(1, int(settings.copy_dedup_window_seconds))
        maker_window = int(getattr(settings, "maker_fill_window_seconds", 900))
        price = float(t.price or 0)

        try:
            stored = await redis_client.get(key)

            if stored is not None:
                try:
                    first_ts_s, first_px_s = (str(stored).split("|") + ["0"])[:2]
                    first_ts = int(float(first_ts_s))
                    first_px = float(first_px_s)
                except (TypeError, ValueError):
                    first_ts, first_px = None, 0.0

                if first_ts is not None:
                    gap = abs(int(t.timestamp) - first_ts)

                    # 1) Транзакции одного рыночного ордера — в пределах
                    #    пары секунд, цена значения не имеет.
                    if gap <= window:
                        logger.info(
                            f"user={user_id}: {t.tx_hash[:18]}... — "
                            f"часть того же ордера (разница {gap}с, "
                            f"окно {window}с)"
                        )
                        return False

                    # 2) Куски одного ЛИМИТНОГО ордера.
                    #
                    # Лимитка трейдера часто исполняется частями: разные
                    # покупатели забирают её в течение нескольких минут.
                    # Каждая часть приходит отдельной сделкой с разным
                    # временем — и без этой проверки бот скопировал бы
                    # ОДНУ ставку трейдера многими полными ставками.
                    #
                    # Признак: части одного лимитного ордера исполняются
                    # РОВНО по его лимитной цене. Настоящий перезаход
                    # трейдера почти всегда идёт по другой цене.
                    same_price = first_px > 0 and abs(price - first_px) < 0.0005
                    if same_price and gap <= maker_window:
                        logger.info(
                            f"user={user_id}: {t.tx_hash[:18]}... — "
                            f"часть того же ЛИМИТНОГО ордера (та же цена "
                            f"{price:.4f}, прошло {gap}с) — не копируем"
                        )
                        return False

                    logger.info(
                        f"user={user_id}: {t.tx_hash[:18]}... — "
                        f"ПЕРЕЗАХОД в {t.token_id[:12]}... (прошло {gap}с, "
                        f"цена {first_px:.4f} -> {price:.4f})"
                    )

            # Новая группа: запоминаем время И цену этой сделки
            await redis_client.set(
                key, f"{int(t.timestamp)}|{price}",
                ex=max(3600, maker_window * 2),
            )
            return True

        except Exception as e:
            logger.warning(f"_claim_market_entry: {e}")
            return True



wallet_watcher = WalletWatcher()