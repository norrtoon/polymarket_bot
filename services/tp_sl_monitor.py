import asyncio
import time
from decimal import Decimal
from typing import Awaitable, Callable
from loguru import logger

from core.config import settings
from sqlalchemy import select

from core.database import async_session
from models.position import Position
from poly.client import polymarket_client
from poly.ws_market import market_ws_manager



def _positive(value) -> Decimal | None:
    """
    Цена, только если она реальная (> 0).

    Когда покупателей в стакане нет, биржа передаёт лучший бид как "0".
    Это НЕ цена — это отсутствие цены. Раньше бот читал "0" как "цена
    упала до нуля": 0 меньше любого стоп-уровня, и стоп срабатывал. На
    коротких рынках (5-минутные Up/Down) все заявки снимаются в момент
    закрытия — поэтому ложные стопы шли ровно на границах пятиминуток,
    по нескольку в одну секунду на разных рынках.
    """
    if value in (None, ""):
        return None
    try:
        d = Decimal(str(value))
    except Exception:
        return None
    return d if d > 0 else None


def _bid_side_emptied(event: dict) -> bool:
    """
    Событие говорит, что покупателей в стакане НЕ ОСТАЛОСЬ.

    Так бывает, когда стакан очищается целиком: на спортивных рынках
    Polymarket по правилам снимает все лимитные ордера в момент начала
    матча, у коротких рынков — при закрытии.
    """
    et = event.get("event_type")
    if et == "best_bid_ask":
        return event.get("best_bid") not in (None, "") and \
            _positive(event.get("best_bid")) is None
    if et == "price_change":
        changes = event.get("price_changes") or []
        bids = [ch.get("best_bid") for ch in changes
                if ch.get("best_bid") not in (None, "")]
        return bool(bids) and all(_positive(b) is None for b in bids)
    if et == "book":
        return not any(
            _positive(b.get("price")) for b in event.get("bids") or []
        )
    return False


def _extract_exit_price(event: dict) -> Decimal | None:
    """
    Цена, по которой мы РЕАЛЬНО смогли бы выйти из лонга — лучший бид.
    Возвращает None, если событие не несёт информации о цене — в том
    числе когда покупателей нет вовсе (пустая сторона стакана).
    """
    et = event.get("event_type")

    if et == "best_bid_ask":
        return _positive(event.get("best_bid"))

    if et == "price_change":
        best = None
        for ch in event.get("price_changes") or []:
            val = _positive(ch.get("best_bid"))
            if val is None:
                continue
            best = val if best is None else max(best, val)
        if best is not None:
            return best
        return _positive(event.get("best_bid"))

    if et == "book":
        prices = [
            p for p in (_positive(b.get("price")) for b in event.get("bids") or [])
            if p is not None
        ]
        return max(prices) if prices else None

    if et == "last_trade_price":
        return _positive(event.get("price"))

    return None


class TpSlMonitor:
    def __init__(self):
        # Ключ — ID ПОЗИЦИИ, а не token_id.
        #
        # Раньше здесь был set token_id-ов (_active_tokens), и
        # watch_position() выходил сразу, если токен уже отслеживается.
        # Это ломало сразу два сценария:
        #   1) ДВА ПОЛЬЗОВАТЕЛЯ держат позицию по одному рынку — второй
        #      не получал обработчик вообще, его TP/SL не работал;
        #   2) один пользователь делает ПЕРЕЗАХОД в тот же рынок —
        #      вторая позиция тоже оставалась без мониторинга.
        # Теперь каждая позиция получает свой обработчик, а WS-подписка
        # на токен разделяется между ними (см. ws_market: отписка
        # происходит только когда обработчиков не осталось).
        # Когда стакан рынка был очищен (для паузы стоп-лосса)
        self._book_cleared_at: dict[str, float] = {}
        self._handlers: dict[
            int, tuple[str, Callable[[dict], Awaitable[None]]]
        ] = {}

    async def start(self):
        await market_ws_manager.start()
        async with async_session() as session:
            stmt = select(Position).where(Position.status == "open")
            positions = (await session.execute(stmt)).scalars().all()
            for pos in positions:
                await self.watch_position(pos)
        asyncio.create_task(self._reconciliation_loop())

    async def watch_position(self, position: Position):
        position_id = position.id
        token_id = position.token_id

        if position_id in self._handlers:
            return

        async def handler(event: dict):
            """
            Реагируем на ВСЕ события, меняющие цену выхода, а не только
            на состоявшиеся сделки.

            Раньше слушался только last_trade_price — событие, которое
            приходит лишь когда кто-то реально торгует. Между сделками
            заявки в стакане могли уехать далеко вниз, бот этого не
            видел, и SL срабатывал уже на −70% вместо заданных −40%:
            цена перепрыгивала уровень, пока монитор молчал.

            price_change приходит при постановке/отмене ЛЮБОЙ заявки,
            best_bid_ask — при изменении лучших цен. Оба появляются
            заметно чаще сделок.

            Считаем по ЛУЧШЕМУ БИДУ: позицию мы держим в лонг, и
            продавать будем именно в бид. last_trade_price мог быть
            чужой покупкой по аску — это другая цена.
            """
            event_type = event.get("event_type")

            if event_type == "market_resolved":
                await self._handle_market_resolved(position_id, event)
                return

            # Стакан очищен — запоминаем момент и ничего не делаем.
            if _bid_side_emptied(event):
                if token_id not in self._book_cleared_at:
                    logger.info(
                        f"Стакан {token_id[:12]}... очищен (начало матча "
                        f"или закрытие рынка) — стоп-лосс ждёт "
                        f"нормальных заявок"
                    )
                self._book_cleared_at[token_id] = time.monotonic()
                return

            price = _extract_exit_price(event)
            if price is None:
                return

            # ПАУЗА после очистки стакана.
            #
            # Сразу после того, как биржа сняла все заявки (на спорте —
            # в момент начала матча), первые появившиеся заявки на
            # покупку часто грабительские: кто-то ставит 0.01, надеясь
            # поймать панику. Для стоп-лосса это выглядело бы как обвал,
            # и бот продал бы по бросовой цене. Даём рынку время
            # наполниться настоящими заявками.
            cleared = self._book_cleared_at.get(token_id)
            grace = getattr(settings, "book_clear_grace_seconds", 30)
            if cleared is not None:
                if time.monotonic() - cleared < grace:
                    return
                self._book_cleared_at.pop(token_id, None)

            await self._handle_price(position_id, price)

        self._handlers[position_id] = (token_id, handler)
        await market_ws_manager.subscribe(token_id, handler)

    async def _handle_price(self, position_id: int, current_price: Decimal):
        # Страховка: нулевая или отрицательная цена — не цена.
        if current_price is None or current_price <= 0:
            return
        # Момент, когда цена, пробившая уровень, дошла до бота
        triggered_at = time.monotonic()
        async with async_session() as session:
            pos = await session.get(Position, position_id)
            if not pos or pos.status != "open":
                return
            pnl_percent = self._calculate_pnl_percent(
                pos.entry_price, current_price
            )
            pos.current_price = current_price
            pos.pnl_percent = pnl_percent
            await session.commit()

            from services.trader import trader_service

            hit = None
            if pos.tp_price and current_price >= pos.tp_price:
                hit = "tp"
            elif pos.sl_price and current_price <= pos.sl_price:
                hit = "sl"

            if hit:
                # Замер скорости срабатывания: сколько прошло от
                # получения цены, пробившей уровень, до фактического
                # закрытия позиции. Это и есть "насколько быстро
                # отработал TP/SL" — сетевой путь ордера, а не
                # ожидание цены.
                elapsed_ms = (time.monotonic() - triggered_at) * 1000
                await trader_service.close_position(
                    pos, reason=hit, trigger_elapsed_ms=elapsed_ms,
                    trigger_price=current_price,
                )
                await self.stop_watching_position(position_id)

    async def _handle_market_resolved(self, position_id: int, event: dict):
        async with async_session() as session:
            pos = await session.get(Position, position_id)
            if not pos or pos.status != "open":
                return
            winner = event.get("winning_asset_id")
            if not winner:
                # Поле отсутствует — НЕ решаем наугад. Ложный "проигрыш"
                # так же плох, как ложный "выигрыш": пользователь увидит
                # неверный итог. Позицию оставляем открытой, её подберёт
                # сверка раз в минуту, где исход определяется по
                # фактической итоговой цене.
                logger.warning(
                    f"market_resolved без winning_asset_id для позиции "
                    f"{position_id} — исход определит сверка по цене"
                )
                return

            won = winner == pos.token_id
            from services.trader import trader_service
            await trader_service.redeem_resolved_position(pos, won=won)
            await self.stop_watching_position(position_id)

    async def stop_watching_position(self, position_id: int):
        """
        Снять мониторинг КОНКРЕТНОЙ позиции. Подписка на токен
        сохранится, если по нему есть другие позиции — другой
        пользователь или перезаход этого же пользователя.
        """
        entry = self._handlers.pop(position_id, None)
        if not entry:
            return
        token_id, handler = entry
        await market_ws_manager.unsubscribe(token_id, handler)

    def _calculate_pnl_percent(
        self, entry: Decimal, current: Decimal
    ) -> float:
        if not entry or entry == 0:
            return 0.0
        return float((current - entry) / entry * 100)

    async def _reconciliation_loop(self, interval: float = 60.0):
        """Fallback-сверка через Data API /positions?redeemable=true на случай пропуска WS-события."""
        from core.config import settings
        while True:
            await asyncio.sleep(
                settings.reconciliation_sweep_interval_seconds
            )
            try:
                await self._sweep_redeemable()
            except Exception as e:
                logger.error(f"tp_sl reconciliation error: {e}")

    async def _sweep_redeemable(self):
        from models.user import User
        async with async_session() as session:
            stmt = select(Position).where(Position.status == "open")
            open_positions = (await session.execute(stmt)).scalars().all()
            if not open_positions:
                return

            user_ids = {p.user_id for p in open_positions}
            proxy_wallets = {}
            for uid in user_ids:
                user = await session.get(User, uid)
                if user and user.proxy_wallet:
                    proxy_wallets[uid] = user.proxy_wallet

        for uid, wallet in proxy_wallets.items():
            redeemable = await polymarket_client.get_positions(
                wallet, redeemable=True
            )
            # Итоговая цена по каждому активу, а не просто факт
            # "подлежит погашению".
            #
            # Раньше здесь стояло won=True БЕЗУСЛОВНО — для всего, что
            # API вернул как redeemable. Но в этот список попадают и
            # ПРОИГРАВШИЕ позиции: их тоже нужно погасить, просто
            # выплата нулевая. В результате проигрыш объявлялся
            # выигрышем, а при низкой цене входа проценты получались
            # абсурдными: вход по 0.01 давал "+9900%", потому что
            # доли считались погашенными по 1 USDC вместо 0.
            redeemable_prices = {p.asset: p.cur_price for p in redeemable}
            user_positions = [p for p in open_positions if p.user_id == uid]

            for pos in user_positions:
                if pos.token_id not in redeemable_prices:
                    continue

                settle_price = Decimal(
                    str(redeemable_prices[pos.token_id] or 0)
                )
                # Разрешившийся рынок гасит выигравший токен около 1,
                # проигравший около 0. Порог посередине.
                won = settle_price >= Decimal("0.5")

                logger.info(
                    f"Reconciliation sweep: position {pos.id} "
                    f"погашается, итоговая цена {settle_price} -> "
                    f"{'выигрыш' if won else 'проигрыш'}"
                )
                async with async_session() as session:
                    fresh = await session.get(Position, pos.id)
                    if fresh and fresh.status == "open":
                        from services.trader import trader_service
                        await trader_service.redeem_resolved_position(
                            fresh, won=won
                        )
                        await self.stop_watching_position(fresh.id)


tp_sl_monitor = TpSlMonitor()