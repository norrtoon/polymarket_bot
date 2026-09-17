import asyncio
import time
from decimal import Decimal
from typing import Awaitable, Callable
from loguru import logger
from sqlalchemy import select

from core.database import async_session
from models.position import Position
from poly.client import polymarket_client
from poly.ws_market import market_ws_manager



def _extract_exit_price(event: dict) -> Decimal | None:
    """
    Цена, по которой мы РЕАЛЬНО смогли бы выйти из лонга — лучший бид.
    Возвращает None, если событие не несёт информации о цене.
    """
    et = event.get("event_type")

    if et == "best_bid_ask":
        bid = event.get("best_bid")
        return Decimal(str(bid)) if bid not in (None, "") else None

    if et == "price_change":
        # В price_changes каждый элемент несёт актуальные best_bid/best_ask
        best = None
        for ch in event.get("price_changes") or []:
            bid = ch.get("best_bid")
            if bid in (None, ""):
                continue
            val = Decimal(str(bid))
            best = val if best is None else max(best, val)
        if best is not None:
            return best
        bid = event.get("best_bid")
        return Decimal(str(bid)) if bid not in (None, "") else None

    if et == "book":
        bids = event.get("bids") or []
        prices = [
            Decimal(str(b.get("price")))
            for b in bids if b.get("price") not in (None, "")
        ]
        return max(prices) if prices else None

    if et == "last_trade_price":
        # Оставляем как запасной сигнал: лучше, чем ничего, если по
        # рынку не приходит книга.
        p = event.get("price")
        return Decimal(str(p)) if p not in (None, "") else None

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

            price = _extract_exit_price(event)
            if price is not None:
                await self._handle_price(position_id, price)

        self._handlers[position_id] = (token_id, handler)
        await market_ws_manager.subscribe(token_id, handler)

    async def _handle_price(self, position_id: int, current_price: Decimal):
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
            won = event.get("winning_asset_id") == pos.token_id
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
            redeemable_assets = {p.asset for p in redeemable}
            user_positions = [p for p in open_positions if p.user_id == uid]

            for pos in user_positions:
                if pos.token_id in redeemable_assets:
                    logger.info(
                        f"Reconciliation sweep: position {pos.id} "
                        f"redeemable (fallback)"
                    )
                    async with async_session() as session:
                        fresh = await session.get(Position, pos.id)
                        if fresh and fresh.status == "open":
                            from services.trader import trader_service
                            await trader_service.redeem_resolved_position(
                                fresh, won=True
                            )
                            await self.stop_watching_position(fresh.id)


tp_sl_monitor = TpSlMonitor()