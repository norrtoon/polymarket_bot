import time
from decimal import Decimal
from loguru import logger
from sqlalchemy import select

from core.database import async_session
from core.config import settings
from models.position import Position
from models.trade_log import TradeLog
from poly.relayer_client import relayer_client
from poly.client import polymarket_client
from poly.clob_auth import build_builder_headers


class ReconciliationService:

    async def run_on_startup(self):
        if settings.simulation_mode:
            logger.info("Reconciliation skipped (simulation mode)")
            return

        # Стартовая сверка работает с ГЛОБАЛЬНЫМ кошельком из .env.
        # После перехода на ключи каждого пользователя эти настройки
        # обычно пустые — и запрос уходил с невалидной подписью,
        # получая 401 (видно в логе как get_own_trades status=401).
        # Без кред сверять нечего, поэтому просто пропускаем.
        if not (settings.my_proxy_wallet_address and settings.clob_api_key):
            logger.info(
                "Reconciliation пропущена: глобальные CLOB-креды не "
                "заданы (бот работает на ключах пользователей). "
                "Сверка позиций идёт через tp_sl_monitor."
            )
            return

        logger.info("Running startup reconciliation...")

        if settings.builder_api_key:
            builder_headers = build_builder_headers(
                api_key=settings.builder_api_key, secret=settings.builder_api_secret,
                passphrase=settings.builder_api_passphrase, method="GET", path="/transactions",
            )
            transactions = await relayer_client.get_recent_transactions(builder_headers)
            pending = [tx for tx in transactions if tx.get("state") in ("STATE_NEW", "STATE_EXECUTED", "STATE_MINED")]
            if pending:
                logger.warning(f"Reconciliation: {len(pending)} pending relayer tx found")
                async with async_session() as session:
                    for tx in pending:
                        resolved = await relayer_client.poll_transaction(tx["transactionID"], timeout=30.0)
                        if resolved:
                            await self._apply_resolved_tx(session, resolved)
                    await session.commit()

        one_hour_ago = int(time.time()) - 3600
        own_trades = await polymarket_client.get_own_trades(
            maker_address=settings.my_proxy_wallet_address, after=one_hour_ago,
        )
        async with async_session() as session:
            for trade in own_trades:
                if trade.get("status") != "TRADE_STATUS_CONFIRMED":
                    continue
                tx_hash = trade.get("transaction_hash")
                stmt = select(Position).where(Position.tx_hash_ours == tx_hash)
                pos = (await session.execute(stmt)).scalar_one_or_none()
                if pos and pos.entry_price is None:
                    pos.entry_price = Decimal(trade["price"])
                    pos.shares_bought = Decimal(trade["size"]) / Decimal("1e6")
                    logger.info(f"Reconciliation: filled position {pos.id} from /data/trades")
            await session.commit()

        logger.info("Reconciliation complete")

    async def _apply_resolved_tx(self, session, tx: dict):
        if tx.get("state") != "STATE_CONFIRMED":
            logger.warning(f"Reconciliation: tx {tx.get('transactionHash')} state={tx.get('state')}")
            return
        tx_hash = tx.get("transactionHash")
        stmt = select(Position).where(Position.tx_hash_ours == tx_hash)
        pos = (await session.execute(stmt)).scalar_one_or_none()
        if pos and pos.status == "open":
            pos.status = "closed"
            session.add(TradeLog(
                user_id=pos.user_id, position_id=pos.id,
                action="reconciled_closed", details={"tx_hash": tx_hash},
            ))
            logger.info(f"Reconciliation: position {pos.id} marked closed via tx {tx_hash}")


reconciliation_service = ReconciliationService()