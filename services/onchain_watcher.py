"""
Отслеживание сделок напрямую в блокчейне через WebSocket RPC (Alchemy).

ЗАЧЕМ. Раньше бот узнавал о сделке трейдера, опрашивая Data API
Polymarket. Data API индексирует сделки с задержкой — на практике от
десятков секунд до полутора минут. На быстрых рынках (BTC Up/Down на
5 минут) за это время цена уходит так далеко, что копия почти
гарантированно убыточна.

Здесь бот подписывается на само событие OrderFilled в контрактах
биржи и видит сделку в момент её записи в блок (Polygon, ~2 секунды).

РЕЖИМЫ (ONCHAIN_MODE):
  shadow — только наблюдение: расшифровывает сделки и пишет в лог, во
           сколько раз быстрее цепь, чем Data API. Ничего не копирует.
           По умолчанию — чтобы проверить расшифровку, не рискуя деньгами.
  live   — копирует по событиям из цепи. Опрос Data API продолжает
           работать параллельно как страховка: если WebSocket отвалится,
           сделка всё равно придёт с него. Дубли отсекаются по хэшу.
  off    — выключено.
"""
import asyncio
import json
import time
from decimal import Decimal

import websockets
from loguru import logger

from core.config import settings
from poly.schemas_local import Trade

# Хэш события OrderFilled контрактов биржи V2.
#
# Проверен двумя способами: совпадает с topic0 в расшифрованных логах
# реальных транзакций на Polygonscan и с keccak256 от сигнатуры
#   OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,
#               uint256,bytes32,bytes32)
# ВНИМАНИЕ: в ряде статей приводится 8-польная сигнатура с uint256 side —
# это хэш СТАРОГО контракта V1, с ним подписка ничего бы не получала.
ORDER_FILLED_TOPIC = (
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
)

# Контракты биржи V2 (переход 28.04.2026). Сверено по нескольким
# независимым источникам, включая Polygonscan.
EXCHANGE_CONTRACTS = [
    "0xE111180000d2663C0091e4f400237545B87B996B",  # CTF Exchange V2
    "0xe2222d279d744050d28e00520010520000310F59",  # NegRisk Exchange V2
    "0xe2222d002000ba0053cef3375333610f64600036",  # NegRisk Exchange V2 (b)
]

UNIT = Decimal(10) ** 6   # pUSD и доли — 6 знаков после запятой


def _addr_topic(address: str) -> str:
    """Адрес в формате индексированного топика (32 байта)."""
    return "0x" + "0" * 24 + address.lower().replace("0x", "")


def _topic_addr(topic: str) -> str:
    """Обратно: из топика — адрес."""
    return "0x" + topic[-40:].lower()


def decode_order_filled(log: dict, watched: str) -> Trade | None:
    """
    Расшифровать OrderFilled С ТОЧКИ ЗРЕНИЯ отслеживаемого кошелька.

    Поле side в событии — это сторона МЕЙКЕРА (0 = BUY, 1 = SELL).
      * кошелёк — мейкер  -> его сторона = side
      * кошелёк — тейкер  -> его сторона ПРОТИВОПОЛОЖНАЯ
    Количества тоже зависят от стороны мейкера:
      * мейкер BUY:  makerAmount = pUSD,  takerAmount = доли
      * мейкер SELL: makerAmount = доли,  takerAmount = pUSD

    Ошибка в любом из этих правил означала бы, что бот покупает, когда
    трейдер продаёт. Поэтому они покрыты тестами (tests/test_onchain.py).
    """
    if log.get("removed"):
        return None     # блок откатился при реорганизации цепи

    topics = log.get("topics") or []
    if len(topics) != 4 or topics[0].lower() != ORDER_FILLED_TOPIC:
        return None

    maker = _topic_addr(topics[2])
    taker = _topic_addr(topics[3])
    watched = watched.lower()

    if watched == maker:
        is_maker = True
    elif watched == taker:
        is_maker = False
    else:
        return None

    data = (log.get("data") or "0x")[2:]
    words = [data[i:i + 64] for i in range(0, len(data), 64)]
    if len(words) < 4:
        return None

    side_raw = int(words[0], 16)
    token_id = str(int(words[1], 16))
    maker_amount = Decimal(int(words[2], 16)) / UNIT
    taker_amount = Decimal(int(words[3], 16)) / UNIT

    if side_raw not in (0, 1):
        return None
    maker_side = "BUY" if side_raw == 0 else "SELL"
    opposite = "SELL" if maker_side == "BUY" else "BUY"
    side = maker_side if is_maker else opposite

    if maker_side == "BUY":
        usdc, shares = maker_amount, taker_amount
    else:
        usdc, shares = taker_amount, maker_amount

    if shares <= 0 or usdc <= 0:
        return None
    price = usdc / shares
    if not (Decimal("0") < price < Decimal("1.0001")):
        # Цена доли на Polymarket всегда в диапазоне 0..1. Иное значит,
        # что расшифровка разошлась с реальностью — лучше пропустить.
        logger.warning(
            f"onchain: невозможная цена {price} в {log.get('transactionHash')}"
            f" — событие пропущено"
        )
        return None

    # Время блока берём по часам биржи: блок только что записан, разница
    # с реальной меткой блока — единицы секунд. Курсор копирования эти
    # сделки НЕ сдвигают (см. ingest_onchain), так что точной метки не нужно.
    try:
        from poly.client import polymarket_client
        now_ts = int(polymarket_client.polymarket_now())
    except Exception:
        now_ts = int(time.time())

    return Trade(
        tx_hash=log.get("transactionHash", ""),
        market_id="",          # в событии нет — трейдер возьмёт из стакана
        outcome_id="",
        token_id=token_id,
        side=side,
        price=price,
        size=shares,
        usdc_amount=usdc,
        timestamp=now_ts,
    )


class OnChainWatcher:
    def __init__(self):
        self._wallets: set[str] = set()
        self._task: asyncio.Task | None = None
        self._ws = None
        self._resubscribe = asyncio.Event()
        # когда цепь впервые показала сделку — для сравнения с Data API
        self.first_seen: dict[str, float] = {}

    @property
    def mode(self) -> str:
        if not getattr(settings, "alchemy_ws_url", ""):
            return "off"
        return (getattr(settings, "onchain_mode", "shadow") or "shadow").lower()

    def watch(self, wallet: str):
        wallet = wallet.lower()
        if wallet not in self._wallets:
            self._wallets.add(wallet)
            self._resubscribe.set()

    def unwatch(self, wallet: str):
        wallet = wallet.lower()
        if wallet in self._wallets:
            self._wallets.discard(wallet)
            self._resubscribe.set()

    async def start(self):
        if self.mode == "off":
            logger.info("Отслеживание по блокчейну выключено (нет ALCHEMY_WS_URL)")
            return
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())
            logger.info(f"Отслеживание по блокчейну запущено, режим: {self.mode}")

    async def _run(self):
        delay = 1
        while True:
            if not self._wallets:
                self._resubscribe.clear()
                await self._resubscribe.wait()
                continue
            try:
                async with websockets.connect(
                    settings.alchemy_ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=2 ** 22,
                ) as ws:
                    self._ws = ws
                    await self._subscribe_all(ws)
                    delay = 1
                    logger.info(
                        f"onchain: подключено, кошельков: {len(self._wallets)}"
                    )
                    await self._listen(ws)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    f"onchain: соединение потеряно ({type(e).__name__}: {e}), "
                    f"переподключение через {delay}с. Пока цепь недоступна, "
                    f"сделки продолжают приходить через Data API."
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
            finally:
                self._ws = None

    async def _subscribe_all(self, ws):
        """
        На каждый кошелёк — одна подписка: события, где он мейкер.
        """
        self._resubscribe.clear()
        req_id = 1
        for wallet in sorted(self._wallets):
            t = _addr_topic(wallet)
            # ТОЛЬКО события, где кошелёк — МЕЙКЕР.
            #
            # Каждый исполненный ордер трейдера даёт событие, где мейкер
            # — он сам (для рыночного ордера тейкером контракт пишет
            # себя). События, где трейдер указан ТЕЙКЕРОМ, — это чужие
            # ордера, и сторона с токеном в них описывают контрагента.
            #
            # Раньше подписка была и на тейкерские события, и правило
            # "тейкер против покупателя = продажа" врало, когда биржа
            # сводила ордера на ПРОТИВОПОЛОЖНЫЕ исходы через чеканку или
            # сжигание комплекта (цены в сумме дают ровно 1.00):
            #   * трейдер купил Up  -> бот видел ещё "продал Down"
            #   * трейдер продал Up -> бот видел ещё "КУПИЛ Down"
            # Второе особенно опасно: покупка противоположного исхода.
            for topics in (
                [ORDER_FILLED_TOPIC, None, t],          # кошелёк — мейкер
            ):
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "id": req_id,
                    "method": "eth_subscribe",
                    "params": ["logs", {
                        "address": EXCHANGE_CONTRACTS,
                        "topics": topics,
                    }],
                }))
                req_id += 1

    async def _listen(self, ws):
        while True:
            if self._resubscribe.is_set():
                # набор кошельков изменился — переподключаемся с новым
                return
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                continue

            msg = json.loads(raw)
            if msg.get("method") != "eth_subscription":
                if "error" in msg:
                    logger.error(f"onchain: ошибка подписки: {msg['error']}")
                continue

            log = (msg.get("params") or {}).get("result") or {}
            await self._handle_log(log)

    async def _handle_log(self, log: dict):
        topics = log.get("topics") or []
        if len(topics) < 4:
            return
        maker = _topic_addr(topics[2])

        # Только мейкер — см. пояснение в _subscribe_all.
        for wallet in (maker,):
            if wallet not in self._wallets:
                continue
            trade = decode_order_filled(log, wallet)
            if trade is None:
                continue

            self.first_seen.setdefault(trade.tx_hash, time.monotonic())
            if len(self.first_seen) > 5000:
                for k in list(self.first_seen)[:2500]:
                    self.first_seen.pop(k, None)

            role = "свой ордер"
            logger.info(
                f"ЦЕПЬ [{self.mode}]: {wallet[:10]}... {trade.side} "
                f"{trade.size:.2f} долей по {trade.price:.4f} "
                f"({role}) tx={trade.tx_hash[:14]}..."
            )

            if self.mode == "live":
                from services.watcher import wallet_watcher
                await wallet_watcher.ingest_onchain(wallet, [trade])


onchain_watcher = OnChainWatcher()