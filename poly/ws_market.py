# poly/ws_market.py
import asyncio
import json
from decimal import Decimal
from typing import Callable, Awaitable
import websockets
from loguru import logger

from core.config import settings

# Официального жёсткого лимита Polymarket не публикует, но по опыту
# разработчиков (issue Polymarket/py-clob-client #292, адаптер
# NautilusTrader) большие пачки assets_ids в одном сообщении на
# практике приводят к обрыву соединения или "тихому" зависанию потока
# данных. Дробим переподписку при реконнекте на пачки такого размера.
SUBSCRIBE_CHUNK_SIZE = 100


class MarketWebSocketManager:
    """
    Публичный Market Channel — realtime цены, orderbook, market_resolved.
    Одно соединение на весь процесс, динамическая подписка per token_id.
    """

    def __init__(self):
        self._ws = None
        self._subscribed: set[str] = set()
        self._handlers: dict[str, list[Callable[[dict], Awaitable[None]]]] = {}
        self._lock = asyncio.Lock()
        self._connected = asyncio.Event()
        self._initial_sent = False
        self._started = False
        # Взводится, как только появляется хотя бы один токен для
        # подписки. Пока событие не взведено, _run НЕ открывает
        # соединение — см. комментарий в _run.
        self._has_tokens = asyncio.Event()
        # Кэш последней известной цены по token_id — чтобы не делать
        # REST/SDK round-trip на каждый place_market_order
        self._last_price: dict[str, "Decimal"] = {}

    def get_cached_price(self, token_id: str):
        """Последняя цена из WS-фида, если по этому токену уже есть
        подписка (обычно — уже открытая позиция по нему). Для
        совершенно новых токенов вернёт None — тогда клиент идёт
        в REST/SDK как раньше."""
        return self._last_price.get(token_id)

    async def start(self):
        if self._started:
            return
        self._started = True
        asyncio.create_task(self._run())
        logger.info("Market WS task запущен")

    async def _run(self):
        while True:
            # НЕ подключаемся, пока подписываться не на что.
            #
            # Polymarket требует прислать корректное сообщение подписки
            # СРАЗУ после коннекта. Раньше при пустом списке токенов бот
            # всё равно открывал соединение и ничего не слал — а через
            # 10 секунд _ping_loop отправлял "PING", который становился
            # ПЕРВЫМ сообщением в соединении. Сервер закрывал его с
            # "1008 invalid subscription payload", бот переподключался,
            # и цикл повторялся каждые ~12 секунд бесконечно. В логах
            # это видно как ровно 10-секундный интервал между "Market WS
            # connected" и ошибкой 1008.
            if not self._subscribed:
                self._has_tokens.clear()
                logger.info(
                    "Market WS: подписок пока нет — соединение не "
                    "открываем, ждём первый subscribe()"
                )
                await self._has_tokens.wait()

            try:
                async with websockets.connect(
                    settings.market_ws_url,
                    ping_interval=None
                ) as ws:
                    self._ws = ws
                    self._initial_sent = False
                    self._connected.set()
                    logger.info("Market WS connected")

                    # Отправляем подписку только если есть токены.
                    # ДРОБИМ на пачки — раньше весь накопленный за
                    # сессию список уходил ОДНИМ сообщением, что при
                    # достаточно большом наборе токенов приводило к
                    # разрыву соединения с "invalid subscription
                    # payload" (1008).
                    if self._subscribed:
                        tokens = list(self._subscribed)
                        for i in range(0, len(tokens), SUBSCRIBE_CHUNK_SIZE):
                            chunk = tokens[i:i + SUBSCRIBE_CHUNK_SIZE]
                            await self._send_subscribe(chunk)
                        logger.info(
                            f"Market WS: переподписка на "
                            f"{len(tokens)} токенов ("
                            f"{(len(tokens) - 1) // SUBSCRIBE_CHUNK_SIZE + 1} "
                            f"пачками)"
                        )


                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        async for raw in ws:
                            await self._dispatch(raw)
                    finally:
                        ping_task.cancel()

            except Exception as e:
                logger.error(f"Market WS error, reconnect in 2s: {e}")
                self._connected.clear()
                self._ws = None
                self._initial_sent = False  # сбрасываем при реконнекте
                await asyncio.sleep(2)

    async def _ping_loop(self, ws):
        while True:
            await asyncio.sleep(10)
            try:
                await ws.send("PING")
            except Exception:
                return

    async def _dispatch(self, raw: str):
        if raw == "PONG":
            return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        events = data if isinstance(data, list) else [data]
        for event in events:
            asset_id = event.get("asset_id")
            if not asset_id:
                continue

            price_raw = event.get("price")
            if price_raw is not None:
                try:
                    self._last_price[asset_id] = Decimal(str(price_raw))
                except Exception:
                    pass

            for h in list(self._handlers.get(asset_id, [])):
                try:
                    await h(event)
                except Exception as e:
                    logger.error(f"market ws handler error: {e}")

    async def subscribe(
        self,
        token_id: str,
        handler: Callable[[dict], Awaitable[None]]
    ):
        """
        Подписаться на токен. Несколько обработчиков на один токен —
        нормальная ситуация: например, два РАЗНЫХ пользователя держат
        позицию по одному и тому же рынку. Само WS-сообщение подписки
        отправляется только при первом обработчике.
        """
        if not self._started:
            await self.start()

        async with self._lock:
            self._handlers.setdefault(token_id, []).append(handler)
            is_new = token_id not in self._subscribed
            if is_new:
                self._subscribed.add(token_id)
            # Уже подключены прямо сейчас? Тогда досылаем подписку
            # инкрементально. Если нет — токен уже лежит в
            # self._subscribed и уйдёт в стартовой пачке, когда _run
            # установит соединение. Так исключается и дубль подписки,
            # и гонка между subscribe() и переподключением.
            connected_now = (
                self._ws is not None and self._connected.is_set()
            )

        # Будим _run, если он ждёт первых токенов
        self._has_tokens.set()

        if not is_new:
            return

        if connected_now:
            async with self._lock:
                await self._send_subscribe([token_id])
        logger.info(
            f"Subscribed to token: {token_id[:16]}... "
            f"(обработчиков: {len(self._handlers.get(token_id, []))})"
        )

    async def unsubscribe(
        self,
        token_id: str,
        handler: Callable[[dict], Awaitable[None]] | None = None
    ):
        """
        Снять ОДИН обработчик. Реальная отписка от токена происходит
        только когда обработчиков не осталось ни одного.

        Раньше этот метод делал self._handlers.pop(token_id) и слал
        unsubscribe безусловно — то есть закрытие позиции ОДНИМ
        пользователем убивало поток цен для ВСЕХ остальных, кто держит
        позицию по тому же рынку. При двух и более пользователях это
        ломало TP/SL у всех, кроме того, кто закрылся последним.
        """
        async with self._lock:
            handlers = self._handlers.get(token_id, [])
            if handler is not None:
                self._handlers[token_id] = [
                    h for h in handlers if h is not handler
                ]
            else:
                # Совместимость со старыми вызовами без handler —
                # снимаем всё (используется только при полной остановке)
                self._handlers[token_id] = []

            remaining = len(self._handlers.get(token_id, []))
            if remaining == 0:
                self._handlers.pop(token_id, None)
                if token_id in self._subscribed:
                    self._subscribed.discard(token_id)
                    await self._send_unsubscribe([token_id])
                    logger.info(
                        f"Unsubscribed from token: {token_id[:16]}... "
                        f"(обработчиков не осталось)"
                    )
            else:
                logger.debug(
                    f"token {token_id[:16]}...: обработчик снят, "
                    f"осталось {remaining} — подписку держим"
                )

    async def _send_subscribe(self, token_ids: list[str]):
        if self._ws is None:
            return

        # Дедуп + защита от пустого списка. Дубликаты в assets_ids
        # сами по себе не должны ничего ломать по протоколу, но раз уж
        # мы разбираемся с "invalid subscription payload" — лишняя
        # причина для отказа сервера нам не нужна.
        token_ids = list(dict.fromkeys(t for t in token_ids if t))
        if not token_ids:
            logger.debug("_send_subscribe: пустой список, пропускаем")
            return

        if not self._initial_sent:
            payload = {
                "assets_ids": token_ids,
                "type": "market",
                "custom_feature_enabled": True
            }
            self._initial_sent = True
        else:
            # custom_feature_enabled нужен для market_resolved и
            # best_bid_ask. Раньше передавался только в самом первом
            # сообщении за коннект — то есть для подавляющего
            # большинства токенов (подписанных вторым, третьим и т.д.
            # вызовом subscribe()) эти события могли не приходить.
            payload = {
                "operation": "subscribe",
                "assets_ids": token_ids,
                "custom_feature_enabled": True
            }

        logger.debug(f"WS → {json.dumps(payload)[:100]}")
        await self._ws.send(json.dumps(payload))

    async def _send_unsubscribe(self, token_ids: list[str]):
        if self._ws is None:
            return
        if not token_ids:
            return
        await self._ws.send(
            json.dumps({
                "operation": "unsubscribe",
                "assets_ids": token_ids
            })
        )


market_ws_manager = MarketWebSocketManager()