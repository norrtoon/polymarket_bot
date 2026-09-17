import asyncio
import json
import websockets
from loguru import logger

from core.config import settings


class UserWebSocketManager:
    """Авторизованный канал подтверждения исполнения своих ордеров/сделок."""

    def __init__(self, api_key: str, secret: str, passphrase: str):
        self._auth = {"apiKey": api_key, "secret": secret, "passphrase": passphrase}
        self._ws = None
        self._connected = asyncio.Event()
        self._waiters: dict[str, asyncio.Future] = {}

    async def start(self):
        asyncio.create_task(self._run())

    async def _run(self):
        while True:
            try:
                async with websockets.connect(settings.user_ws_url, ping_interval=None) as ws:
                    self._ws = ws
                    await ws.send(json.dumps({"auth": self._auth, "type": "user"}))
                    self._connected.set()
                    logger.info("User WS connected & authenticated")

                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        async for raw in ws:
                            await self._dispatch(raw)
                    finally:
                        ping_task.cancel()
            except Exception as e:
                logger.error(f"User WS error, reconnect in 2s: {e}")
                self._connected.clear()
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
            event_type = event.get("event_type")
            order_id = event.get("taker_order_id") if event_type == "trade" else event.get("id")
            fut = self._waiters.get(order_id)
            if not fut or fut.done():
                continue
            if event_type == "trade" and event.get("status") in ("MATCHED", "MINED", "CONFIRMED"):
                fut.set_result(event)
            elif event_type == "order" and event.get("status") == "MATCHED":
                fut.set_result(event)

    async def wait_for_fill(self, order_id: str, timeout: float = 5.0) -> dict | None:
        await self._connected.wait()
        fut = asyncio.get_event_loop().create_future()
        self._waiters[order_id] = fut
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"User WS fill timeout order_id={order_id}")
            return None
        finally:
            self._waiters.pop(order_id, None)


user_ws_manager: "UserWebSocketManager | None" = None