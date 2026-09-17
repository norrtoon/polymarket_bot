import asyncio
import aiohttp
from loguru import logger

from core.config import settings
from poly.rate_limiter import relayer_submit_limiter


class RelayerClient:
    def __init__(self):
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        return self._session

    async def get_nonce(self, address: str, nonce_type: str) -> int:
        session = await self._get_session()
        async with session.get(
            f"{settings.relayer_base_url}/nonce",
            params={"address": address, "type": nonce_type},
        ) as resp:
            data = await resp.json()
            if resp.status != 200:
                raise RuntimeError(f"get_nonce error: {data}")
            return int(data["nonce"])

    async def submit_transaction(self, payload: dict, auth_headers: dict) -> dict:
        await relayer_submit_limiter.acquire()
        session = await self._get_session()
        async with session.post(
            f"{settings.relayer_base_url}/submit", json=payload, headers=auth_headers
        ) as resp:
            data = await resp.json()
            if resp.status != 200:
                logger.error(f"relayer submit failed: {data}")
            return data

    async def poll_transaction(self, tx_id: str, timeout: float = 30.0, interval: float = 1.0) -> dict | None:
        session = await self._get_session()
        elapsed = 0.0
        while elapsed < timeout:
            async with session.get(
                f"{settings.relayer_base_url}/transaction", params={"id": tx_id}
            ) as resp:
                if resp.status == 200:
                    items = await resp.json()
                    if items:
                        tx = items[0]
                        state = tx.get("state")
                        if state in ("STATE_CONFIRMED", "STATE_MINED"):
                            return tx
                        if state in ("STATE_INVALID", "STATE_FAILED"):
                            logger.error(f"relayer tx failed: {tx}")
                            return tx
            await asyncio.sleep(interval)
            elapsed += interval
        logger.warning(f"relayer poll timeout tx_id={tx_id}")
        return None

    async def get_recent_transactions(self, auth_headers: dict) -> list[dict]:
        session = await self._get_session()
        async with session.get(
            f"{settings.relayer_base_url}/transactions", headers=auth_headers
        ) as resp:
            if resp.status != 200:
                logger.error(f"get_recent_transactions failed: {resp.status}")
                return []
            return await resp.json()

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


relayer_client = RelayerClient()