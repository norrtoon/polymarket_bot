import asyncio
import time
from collections import deque


class SlidingWindowLimiter:
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        """
        Ждать освобождения слота.

        Раньше asyncio.sleep() выполнялся ВНУТРИ `async with self._lock`.
        Пока один вызов спал в ожидании слота, лок был занят, и все
        остальные запросы стояли в очереди — даже если свободные слоты
        уже появились. При копировании сделки это означало, что
        критический путь мог ждать не из-за лимита, а из-за того, что
        кто-то другой держит лок.

        Теперь спим ВНЕ лока и после пробуждения перепроверяем окно.
        """
        while True:
            async with self._lock:
                now = time.monotonic()
                while self._timestamps and \
                        now - self._timestamps[0] > self.window_seconds:
                    self._timestamps.popleft()

                if len(self._timestamps) < self.max_requests:
                    self._timestamps.append(now)
                    return

                # Слотов нет — считаем, сколько ждать, и отпускаем лок
                wait = self.window_seconds - (now - self._timestamps[0])

            await asyncio.sleep(max(wait, 0.001))


# Лимиты с запасом от официальных (см. Rate Limits doc). Значение для
# /trades было снижено со 150 до 90 после того, как 150/10с всё равно
# приводило к 429 на практике — либо из-за нескольких watcher-циклов
# на общий IP, либо burst-лимит Cloudflare строже заявленного
# sustained-лимита. При повторных 429 см. RateLimited в poly/client.py.
# ОБЩИЙ лимитер на весь хост data-api.polymarket.com.
#
# Cloudflare считает лимит ПО IP НА ХОСТ, а не по каждому пути
# отдельно. Раньше /trades, /positions и /value имели независимые
# счётчики (90 + 110 + вообще никакого у /value) — они не знали друг о
# друге, и суммарная нагрузка на хост могла втрое превышать то, что
# видел каждый лимитер по отдельности. Отсюда 429 даже при скромном
# интервале опроса. Теперь любой запрос к data-api сначала проходит
# через общий лимитер, и только потом через свой частный.
data_api_global_limiter = SlidingWindowLimiter(max_requests=55, window_seconds=10)

data_api_trades_limiter = SlidingWindowLimiter(max_requests=90, window_seconds=10)
data_api_positions_limiter = SlidingWindowLimiter(max_requests=110, window_seconds=10)
clob_midpoint_limiter = SlidingWindowLimiter(max_requests=1200, window_seconds=10)
clob_order_limiter = SlidingWindowLimiter(max_requests=35, window_seconds=1)
relayer_submit_limiter = SlidingWindowLimiter(max_requests=20, window_seconds=60)