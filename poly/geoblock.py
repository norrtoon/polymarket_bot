import aiohttp
from dataclasses import dataclass
from loguru import logger


@dataclass
class GeoblockStatus:
    blocked: bool
    ip: str
    country: str
    region: str
    # Проверка не дала однозначного ответа (сеть, не-JSON, таймаут).
    # Отличать это от "разрешено" критично: раньше check() при любой
    # ошибке возвращал None, вызывающий код видел "не заблокировано" и
    # разрешал торговлю. То есть защита fail-closed не работала вообще.
    unknown: bool = False


class GeoblockChecker:
    URL = "https://polymarket.com/api/geoblock"
    # Без браузерного User-Agent Cloudflare отдаёт HTML-заглушку вместо
    # JSON — отсюда ошибка вида "0, message=''" при разборе ответа.
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
    }

    async def check(self) -> GeoblockStatus:
        try:
            timeout = aiohttp.ClientTimeout(total=8)
            # max_field_size: у aiohttp жёсткий лимит 8190 байт на один
            # HTTP-заголовок. Cloudflare перед polymarket.com отдаёт
            # заголовок длиннее (наблюдалось 8215 байт — вероятно,
            # большой Set-Cookie с токенами бот-защиты), парсер падает
            # с LineTooLong, и это всплывало как
            # "ClientResponseError: 0, message=''" — то есть проверка
            # региона не проходила НИКОГДА, хотя сервер отвечал
            # корректным JSON.
            async with aiohttp.ClientSession(
                timeout=timeout,
                max_field_size=65536,
                max_line_size=65536,
            ) as s:
                async with s.get(self.URL, headers=self.HEADERS) as resp:
                    if resp.status != 200:
                        # Показываем начало тела: по нему сразу видно,
                        # это челлендж Cloudflare, страница ошибки или
                        # что-то ещё.
                        body = (await resp.text())[:200].replace("\n", " ")
                        logger.warning(
                            f"geoblock: HTTP {resp.status}, "
                            f"content-type={resp.headers.get('content-type')}, "
                            f"тело: {body!r}"
                        )
                        return GeoblockStatus(
                            blocked=False, ip="", country="?",
                            region="", unknown=True,
                        )
                    # content_type=None: не падаем, если сервер отдал
                    # JSON с неожиданным mimetype
                    data = await resp.json(content_type=None)

            if not isinstance(data, dict) or "blocked" not in data:
                logger.warning(
                    "geoblock: ответ не содержит поля blocked — "
                    "статус региона неизвестен"
                )
                return GeoblockStatus(
                    blocked=False, ip="", country="?",
                    region="", unknown=True,
                )

            return GeoblockStatus(
                blocked=bool(data.get("blocked", True)),
                ip=data.get("ip", ""),
                country=data.get("country", "?"),
                region=data.get("region", ""),
            )
        except Exception as e:
            # Полный тип исключения важен: ClientConnectorError — не
            # достучались (сеть/DNS/IPv6), ContentTypeError — ответ не
            # JSON (скорее всего челлендж Cloudflare), TimeoutError —
            # молчит.
            logger.error(
                f"geoblock check failed: {type(e).__name__}: {e!r} — "
                f"статус региона неизвестен"
            )
            return GeoblockStatus(
                blocked=False, ip="", country="?", region="", unknown=True,
            )


geoblock_checker = GeoblockChecker()