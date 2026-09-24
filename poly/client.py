import asyncio
import time
import aiohttp
from decimal import Decimal
from loguru import logger

from core.config import settings
from poly.schemas_local import Trade, OrderResult, PositionInfo
from poly.clob_auth import build_l2_headers
from poly.rate_limiter import (
    data_api_global_limiter, data_api_trades_limiter,
    data_api_positions_limiter, clob_order_limiter,
)
from poly.ws_market import market_ws_manager

try:
    # OrderSide/OrderType в SDK — это Literal-алиасы, а не enum,
    # поэтому импортируем только клиентов и передаём строки.
    from polymarket import AsyncPublicClient, AsyncSecureClient
    from polymarket import RateLimitError, UserInputError, PolymarketError, ApiKeyCreds
    SDK_AVAILABLE = True
except ImportError:
    SDK_AVAILABLE = False
    logger.warning("polymarket-client SDK не установлен — доступен только SIMULATION_MODE")


class RateLimited(Exception):
    """Data API вернул 429 и повторные попытки не помогли — вызывающий
    код должен сам решить, как долго ждать перед следующим опросом."""
    pass


# 28 апреля 2026 Polymarket перевёл всю торговлю с USDC.e на новый
# залоговый токен pUSD (Polymarket USD) — см. docs.polymarket.com/
# concepts/pusd. Обычный ERC-20 на Polygon, 6 знаков после запятой,
# обеспечен USDC 1:1. Именно в нём теперь лежат "живые" деньги на
# proxy/Safe кошельке, а не в USDC.e.
PUSD_CONTRACT_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
# Conditional Tokens (ERC-1155): здесь хранятся доли всех рынков.
# Адрес по официальной документации, при переходе на V2 не менялся.
CTF_CONTRACT_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
PUSD_DECIMALS = 6

# Резервные публичные RPC Polygon. Штатный polygon-rpc.com на практике
# отвечает "API key disabled / tenant disabled" (403) — публичный доступ
# по нему закрыт. Перебираем по очереди, пока какой-нибудь не ответит.
# Свой приватный эндпоинт (Alchemy/Infura/Chainstack) можно задать
# через POLYGON_RPC_URL — он всегда пробуется первым.
# Контракт Conditional Tokens: здесь хранятся доли (ERC1155).
# Сверено по официальной документации Polymarket: при переходе на V2
# адрес не менялся.
CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
# keccak256("balanceOf(address,uint256)")[:4] — стандарт ERC1155
ERC1155_BALANCE_OF = "00fdd58e"

# Контракты биржи V2 — для понятного отчёта о выданных разрешениях.
# Адреса сверены по нескольким источникам, включая Polygonscan.
_SPENDER_NAMES = {
    "0xe111180000d2663c0091e4f400237545b87b996b": "обычные рынки",
    "0xe2222d279d744050d28e00520010520000310f59":
        "рынки с несколькими исходами (NegRisk)",
    "0xe2222d002000ba0053cef3375333610f64600036":
        "рынки с несколькими исходами (NegRisk)",
}

FALLBACK_POLYGON_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.llamarpc.com",
    "https://1rpc.io/matic",
    "https://rpc.ankr.com/polygon",
]


ATOMIC_SCALE = Decimal("1000000")   # 6 знаков, как у USDC


def _normalize_amounts(making: Decimal, taking: Decimal) -> tuple:
    """
    Привести суммы из ответа биржи к обычным единицам.

    Возвращает (making, taking) как есть, если они уже нормальные, или
    делит обе на 1e6, если биржа отдала атомарные единицы.
    """
    if making <= 0 or taking <= 0:
        return making, taking

    # Цена = меньшая величина / большая. На Polymarket она всегда 0..1,
    # поэтому долей всегда БОЛЬШЕ, чем потраченных USDC (кроме цены 1.0).
    ratio = max(making, taking) / min(making, taking)

    # Отношение больше 1000 невозможно: минимальная цена доли 0.001.
    # Значит перед нами атомарные единицы... но только если ПОСЛЕ
    # деления величины становятся правдоподобными.
    if max(making, taking) >= ATOMIC_SCALE and ratio <= 1000:
        scaled_making = making / ATOMIC_SCALE
        scaled_taking = taking / ATOMIC_SCALE
        logger.info(
            f"Ответ биржи в атомарных единицах "
            f"({making}/{taking}) -> "
            f"{scaled_making}/{scaled_taking}"
        )
        return scaled_making, scaled_taking

    return making, taking


def _require_side(item: dict) -> str:
    """Сторона сделки строго из ответа API, без догадок."""
    raw = (item.get("side") or "").strip().upper()
    if raw in ("BUY", "SELL"):
        return raw
    raise ValueError(
        f"в ответе Data API нет корректного поля side: {item.get('side')!r}"
    )


class PolymarketClient:
    """
    Гибридная интеграция:
      - Watcher чужих кошельков -> прямой REST к Data API
      - Торговля и собственные данные -> unified SDK (polymarket-client).
      - Реалтайм цены/resolved -> Market WS.
      - Подтверждение своих сделок -> User WS + REST /data/trades fallback.
    """

    def __init__(self):
        self._public = None
        self._secure = None
        self._http: aiohttp.ClientSession | None = None
        # Первый RPC, который реально ответил — чтобы не перебирать
        # весь список на каждом запросе баланса
        self._working_rpc: str | None = None
        # SecureClient на каждого пользователя (свои ключи у каждого)
        self._user_clients: dict = {}
        # Блокировки создания клиента, по одной на пользователя
        self._client_locks: dict = {}
        self._keepalive_task: asyncio.Task | None = None
        self._warm_failed_logged = False
        # Смещение наших часов относительно Polymarket (секунды)
        self.time_offset: float = 0.0
        self._time_synced = False
        # Счётчик полученных 429. Вотчер читает дельту и по ней
        # подстраивает интервал опроса под реальный лимит IP.
        self.rate_limit_events: int = 0

    async def public(self):
        if not SDK_AVAILABLE:
            raise RuntimeError("polymarket-client SDK не установлен")
        if self._public is None:
            self._public = AsyncPublicClient()
            await self._public.__aenter__()
        return self._public

    async def secure(self):
        if not SDK_AVAILABLE:
            raise RuntimeError("polymarket-client SDK не установлен")
        if self._secure is None:
            try:
                credentials = None

                if settings.clob_api_key and settings.clob_api_secret:
                    # passphrase = clob_api_passphrase если есть,
                    # иначе дублируем secret
                    passphrase = (
                        settings.clob_api_passphrase
                        or settings.clob_api_secret
                    )
                    try:
                        # Пробуем с тремя полями
                        credentials = ApiKeyCreds(
                            key=settings.clob_api_key,
                            secret=settings.clob_api_secret,
                            passphrase=passphrase,
                        )
                        logger.debug("ApiKeyCreds создан с passphrase")
                    except TypeError:
                        try:
                            # Пробуем без passphrase
                            credentials = ApiKeyCreds(
                                key=settings.clob_api_key,
                                secret=settings.clob_api_secret,
                            )
                            logger.debug("ApiKeyCreds создан без passphrase")
                        except TypeError as e:
                            logger.warning(
                                f"ApiKeyCreds не принял стандартные поля: {e}"
                            )
                            credentials = None

                self._secure = await AsyncSecureClient.create(
                    private_key=settings.my_private_key,
                    credentials=credentials,
                )
                await self._secure.__aenter__()
                logger.info("AsyncSecureClient инициализирован")

            except Exception as e:
                logger.error(f"AsyncSecureClient.create error: {e}")
                raise

        return self._secure

    async def secure_for_user(self, user):
        """
        SecureClient, работающий с ключами КОНКРЕТНОГО пользователя.

        Раньше secure() создавал один глобальный клиент из .env — то есть
        ВСЕ пользователи торговали бы с вашего кошелька. Для платного
        сервиса это неприемлемо: у каждого должны быть свои ключи.

        Клиенты кэшируются по user_id, чтобы не пересоздавать соединение
        на каждой сделке.
        """
        from core import crypto

        cached = self._user_clients.get(user.id)
        if cached is not None:
            return cached

        # Блокировка на пользователя.
        #
        # Раньше её не было: при пачке сделок все задачи одновременно
        # видели пустой кэш и КАЖДАЯ начинала создавать своего клиента.
        # А создание включает деривацию CLOB-кредов — подпись плюс
        # сетевой запрос. В логах это выглядело как ~1900мс на этапе
        # проверок сразу у трёх сделок подряд, хотя тёплый путь
        # занимает 50-70мс.
        #
        # Теперь клиента создаёт только первая задача, остальные ждут
        # её и берут готовый из кэша.
        lock = self._client_locks.get(user.id)
        if lock is None:
            lock = asyncio.Lock()
            self._client_locks[user.id] = lock

        async with lock:
            cached = self._user_clients.get(user.id)
            if cached is not None:
                return cached

            pk = crypto.decrypt(getattr(user, "private_key_enc", None))
            if not pk:
                raise RuntimeError(
                    f"у пользователя {user.id} нет приватного ключа — "
                    f"нужна настройка через /setup"
                )

            credentials = None
            api_key = crypto.decrypt(getattr(user, "clob_api_key_enc", None))
            api_secret = crypto.decrypt(getattr(user, "clob_api_secret_enc", None))
            api_pass = crypto.decrypt(
                getattr(user, "clob_api_passphrase_enc", None)
            )
            if SDK_AVAILABLE and api_key and api_secret:
                try:
                    credentials = ApiKeyCreds(
                        key=api_key, secret=api_secret, passphrase=api_pass or "",
                    )
                except TypeError:
                    try:
                        credentials = ApiKeyCreds(key=api_key, secret=api_secret)
                    except TypeError:
                        credentials = None

            # Адрес кошелька передаём ЯВНО.
            #
            # Без него SDK торгует от "Deposit Wallet" — адреса, который
            # сам выводит из ключа. Но деньги пользователя лежат там, где
            # их держит Polymarket: у аккаунтов через MetaMask это Gnosis
            # Safe, через почту — отдельный прокси. Это ДРУГИЕ адреса.
            # В итоге кнопка "Баланс" читала правильный адрес и
            # показывала деньги, а ордер уходил с пустого кошелька:
            # "not enough balance".
            #
            # Когда адрес передан, SDK сам сверяет его с ключом и
            # определяет тип (EOA / POLY_PROXY / GNOSIS_SAFE /
            # DEPOSIT_WALLET), а если адрес ключу не принадлежит —
            # явно отказывает, вместо того чтобы молча торговать
            # с другого кошелька.
            wallet_addr = getattr(user, "proxy_wallet", None) or None
            try:
                client = await AsyncSecureClient.create(
                    private_key=pk, credentials=credentials,
                    wallet=wallet_addr,
                )
            except Exception as e:
                if "does not match the signer" in str(e):
                    raise RuntimeError(
                        "адрес Polymarket из /setup не принадлежит "
                        "введённому приватному ключу. Проверьте, что "
                        "адрес скопирован из polymarket.com → Settings "
                        "именно того аккаунта, от которого ключ"
                    ) from e
                raise

            wtype = (
                getattr(getattr(client, "_ctx", None), "wallet_type", None)
                or "определён SDK"
            )
            logger.info(
                f"Клиент биржи для user={user.id}: торговля от "
                f"{(wallet_addr or 'Deposit Wallet')[:12]}..., тип {wtype}"
            )
            await client.__aenter__()
            self._user_clients[user.id] = client
            logger.info(f"SecureClient создан для пользователя {user.id}")
            return client

    async def ensure_trading_approvals(self, user) -> tuple[bool, str]:
        """
        Выдать разрешения контрактам биржи — в три ступени.

        1. Проверяем, не выданы ли они УЖЕ. Если аккаунтом хоть раз
           торговали на polymarket.com, сайт выдал их сам — транзакция
           не нужна вовсе.
        2. Пробуем выдать обычным способом.
        3. Если кошелёк — смарт-контракт (Safe или прокси аккаунта через
           Google/почту), транзакцию нужно провести через газлесс-сервис
           Polymarket (релейер), а ему нужен Builder API Key. SDK умеет
           выпустить такой ключ сам — выпускаем и повторяем.
        """
        if settings.simulation_mode:
            return True, "симуляция — разрешения не нужны"

        try:
            client = await self.secure_for_user(user)
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

        # --- Ступень 1: что уже выдано ---
        status = await self._approval_status(client)
        if status and any(status.values()):
            lines = [
                f"{'✅' if ok else '⛔'} {name}"
                for name, ok in status.items()
            ]
            missing = [name for name, ok in status.items() if not ok]
            text = "Торговать уже можно.\n\n" + "\n".join(lines)
            if missing:
                # Газлесс-выдачу здесь не запускаем: на аккаунтах через
                # Google она отвечает "internal error". Недостающие
                # разрешения сайт выдаст сам при первой ставке на рынке
                # такого типа.
                text += (
                    "\n\nНа рынках с ⛔ ордера будут отклоняться, пока "
                    "не выдано разрешение. Чтобы выдать его, сделайте на "
                    "polymarket.com одну ставку на минимальную сумму на "
                    "рынке такого типа — сайт выдаст разрешение сам."
                )
            return True, text

        # --- Ступень 2: обычная выдача ---
        try:
            handle = await client.setup_trading_approvals()
            if handle is not None and hasattr(handle, "wait"):
                await handle.wait()
            return True, "разрешения выданы"
        except Exception as e:
            first_error = f"{type(e).__name__}: {e}"
            needs_relayer = (
                "gasless" in str(e).lower()
                or "builder api key" in str(e).lower()
                or "relayer api key" in str(e).lower()
            )
            if not needs_relayer:
                logger.error(
                    f"ensure_trading_approvals user={user.id}: {first_error}"
                )
                return False, first_error

        # --- Ступень 3: через релейер с Builder API Key ---
        #
        # Кошелёк аккаунта через Google/почту или MetaMask-Safe — это
        # смарт-контракт: сам он транзакцию не подпишет, её проводит
        # релейер Polymarket без газа. Для этого SDK нужен Builder API
        # Key. Выпускаем его от имени этого же аккаунта.
        logger.info(
            f"user={user.id}: кошелёк требует газлесс-выдачи разрешений — "
            f"выпускаю Builder API Key"
        )
        relayed = None
        try:
            builder_key = await client.create_builder_api_key()

            from core import crypto
            pk = crypto.decrypt(getattr(user, "private_key_enc", None))
            relayed = await AsyncSecureClient.create(
                private_key=pk,
                wallet=getattr(user, "proxy_wallet", None) or None,
                api_key=builder_key,
            )
            await relayed.__aenter__()
            handle = await relayed.setup_trading_approvals()
            if handle is not None and hasattr(handle, "wait"):
                await handle.wait()

            # Проверяем, что разрешения действительно появились
            if await self._approvals_already_granted(relayed):
                return True, "разрешения выданы через газлесс-сервис Polymarket"
            return True, (
                "запрос на выдачу разрешений отправлен через газлесс-сервис; "
                "применение может занять до минуты"
            )
        except Exception as e:
            logger.error(
                f"газлесс-выдача разрешений user={user.id}: "
                f"{type(e).__name__}: {e}"
            )
            return False, (
                f"автоматически выдать разрешения не удалось "
                f"({type(e).__name__}: {e}).\n\n"
                f"Самый простой способ: зайдите на polymarket.com этим же "
                f"аккаунтом и сделайте одну ставку вручную на минимальную "
                f"сумму. Сайт сам выдаст разрешения, после этого бот "
                f"сможет торговать."
            )
        finally:
            if relayed is not None:
                try:
                    await relayed.__aexit__(None, None, None)
                except Exception:
                    pass

    async def _approval_status(self, client) -> dict | None:
        """
        Какие контракты биржи уже могут тратить залог (pUSD) кошелька.

        Возвращает {название: выдано ли} или None, если спросить не вышло.

        Раньше проверка требовала разрешения ВСЕМ контрактам сразу. Но
        сайт выдаёт их только тем, что нужны для рынка, где сделана
        ставка: обычные рынки идут через один контракт, рынки с
        несколькими исходами (NegRisk) — через другие. После ставки на
        одном рынке часть разрешений нулевая, и проверка "всё или
        ничего" ошибочно решала, что разрешений нет вовсе.
        """
        try:
            info = await client.get_balance_allowance(asset_type="COLLATERAL")
            allowances = getattr(info, "allowances", None) or {}
        except Exception as e:
            logger.debug(f"проверка разрешений: {type(e).__name__}: {e}")
            return None

        logger.info(f"Разрешения по контрактам: {dict(allowances)}")
        status = {}
        for spender, amount in allowances.items():
            name = _SPENDER_NAMES.get(spender.lower(), f"контракт {spender[:10]}...")
            granted = int(amount) > 0
            # у рынков NegRisk два контракта — достаточно любого из них
            status[name] = status.get(name, False) or granted
        return status

    async def _approvals_already_granted(self, client) -> bool:
        """Выдано ли хоть что-то — значит, торговать уже можно."""
        status = await self._approval_status(client)
        return bool(status) and any(status.values())

    async def drop_user_client(self, user_id: int):
        client = self._user_clients.pop(user_id, None)
        if client is not None:
            try:
                await client.__aexit__(None, None, None)
            except Exception:
                pass

    async def http(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            # Настройки соединения критичны для скорости копирования.
            #
            # По умолчанию aiohttp закрывает простаивающее соединение
            # через 15с и кэширует DNS всего 10с. Сделки трейдера
            # приходят раз в несколько минут — то есть к моменту
            # копирования соединение уже закрыто, и каждый ордер платит
            # заново: DNS-резолв + TCP-рукопожатие + TLS-рукопожатие.
            # Это 2-3 полных round-trip до Лондона ДО того, как уйдёт
            # хоть один байт полезных данных.
            connector = aiohttp.TCPConnector(
                keepalive_timeout=600,   # держим соединение 10 минут
                ttl_dns_cache=600,       # DNS не перерезолвим каждые 10с
                limit=100,
                limit_per_host=30,
                enable_cleanup_closed=True,
            )
            self._http = aiohttp.ClientSession(
                connector=connector,
                # Cloudflare перед доменами Polymarket способен прислать
                # заголовок длиннее стандартных 8190 байт, и aiohttp
                # тогда падает с LineTooLong ->
                # "ClientResponseError: 0, message=''".
                max_field_size=65536,
                max_line_size=65536,
                timeout=aiohttp.ClientTimeout(total=5),
            )
        return self._http

    async def close(self):
        if self._public:
            try:
                await self._public.__aexit__(None, None, None)
            except Exception as e:
                logger.warning(f"public client close error: {e}")
            self._public = None

        if self._secure:
            try:
                await self._secure.__aexit__(None, None, None)
            except Exception as e:
                logger.warning(f"secure client close error: {e}")
            self._secure = None

        if self._http and not self._http.closed:
            try:
                await self._http.close()
            except Exception as e:
                logger.warning(f"http session close error: {e}")
            self._http = None

    # ---------- WATCHER: чужие кошельки, Data API REST ----------

    async def get_wallet_trades(
        self, wallet: str, limit: int = 5, _retry: int = 0,
        offset: int = 0,
    ) -> list[Trade]:
        await data_api_global_limiter.acquire()
        await data_api_trades_limiter.acquire()
        session = await self.http()
        url = f"{settings.data_api_base_url}/trades"
        # offset — для постраничной выборки. Без неё бот видел только
        # последние N сделок: если трейдер делал больше между опросами
        # (или бот перезапускался), остальные просто не попадали в
        # выборку и терялись.
        params = {
            "user": wallet,
            "limit": limit,
            # КЛЮЧЕВОЕ: включаем сделки, где трейдер был МЕЙКЕРОМ.
            #
            # По умолчанию Data API отдаёт takerOnly=true — только
            # сделки, где трейдер СНОСИЛ заявки из стакана (рыночный
            # ордер). Если трейдер ставил ЛИМИТНЫЙ ордер, который ждал
            # в стакане и исполнился позже, он был мейкером — и такая
            # сделка из выдачи выпадала целиком.
            #
            # Именно так и выглядело "первую покупку бот видит, а
            # докупки и продажи нет": опытные трейдеры часто
            # усредняются и выходят лимитками. Поле side при этом
            # отдаётся с точки зрения пользователя (BUY/SELL — это то,
            # что сделал ИМЕННО он), так что смысл сделки не меняется.
            "takerOnly": "false",
        }
        if offset:
            params["offset"] = offset
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 429:
                    # Polymarket банит по IP через Cloudflare. Раньше тут
                    # был просто "return []" — молча проглатывали 429,
                    # а внешний poll-цикл тут же лупил новый запрос через
                    # poll_interval_seconds, только усугубляя бан.
                    #
                    # Ретраим только 1 раз здесь (было 4): во время
                    # затяжного 429-шторма каждый вызов, делающий по
                    # 4 попытки, сам по себе жжёт квоту и продлевает
                    # throttle. Быстрее отдаём решение наверх — там
                    # (см. watcher.py) куда более терпеливый бэкофф
                    # (до 60с), который реально даёт Cloudflare отпустить.
                    if _retry >= 1:
                        logger.warning(
                            f"data-api trades: 429 после {_retry + 1} "
                            f"попыток, сдаюсь на этот цикл"
                        )
                        raise RateLimited()
                    # Сервер часто отдаёт "Retry-After: 0", и раньше мы
                    # ретраили МГНОВЕННО (в логах это видно как
                    # "backoff 0.0s") — во время 429-шторма это просто
                    # добавляет ещё один запрос в ту же секунду и
                    # продлевает бан. Держим разумный минимум.
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        delay = float(retry_after) if retry_after else 0.0
                    except (TypeError, ValueError):
                        delay = 0.0
                    delay = max(delay, 1.5)
                    self.rate_limit_events += 1
                    # DEBUG, а не WARNING: одиночный 429 с успешным
                    # ретраем — штатная ситуация при общем IP, и на
                    # каждом опросе она засоряла лог. Реальную проблему
                    # (когда ретраи не помогли) по-прежнему видно
                    # предупреждением ниже.
                    logger.debug(
                        f"data-api trades status=429, backoff "
                        f"{delay:.1f}s (попытка {_retry + 1}/2)"
                    )
                    await asyncio.sleep(delay)
                    return await self.get_wallet_trades(
                        wallet, limit, _retry + 1, offset
                    )
                if resp.status != 200:
                    logger.warning(f"data-api trades status={resp.status}")
                    return []
                data = await resp.json()
        except RateLimited:
            raise
        except Exception as e:
            logger.error(
                f"get_wallet_trades error: {type(e).__name__}: {e}"
            )
            return []

        trades = []
        for item in data:
            try:
                price = Decimal(str(item.get("price", "0")))
                size = Decimal(str(item.get("size", "0")))
                trades.append(Trade(
                    tx_hash=item.get("transactionHash", item.get("id", "")),
                    market_id=item.get("conditionId", ""),
                    outcome_id=item.get("outcome", ""),
                    token_id=item.get("asset", item.get("tokenId", "")),
                    # side без молчаливого умолчания: если поле вдруг
                    # отсутствует, подстановка "BUY" превратила бы
                    # продажу трейдера в ПОКУПКУ — бот докупал бы там,
                    # где нужно закрываться. Лучше пропустить запись и
                    # увидеть это в логе.
                    side=_require_side(item),
                    price=price,
                    size=size,
                    usdc_amount=price * size,
                    timestamp=int(item.get("timestamp", time.time())),
                ))
            except Exception as e:
                logger.error(f"parse trade error: {e}")
        return trades

    async def get_positions(
        self,
        wallet: str,
        redeemable: bool | None = None,
        size_threshold: float = 1.0
    ) -> list[PositionInfo]:
        await data_api_global_limiter.acquire()
        await data_api_positions_limiter.acquire()
        session = await self.http()
        params = {"user": wallet, "sizeThreshold": size_threshold}
        if redeemable is not None:
            params["redeemable"] = str(redeemable).lower()
        try:
            async with session.get(
                f"{settings.data_api_base_url}/positions",
                params=params
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
        except Exception as e:
            logger.error(f"get_positions error: {type(e).__name__}: {e}")
            return []

        result = []
        for item in data:
            try:
                result.append(PositionInfo(
                    proxy_wallet=item["proxyWallet"],
                    asset=item["asset"],
                    condition_id=item["conditionId"],
                    size=Decimal(str(item["size"])),
                    avg_price=Decimal(str(item["avgPrice"])),
                    initial_value=Decimal(str(item["initialValue"])),
                    current_value=Decimal(str(item["currentValue"])),
                    cash_pnl=Decimal(str(item["cashPnl"])),
                    percent_pnl=float(item["percentPnl"]),
                    cur_price=Decimal(str(item["curPrice"])),
                    redeemable=item.get("redeemable", False),
                    mergeable=item.get("mergeable", False),
                    title=item.get("title", ""),
                    outcome=item.get("outcome", ""),
                    negative_risk=item.get("negativeRisk", False),
                ))
            except (KeyError, TypeError) as e:
                logger.error(f"parse position error: {e}")
        return result

    async def get_portfolio_value(self, wallet: str) -> Decimal:
        # Раньше этот вызов шёл БЕЗ лимитера вообще, хотя бьёт в тот же
        # хост data-api, что и /trades — и незаметно съедал квоту
        # Cloudflare у вотчера.
        await data_api_global_limiter.acquire()
        session = await self.http()
        try:
            async with session.get(
                f"{settings.data_api_base_url}/value",
                params={"user": wallet}
            ) as resp:
                if resp.status != 200:
                    return Decimal("0")
                data = await resp.json()
                return Decimal(str(data[0]["value"])) if data else Decimal("0")
        except Exception as e:
            logger.error(
                f"get_portfolio_value error: {type(e).__name__}: {e}"
            )
            return Decimal("0")

    async def get_pusd_balance_onchain(self, wallet: str) -> Decimal:
        """
        Баланс pUSD напрямую через eth_call к контракту (стандартный
        ERC-20 balanceOf), без SDK и без приватного ключа.

        Раньше свободный баланс читался через SDK-метод
        fetch_balance_allowance, который требует: (1) SDK_AVAILABLE,
        (2) непустой MY_PRIVATE_KEY, (3) актуальную версию SDK. После
        миграции Polymarket на V2 (28 апреля 2026, смена залогового
        токена USDC.e → pUSD) старые SDK-клиенты вообще перестали
        работать — а версия polymarket-client в requirements.txt не
        закреплена, так что неизвестно, обновлена ли она под V2. Итог:
        баланс мог показывать 0 либо из-за отсутствующего ключа, либо
        из-за несовместимой версии SDK, либо из-за того, что деньги
        физически лежат в pUSD, а не в USDC.e, который проверялся бы
        старым кодом.

        Прямой RPC-запрос обходит всё это: работает всегда, в том
        числе в SIMULATION_MODE, не требует ключей и не зависит от
        версии SDK.
        """
        selector = "70a08231"  # keccak256("balanceOf(address)")[:4]
        try:
            padded = wallet.lower().replace("0x", "").rjust(64, "0")
        except Exception:
            return Decimal("0")
        data = f"0x{selector}{padded}"

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {"to": PUSD_CONTRACT_ADDRESS, "data": data},
                "latest",
            ],
        }

        # Свой эндпоинт из .env пробуем первым, дальше — публичные
        # резервные. Рабочий запоминаем, чтобы не перебирать каждый раз.
        endpoints = []
        if self._working_rpc:
            endpoints.append(self._working_rpc)
        if settings.polygon_rpc_url:
            endpoints.append(settings.polygon_rpc_url)
        endpoints.extend(FALLBACK_POLYGON_RPCS)
        seen = set()
        endpoints = [
            e for e in endpoints
            if e and not (e in seen or seen.add(e))
        ]

        session = await self.http()
        last_error = None

        for url in endpoints:
            try:
                async with session.post(
                    url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    if resp.status != 200:
                        last_error = f"HTTP {resp.status}"
                        continue
                    result = await resp.json()

                if "error" in result:
                    last_error = result["error"]
                    logger.debug(f"RPC {url} отклонил запрос: {last_error}")
                    continue

                raw = result.get("result")
                if raw is None:
                    last_error = "пустой result"
                    continue
                if raw in ("0x", "0x0"):
                    self._working_rpc = url
                    return Decimal("0")

                value = int(raw, 16)
                self._working_rpc = url
                if url != settings.polygon_rpc_url:
                    logger.info(f"Баланс получен через резервный RPC: {url}")
                return Decimal(value) / Decimal(10 ** PUSD_DECIMALS)

            except Exception as e:
                last_error = f"{type(e).__name__}: {e}"
                logger.debug(f"RPC {url} недоступен: {last_error}")
                continue

        logger.warning(
            f"get_pusd_balance_onchain: ни один RPC не ответил "
            f"(проверено {len(endpoints)}). Последняя ошибка: "
            f"{last_error}. Задай рабочий эндпоинт в POLYGON_RPC_URL."
        )
        return Decimal("0")

    async def _eth_call(self, to: str, data: str) -> str | None:
        """
        Выполнить eth_call на Polygon, перебирая RPC-эндпоинты.
        Возвращает hex-результат или None, если ни один не ответил.
        """
        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [{"to": to, "data": data}, "latest"],
        }
        endpoints = []
        if self._working_rpc:
            endpoints.append(self._working_rpc)
        if settings.polygon_rpc_url:
            endpoints.append(settings.polygon_rpc_url)
        endpoints.extend(FALLBACK_POLYGON_RPCS)
        seen = set()
        endpoints = [e for e in endpoints if e and not (e in seen or seen.add(e))]

        session = await self.http()
        for url in endpoints:
            try:
                async with session.post(
                    url, json=payload, timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        continue
                    result = await resp.json(content_type=None)
                if "error" in result or result.get("result") is None:
                    continue
                self._working_rpc = url
                return result["result"]
            except Exception:
                continue
        return None

    async def get_ctf_balance(
        self, wallet: str, token_id: str
    ) -> Decimal | None:
        """
        Сколько долей токена сейчас на кошельке — прямо из блокчейна.

        Доли Polymarket — это ERC-1155 токены контракта Conditional
        Tokens (адрес не менялся при переходе на V2). Читаем
        balanceOf(address, uint256) напрямую, без Data API: он отстаёт,
        а для расчёта пропорции нужен баланс ровно в момент продажи.

        Возвращает None, если прочитать не удалось — вызывающий код
        должен это обработать, а не считать ноль.
        """
        try:
            addr = wallet.lower().replace("0x", "").rjust(64, "0")
            tid = format(int(token_id), "064x")
        except (ValueError, TypeError):
            return None
        raw = await self._eth_call(
            CTF_CONTRACT_ADDRESS, "0x00fdd58e" + addr + tid
        )
        if raw is None:
            return None
        try:
            return Decimal(int(raw, 16)) / Decimal(10 ** 6)
        except (ValueError, TypeError):
            return None

    async def get_token_balance(
        self, wallet: str, token_id: str
    ) -> Decimal | None:
        """
        Сколько долей токена у кошелька — ПРЯМО ИЗ БЛОКЧЕЙНА.

        Доли Polymarket — это ERC1155 в контракте Conditional Tokens
        (при переходе на V2 он не менялся). Читаем balanceOf напрямую
        через RPC: это точно и мгновенно, в отличие от Data API, который
        отстаёт на десятки секунд. Для пропорциональной продажи это
        принципиально: при отслеживании по цепи продажа трейдера
        приходит раньше, чем Data API успевает обновить его позиции.

        Возвращает None, если прочитать не удалось — вызывающий код
        должен сам решить, что делать без этого числа.
        """
        try:
            addr = wallet.lower().replace("0x", "").rjust(64, "0")
            tid = format(int(token_id), "064x")
        except (ValueError, TypeError):
            return None
        data = f"0x{ERC1155_BALANCE_OF}{addr}{tid}"

        endpoints = []
        if self._working_rpc:
            endpoints.append(self._working_rpc)
        if settings.polygon_rpc_url:
            endpoints.append(settings.polygon_rpc_url)
        endpoints.extend(FALLBACK_POLYGON_RPCS)
        seen = set()
        endpoints = [e for e in endpoints if e and not (e in seen or seen.add(e))]

        session = await self.http()
        for url in endpoints:
            try:
                async with session.post(
                    url,
                    json={
                        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
                        "params": [{"to": CTF_CONTRACT, "data": data}, "latest"],
                    },
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        continue
                    result = await resp.json(content_type=None)
                if "error" in result:
                    continue
                raw = result.get("result")
                if raw is None:
                    continue
                self._working_rpc = url
                if raw in ("0x", "0x0"):
                    return Decimal("0")
                return Decimal(int(raw, 16)) / Decimal(10 ** 6)
            except Exception as e:
                logger.debug(f"get_token_balance {url}: {e}")
                continue
        logger.warning("get_token_balance: ни один RPC не ответил")
        return None

    async def get_free_usdc_balance(self, wallet: str) -> Decimal:
        """
        Свободный баланс на proxy-кошельке. Основной путь — прямой
        RPC-запрос к pUSD (см. get_pusd_balance_onchain), он не требует
        ни ключей, ни SDK и отражает реальный текущий залоговый токен
        Polymarket. SDK — только как fallback, и то лишь если ключ
        реально задан (иначе он всё равно бесполезен).
        """
        onchain = await self.get_pusd_balance_onchain(wallet)
        if onchain > 0:
            return onchain

        if not SDK_AVAILABLE or not settings.my_private_key:
            return Decimal("0")
        try:
            client = await self.secure()
            balance = await client.fetch_balance_allowance(
                asset_type="COLLATERAL"
            )
            raw = getattr(balance, "balance", None)
            if raw is None and isinstance(balance, dict):
                raw = balance.get("balance")
            value = Decimal(str(raw or "0"))
            if value >= Decimal("1000000"):
                value = value / Decimal("1000000")
            return value
        except Exception as e:
            logger.warning(
                f"get_free_usdc_balance SDK fallback failed: "
                f"{type(e).__name__}: {e}"
            )
            return Decimal("0")

    async def get_total_equity(self, wallet: str) -> Decimal:
        positions_value = await self.get_portfolio_value(wallet)
        free_balance = await self.get_free_usdc_balance(wallet)
        return positions_value + free_balance

    async def get_own_trades(
        self,
        maker_address: str,
        after: int | None = None,
        limit_pages: int = 5
    ) -> list[dict]:
        session = await self.http()
        all_trades = []
        cursor = None

        # passphrase = secret если пустой
        passphrase = (
            settings.clob_api_passphrase
            or settings.clob_api_secret
        )

        for _ in range(limit_pages):
            params = {"maker_address": maker_address}
            if after:
                params["after"] = str(after)
            if cursor:
                params["next_cursor"] = cursor
            path = "/data/trades"
            try:
                headers = build_l2_headers(
                    api_key=settings.clob_api_key,
                    secret=settings.clob_api_secret,
                    passphrase=passphrase,
                    address=settings.my_wallet_address,
                    method="GET",
                    request_path=path,
                )
                async with session.get(
                    f"{settings.clob_base_url}{path}",
                    params=params,
                    headers=headers
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"get_own_trades status={resp.status}")
                        break
                    data = await resp.json()
            except Exception as e:
                logger.error(f"get_own_trades error: {e}")
                break

            all_trades.extend(data.get("data", []))
            cursor = data.get("next_cursor")
            if not cursor or cursor == "LTE=":
                break

        return all_trades

    # ---------- MARKET DATA через SDK ----------

    async def prewarm_user_clients(self) -> int:
        """
        Заранее создать SecureClient для всех активных пользователей.

        secure_for_user() создаёт клиента ЛЕНИВО — при первом ордере.
        Но создание включает деривацию CLOB-кредов, а это сетевой
        запрос. То есть первая сделка после каждого перезапуска
        оплачивала его прямо в критическом пути.

        Делаем это при старте, пока никто не торопится.
        """
        if settings.simulation_mode:
            return 0

        from sqlalchemy import select
        from core.database import async_session
        from models.user import User

        warmed = 0
        try:
            async with async_session() as session:
                users = (await session.execute(
                    select(User).where(User.is_active.is_(True))
                )).scalars().all()
                candidates = [u for u in users if u.private_key_enc]

            for user in candidates:
                try:
                    await self.secure_for_user(user)
                    warmed += 1
                except Exception as e:
                    logger.warning(
                        f"prewarm клиента user={user.id}: "
                        f"{type(e).__name__}: {e}"
                    )
        except Exception as e:
            logger.warning(f"prewarm_user_clients: {e}")

        if warmed:
            logger.info(
                f"Клиенты биржи прогреты для {warmed} пользователей"
            )
        return warmed

    def _update_time_offset(self, body: str, local_mid: float) -> None:
        """Разобрать ответ /time и обновить смещение часов."""
        try:
            raw = body.strip().strip('"')
            if raw.startswith("{"):
                import json as _json
                data = _json.loads(raw)
                raw = str(next(iter(data.values())))
            server = float(raw)
            if server > 1e12:          # пришли миллисекунды
                server /= 1000.0
            offset = server - local_mid
            if abs(offset - self.time_offset) > 2:
                logger.info(
                    f"Время сверено с Polymarket: наши часы "
                    f"{'отстают' if offset > 0 else 'спешат'} на "
                    f"{abs(offset):.1f}с"
                )
            self.time_offset = offset
            self._time_synced = True
        except Exception as e:
            logger.debug(f"разбор /time: {e}")

    def polymarket_now(self) -> float:
        """
        Текущее время ПО ЧАСАМ POLYMARKET.

        Не зависит ни от часов сервера, ни от часового пояса, ни от
        того, где запущен бот: берётся время биржи и корректируется
        по измеренному смещению. Если сверка ещё не прошла —
        возвращается время сервера.
        """
        return time.time() + self.time_offset

    async def start_connection_keepalive(self, interval: float = 20.0):
        """
        Держать соединение с CLOB "горячим".

        Сделки трейдера приходят раз в несколько минут. Даже с большим
        keepalive_timeout соединение простаивает, а промежуточное
        оборудование (роутеры, NAT, балансировщики Cloudflare) рвёт
        неактивные TCP-сессии молча. Тогда первая же сделка платит
        полный DNS + TCP + TLS — это 2-3 round-trip до Лондона.

        Лёгкий периодический запрос не даёт соединению умереть, и
        ордер уходит по уже установленному каналу.

        Интервал 20с выбран намеренно: у httpx внутри SDK
        keepalive_expiry=30с, поэтому греть нужно чаще этого порога,
        иначе соединение успевает закрыться между пингами.
        """
        if self._keepalive_task and not self._keepalive_task.done():
            return

        async def loop():
            while True:
                # 1) Наш собственный пул aiohttp (используется для
                #    /book, data-api, RPC).
                try:
                    session = await self.http()
                    t0 = time.time()
                    async with session.get(
                        f"{settings.clob_base_url}/time",
                        timeout=aiohttp.ClientTimeout(total=4),
                    ) as resp:
                        body = (await resp.read()).decode().strip()
                    t1 = time.time()
                    # Тот же запрос заодно даёт ВРЕМЯ БИРЖИ. Считаем
                    # смещение наших часов относительно Polymarket с
                    # поправкой на задержку сети (середина запроса).
                    self._update_time_offset(body, (t0 + t1) / 2)
                except Exception as e:
                    logger.debug(f"keepalive aiohttp: {type(e).__name__}: {e}")

                # 2) Пул httpx ВНУТРИ SDK — именно через него уходит
                #    ордер, и это отдельный пул соединений.
                #
                #    Раньше грелся только наш aiohttp, а SDK-шное
                #    соединение всё равно остывало: у него
                #    keepalive_expiry=30с, а сделки приходят раз в
                #    несколько минут. В результате КАЖДЫЙ ордер платил
                #    полный TCP + TLS до Лондона, хотя keepalive
                #    формально работал — просто грел не тот пул.
                for user_id, client in list(self._user_clients.items()):
                    try:
                        ctx = getattr(client, "_ctx", None)
                        clob = getattr(ctx, "clob", None)
                        if clob is None:
                            continue
                        await clob.get_json("/time")
                    except Exception as e:
                        logger.debug(
                            f"keepalive sdk user={user_id}: "
                            f"{type(e).__name__}: {e}"
                        )

                await asyncio.sleep(interval)

        self._keepalive_task = asyncio.create_task(loop())
        logger.info("Keepalive соединения с CLOB запущен")

    async def warm_order_metadata(self, user, token_id: str) -> None:
        """
        Прогреть кэш метаданных рынка внутри SDK.

        Перед подписью ордера SDK вызывает
        order_metadata.resolve_market() — ему нужны tick_size и
        neg_risk. Результат кэшируется на 10 минут, НО копитрейдинг
        почти всегда заходит в НОВЫЕ рынки, поэтому кэш промахивается
        практически на каждой сделке. Этот сетевой запрос происходит
        внутри place_market_order, то есть прямо в критическом пути,
        последовательно перед отправкой ордера.

        Здесь мы запускаем его ЗАРАНЕЕ и параллельно с нашими
        собственными проверками. Когда дело дойдёт до ордера, SDK
        возьмёт готовое значение из кэша и сразу подпишет.

        Это обращение к внутреннему API SDK, поэтому всё обёрнуто:
        если структура изменится, копирование просто пойдёт как
        раньше, без ускорения, но без поломки.
        """
        if settings.simulation_mode:
            return
        try:
            client = await self.secure_for_user(user)
            ctx = getattr(client, "_ctx", None)
            metadata = getattr(ctx, "order_metadata", None)
            if ctx is None or metadata is None:
                return
            await metadata.resolve_market(ctx, token_id=token_id)
        except Exception as e:
            # Раньше это писалось в debug и терялось. Но если прогрев не
            # работает, SDK тянет метаданные рынка сам — синхронно
            # внутри place_market_order, добавляя сетевой round-trip в
            # критический путь. Молча терять такое нельзя, поэтому
            # предупреждаем один раз, а дальше не шумим.
            if not self._warm_failed_logged:
                self._warm_failed_logged = True
                logger.warning(
                    f"Прогрев метаданных SDK не работает "
                    f"({type(e).__name__}: {e}). Ордера будут медленнее: "
                    f"SDK запросит метаданные сам во время отправки."
                )
            else:
                logger.debug(f"warm_order_metadata: {e}")

    async def get_book_snapshot(self, token_id: str) -> dict:
        """
        ОДИН запрос к /book вместо двух: отдаёт и цены, и ограничения.

        Раньше в критическом пути боевого копирования шли ДВА отдельных
        сетевых вызова — get_market_price и get_market_constraints.
        Это был лишний round-trip на каждой сделке.

        Возвращает best_bid, best_ask, min_order_size, tick_size.
        best_ask — цена, по которой мы реально КУПИМ, best_bid — по
        которой продадим. Она же уходит в ордер как limit_price, чтобы
        SDK не запрашивал стакан повторно.
        """
        try:
            session = await self.http()
            url = f"{settings.clob_base_url}/book"
            async with session.get(
                url, params={"token_id": token_id},
                timeout=aiohttp.ClientTimeout(total=4),
            ) as resp:
                if resp.status != 200:
                    return {}
                data = await resp.json(content_type=None)

            def _best(levels, reverse):
                vals = [
                    Decimal(str(l.get("price")))
                    for l in (levels or []) if l.get("price") not in (None, "")
                ]
                if not vals:
                    return None
                return max(vals) if reverse else min(vals)

            return {
                # condition_id рынка: в событии из блокчейна его нет, а
                # без него невозможно погасить выигравшую позицию
                "market": data.get("market") or "",
                "best_bid": _best(data.get("bids"), reverse=True),
                "best_ask": _best(data.get("asks"), reverse=False),
                "min_order_size": Decimal(
                    str(data.get("min_order_size", "0") or "0")
                ),
                "tick_size": Decimal(
                    str(data.get("tick_size", "0.01") or "0.01")
                ),
            }
        except Exception as e:
            logger.debug(
                f"get_book_snapshot({token_id[:12]}...): "
                f"{type(e).__name__}: {e}"
            )
            return {}

    async def get_market_constraints(self, token_id: str) -> dict:
        """
        Ограничения рынка из стакана: минимальный размер ордера в
        ДОЛЯХ и шаг цены.

        Polymarket меряет минимум в долях (обычно 5), а не в долларах.
        В деньгах это зависит от цены: 5 долей по 0.10 — это $0.50, а
        по 0.90 — уже $4.50. Поэтому мелкая ставка на дорогом рынке
        отклоняется с ошибкой вида
        "Size (1.08) lower than the minimum: 5".
        """
        try:
            session = await self.http()
            url = f"{settings.clob_base_url}/book"
            async with session.get(
                url, params={"token_id": token_id},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status != 200:
                    return {}
                data = await resp.json(content_type=None)
            return {
                "min_order_size": Decimal(
                    str(data.get("min_order_size", "0") or "0")
                ),
                "tick_size": Decimal(
                    str(data.get("tick_size", "0.01") or "0.01")
                ),
            }
        except Exception as e:
            logger.debug(
                f"get_market_constraints({token_id[:12]}...): "
                f"{type(e).__name__}: {e}"
            )
            return {}

    async def get_market_price(self, token_id: str) -> Decimal | None:
        """
        Получить текущую цену токена.
        Сначала — кэш из Market WS (мгновенно, без сети). Если токен
        ещё не подписан (совершенно новый рынок) — идём в REST/SDK
        с fallback-цепочкой, как раньше.
        """
        cached = market_ws_manager.get_cached_price(token_id)
        if cached is not None:
            return cached

        if not SDK_AVAILABLE:
            return None
        try:
            client = await self.public()

            # Вариант 1 — get_midpoint
            if hasattr(client, 'get_midpoint'):
                try:
                    midpoint = await client.get_midpoint(token_id=token_id)
                    value = (
                        getattr(midpoint, "mid", None)
                        or getattr(midpoint, "price", None)
                    )
                    if value is not None:
                        return Decimal(str(value))
                except Exception as e:
                    logger.debug(f"get_midpoint failed: {e}")

            # Вариант 2 — get_last_trade_price
            if hasattr(client, 'get_last_trade_price'):
                try:
                    result = await client.get_last_trade_price(
                        token_id=token_id
                    )
                    value = getattr(result, "price", None)
                    if value is not None:
                        return Decimal(str(value))
                except Exception as e:
                    logger.debug(f"get_last_trade_price failed: {e}")

            # Вариант 3 — get_price
            if hasattr(client, 'get_price'):
                try:
                    result = await client.get_price(
                        token_id=token_id, side="BUY"
                    )
                    value = getattr(result, "price", None) or result
                    if value is not None:
                        return Decimal(str(value))
                except Exception as e:
                    logger.debug(f"get_price failed: {e}")

            # Вариант 4 — get_order_book midpoint
            if hasattr(client, 'get_order_book'):
                try:
                    book = await client.get_order_book(token_id=token_id)
                    bids = getattr(book, "bids", [])
                    asks = getattr(book, "asks", [])
                    if bids and asks:
                        best_bid = Decimal(str(
                            getattr(bids[0], "price", 0)
                        ))
                        best_ask = Decimal(str(
                            getattr(asks[0], "price", 0)
                        ))
                        if best_bid and best_ask:
                            return (best_bid + best_ask) / 2
                except Exception as e:
                    logger.debug(f"get_order_book midpoint failed: {e}")

            logger.warning(
                f"get_market_price: все методы не сработали "
                f"для token={token_id[:16]}..."
            )
            return None

        except Exception as e:
            logger.error(f"get_market_price error: {type(e).__name__}: {e}")
            return None

    # ---------- TRADING через unified SDK ----------

    async def place_market_order(
        self,
        token_id: str,
        side: str,
        amount_usdc: Decimal,
        price_hint: Decimal | None = None,
        user=None,
        shares: Decimal | None = None,
        limit_price: Decimal | None = None,
    ) -> OrderResult:
        """
        Рыночный ордер.

        amount_usdc — сумма в USDC (для BUY).
        shares      — количество долей (для SELL). У Polymarket рыночная
                      ПРОДАЖА измеряется в долях, а не в долларах: раньше
                      сюда передавалась сумма в USDC, из-за чего при
                      входе на 10 USDC по цене 0.25 (=40 долей) продавать
                      пытались 10 долей вместо 40, и три четверти позиции
                      зависали.
        user        — чьими ключами торгуем. Без него в боевом режиме
                      ордер не отправляется.
        """
        if settings.simulation_mode:
            # price_hint — цена сделки, которую мы копируем. Она уже
            # пришла от Data API вместе с самой сделкой, так что в
            # симуляции это и есть корректная цена входа. Раньше здесь
            # безусловно вызывался get_market_price(), который лезет в
            # SDK и занимал ~1.2с на КАЖДОЙ копии — это была основная
            # часть задержки копирования. В сеть идём только если
            # хинта нет.
            price = price_hint
            if price is None or price <= 0:
                price = await self.get_market_price(token_id) or Decimal("0.5")
            filled_size = (
                amount_usdc / price if price > 0 else Decimal("0")
            )
            logger.info(
                f"[SIM] {side} token={token_id[:16]}... amount={amount_usdc}"
            )
            return OrderResult(
                success=True,
                tx_hash=f"SIM-{int(time.time()*1000)}",
                filled_price=price,
                filled_size=filled_size,
            )

        await clob_order_limiter.acquire()
        try:
            if user is not None:
                client = await self.secure_for_user(user)
            else:
                # Боевой режим без пользователя = торговля с глобального
                # кошелька из .env. Для платного сервиса это ошибка, и
                # лучше упасть явно, чем списать чужие деньги со своего
                # счёта.
                raise RuntimeError(
                    "place_market_order без user в боевом режиме запрещён"
                )

            # side передаётся ОБЫЧНОЙ СТРОКОЙ.
            #
            # В SDK OrderSide — это TypeAlias:
            #     OrderSide: TypeAlias = Literal["BUY", "SELL"]
            # то есть псевдоним типа, а НЕ enum. Обращение OrderSide.BUY
            # даёт AttributeError, у которого str(e) == 'BUY' — ровно
            # то, что было видно в логе как
            # "place_market_order failed: BUY". Боевой ордер падал на
            # этой строке, даже не дойдя до биржи. То же самое было с
            # OrderType.FAK.
            order_side = "BUY" if side == "BUY" else "SELL"

            # BUY и SELL — разные наборы параметров (в SDK это две
            # отдельные перегрузки):
            #   BUY  -> amount (сколько потратить) + max_spend
            #   SELL -> shares (сколько долей продать)
            # Явная цена убирает ЛИШНИЙ сетевой запрос внутри SDK.
            #
            # Без неё SDK идёт по "незащищённой" ветке и сам тянет
            # стакан, чтобы вычислить цену для подписи — это целый
            # round-trip до Лондона внутри place_market_order. Но мы
            # стакан УЖЕ получили в своих проверках, поэтому просто
            # передаём цену: SDK берёт защищённую ветку и в сеть за
            # стаканом не ходит.
            #
            # Побочная польза: max_price/min_price работают как защита
            # цены на стороне биржи — ордер не исполнится хуже указанного.
            if order_side == "SELL":
                if shares is None or shares <= 0:
                    return OrderResult(
                        success=False, tx_hash=None, filled_price=None,
                        filled_size=None,
                        error="SELL без количества долей",
                    )
                order_kwargs = {"shares": float(shares)}
                if limit_price and limit_price > 0:
                    order_kwargs["min_price"] = float(limit_price)
            else:
                # max_spend НЕ передаём.
                #
                # По документации SDK это "all-in spend target" —
                # потолок трат ВМЕСТЕ с комиссиями. Передавая
                # max_spend == amount, мы заставляли SDK ужимать сам
                # ордер, чтобы комиссии влезли в тот же доллар: ставка
                # 1.00 превращалась в ордер на $0.96, а минимум
                # площадки для рыночной покупки — ровно $1. Отсюда
                # "invalid amount for a marketable BUY order ($0.96),
                # min size: 1".
                #
                # Передаём только amount: ставка уходит целиком,
                # комиссии считаются сверх неё.
                order_kwargs = {"amount": float(amount_usdc)}
                if limit_price and limit_price > 0:
                    order_kwargs["max_price"] = float(limit_price)

            response = await client.place_market_order(
                token_id=token_id,
                side=order_side,
                order_type="FAK",
                **order_kwargs,
            )
            # Разбор ответа по РЕАЛЬНОЙ модели SDK.
            #
            # Раньше поля читались в camelCase (makingAmount, orderID,
            # transactionsHashes, price). В модели они объявлены в
            # snake_case — camelCase используется только как
            # validation_alias при разборе сырого JSON. Поэтому getattr
            # всегда возвращал значение по умолчанию: filled_size
            # выходил 0, в позицию записывалось shares_bought=0, и при
            # закрытии SELL падал с "SELL без количества долей".
            # Поля price в модели нет вообще.
            if getattr(response, "ok", None) is False:
                # RejectedOrder: биржа отказала с машинным кодом
                code = getattr(response, "code", "?")
                message = getattr(response, "message", "")
                logger.error(
                    f"Ордер отклонён биржей: code={code} {message}"
                )
                return OrderResult(
                    success=False, tx_hash=None, filled_price=None,
                    filled_size=None, error=f"{code}: {message}",
                )

            status = getattr(response, "status", None)
            making = Decimal(str(getattr(response, "making_amount", 0) or 0))
            taking = Decimal(str(getattr(response, "taking_amount", 0) or 0))

            # Нормализация единиц.
            #
            # SDK не приводит making_amount/taking_amount к «человеческим»
            # величинам — валидатор просто парсит строку. Исходный код
            # проекта делил их на 1e6, то есть считал, что биржа отдаёт
            # АТОМАРНЫЕ единицы (6 знаков). Проверить это заранее нельзя:
            # ответ зависит от версии API.
            #
            # Поэтому проверяем на правдоподобие. Цена доли на Polymarket
            # всегда в диапазоне 0..1, значит количество долей не может
            # превышать потраченную сумму больше чем в 1000 раз (минимальная
            # цена 0.001). Если превышает — единицы атомарные, делим.
            #
            # Без этой проверки количество долей завышалось в миллион раз:
            # позиция показывала абсурдный плюс, а на продажу уходило
            # неверное количество.
            making, taking = _normalize_amounts(making, taking)
            tx_hashes = getattr(response, "transactions_hashes", ()) or ()
            order_id = getattr(response, "order_id", None)

            # making_amount — что мы ОТДАЛИ, taking_amount — что
            # ПОЛУЧИЛИ. Для покупки отдаём USDC и получаем доли, для
            # продажи наоборот.
            if order_side == "BUY":
                spent, got_shares = making, taking
            else:
                spent, got_shares = taking, making

            fill_price = (spent / got_shares) if got_shares > 0 else None

            if status == "matched":
                if got_shares <= 0:
                    # Подстраховка: без количества долей позицию потом
                    # нечем будет закрыть.
                    logger.error(
                        f"Ордер matched, но количество долей нулевое "
                        f"(making={making}, taking={taking})"
                    )
                    return OrderResult(
                        success=False, tx_hash=None, filled_price=None,
                        filled_size=None,
                        error="биржа не вернула количество долей",
                    )
                return OrderResult(
                    success=True,
                    tx_hash=tx_hashes[0] if tx_hashes else order_id,
                    filled_price=fill_price,
                    filled_size=got_shares,
                )
            elif status == "delayed":
                # "delayed" — биржа ПРИНЯЛА ордер и исполнит его с
                # задержкой (на части рынков сведение нарочно
                # откладывается на несколько секунд). Это НЕ отказ.
                #
                # Раньше при нулевом количестве в ответе ордер считался
                # неудачей и позиция не создавалась. Но через пару
                # секунд он исполнялся — доли появлялись на кошельке, а
                # в базе позиции не было: ни стоп-лосса, ни
                # тейк-профита, и продажа трейдера давала "закрывать
                # нечего". Пользователь видел "ордер не исполнен", а
                # ставка на рынке стояла.
                #
                # Теперь считаем ордер принятым с ОЦЕНКОЙ количества, а
                # точное значение сверка возьмёт из блокчейна через
                # несколько секунд. Если исполнения так и не будет —
                # сверка пометит позицию несостоявшейся.
                est_price = fill_price or price_hint or limit_price
                if got_shares > 0:
                    est_shares = got_shares
                elif order_side == "SELL":
                    est_shares = Decimal(str(shares or 0))
                elif est_price and Decimal(str(est_price)) > 0:
                    est_shares = (
                        Decimal(str(amount_usdc)) / Decimal(str(est_price))
                    )
                else:
                    est_shares = Decimal("0")

                logger.warning(
                    f"Ордер {order_id} принят с отложенным исполнением — "
                    f"оценка {est_shares:.2f} долей, точное количество "
                    f"сверим по блокчейну"
                )
                return OrderResult(
                    success=est_shares > 0, tx_hash=order_id,
                    filled_price=(
                        Decimal(str(est_price)) if est_price else None
                    ),
                    filled_size=est_shares if est_shares > 0 else None,
                    error="delayed",
                )
            elif status == "live":
                # "live" = ордер ПРИНЯТ, но стоит в стакане и НЕ
                # исполнен (полностью или частично).
                #
                # Раньше это возвращалось как success=True с нулевым
                # количеством долей. В результате записывалась позиция,
                # которой физически не существует: на кошельке ноль
                # долей, а в базе открытая позиция. Потом стоп-лосс
                # пытался её продать и получал от биржи
                # "balance: 0, order amount: ...", а дальше бесконечно
                # повторял попытки.
                if got_shares <= 0:
                    logger.warning(
                        f"Ордер {order_id} принят, но НЕ исполнен "
                        f"(стоит в стакане) — позицию не открываем"
                    )
                    return OrderResult(
                        success=False, tx_hash=order_id,
                        filled_price=None, filled_size=None,
                        error="ордер не исполнен, стоит в стакане",
                    )
                return OrderResult(
                    success=True, tx_hash=order_id,
                    filled_price=fill_price,
                    filled_size=got_shares,
                )
            else:
                logger.warning(
                    f"Неожиданный статус ордера: {status} "
                    f"(making={making}, taking={taking})"
                )
                return OrderResult(
                    success=False, tx_hash=order_id, filled_price=None,
                    filled_size=None, error=f"status={status}",
                )

        except Exception as e:
            err_str = str(e)
            if "closed only mode" in err_str or "address banned" in err_str:
                logger.critical(
                    f"🚫 Аккаунт заблокирован для торговли: {err_str}"
                )
            logger.error(
                f"place_market_order failed ({side}): "
                f"{type(e).__name__}: {e}"
            )
            return OrderResult(
                success=False,
                tx_hash=None,
                filled_price=None,
                filled_size=None,
                error=err_str
            )

    async def place_resting_sell(
        self, token_id: str, shares: Decimal, price: Decimal, user
    ) -> OrderResult:
        """
        Выставить лимитную продажу, которая ЖДЁТ покупателя.

        Зачем: на тонком рынке рыночный ордер (FAK) исполняется только
        тем, что прямо сейчас стоит в стакане. Если встречных заявок в
        этот момент нет, биржа отвечает "no orders found to match" и
        ордер просто уничтожается. Бот повторяет попытку позже — и
        продаёт уже по худшей цене.

        Лимитный ордер вместо этого ОСТАЁТСЯ в стакане и исполнится
        сам, как только появится покупатель по нашей цене. Для выхода
        из позиции это принципиально лучше: не нужно угадывать момент,
        когда в стакане есть ликвидность.
        """
        if settings.simulation_mode:
            return OrderResult(
                success=True, tx_hash=f"SIM-LIMIT-{int(time.time())}",
                filled_price=price, filled_size=shares,
            )

        await clob_order_limiter.acquire()
        try:
            client = await self.secure_for_user(user)
            response = await client.place_limit_order(
                token_id=token_id,
                price=float(price),
                size=float(shares),
                side="SELL",
            )

            if getattr(response, "ok", None) is False:
                return OrderResult(
                    success=False, tx_hash=None, filled_price=None,
                    filled_size=None,
                    error=f"{getattr(response, 'code', '?')}: "
                          f"{getattr(response, 'message', '')}",
                )

            status = getattr(response, "status", None)
            order_id = getattr(response, "order_id", None)
            making = Decimal(str(getattr(response, "making_amount", 0) or 0))
            taking = Decimal(str(getattr(response, "taking_amount", 0) or 0))
            making, taking = _normalize_amounts(making, taking)

            logger.info(
                f"Лимитная продажа выставлена: {shares} долей по "
                f"{price}, статус={status}, id={order_id}"
            )
            return OrderResult(
                success=True, tx_hash=order_id,
                filled_price=price,
                filled_size=making if making > 0 else shares,
                error=None if status == "matched" else "resting",
            )
        except Exception as e:
            logger.error(
                f"place_resting_sell failed: {type(e).__name__}: {e}"
            )
            return OrderResult(
                success=False, tx_hash=None, filled_price=None,
                filled_size=None, error=str(e),
            )

    async def redeem_positions(
        self, condition_id: str, user=None
    ) -> OrderResult:
        if settings.simulation_mode:
            logger.info(f"[SIM] redeem condition_id={condition_id}")
            return OrderResult(
                success=True,
                tx_hash=f"SIM-REDEEM-{int(time.time())}",
                filled_price=None,
                filled_size=None
            )
        try:
            # Клиент КОНКРЕТНОГО пользователя.
            #
            # Раньше здесь был self.secure() — глобальный клиент из
            # .env. После перехода на ключи каждого пользователя
            # глобальные креды пустые, поэтому погашение выигравших
            # позиций падало всегда: деньги оставались в токенах,
            # а не приходили на кошелёк.
            if user is not None:
                client = await self.secure_for_user(user)
            else:
                client = await self.secure()
            redeem = await client.redeem_positions(condition_id=condition_id)
            await redeem.wait()
            return OrderResult(
                success=True,
                tx_hash=getattr(redeem, "transaction_hash", None),
                filled_price=None,
                filled_size=None
            )
        except Exception as e:
            logger.error(f"redeem_positions failed: {e}")
            return OrderResult(
                success=False,
                tx_hash=None,
                filled_price=None,
                filled_size=None,
                error=str(e)
            )


polymarket_client = PolymarketClient()