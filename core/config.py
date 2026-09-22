from decimal import Decimal
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Telegram
    telegram_bot_token: str
    allowed_user_ids: str = ""

    # Blockchain / wallet
    my_wallet_address: str = ""       # EOA signer address
    my_private_key: str = ""          # EOA private key (никогда не коммитить!)
    my_proxy_wallet_address: str = "" # Polymarket proxy/Safe wallet (funder)
    proxy_wallet_type: str = "SAFE"   # "SAFE" | "PROXY" — см. Relayer docs
    polygon_rpc_url: str = "https://polygon-rpc.com"

    # Polymarket endpoints
    clob_base_url: str = "https://clob.polymarket.com"
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    data_api_base_url: str = "https://data-api.polymarket.com"
    relayer_base_url: str = "https://relayer-v2.polymarket.com"
    market_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    user_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

    # CLOB L2 credentials (создаются через auth/api-key flow либо SDK)
    clob_api_key: str = ""
    clob_api_secret: str = ""
    clob_api_passphrase: str = ""

    # Builder API Key (для Relayer/атрибуции, если зарегистрирован)
    builder_api_key: str = ""
    builder_api_secret: str = ""
    builder_api_passphrase: str = ""
    builder_code: str = ""  # bytes32 hex, опционально

    # DB / Redis
    database_url: str = "sqlite+aiosqlite:///./polybot.db"
    redis_url: str = "redis://localhost:6379/0"

    # Trading defaults
    default_bet_amount: Decimal = Decimal("10.0")
    default_bet_mode: str = "fixed"
    max_bet_amount: Decimal = Decimal("100.0")
    poll_interval_seconds: float = 0.35
    # Окно (сек), в течение которого повторный вход в ТОТ ЖЕ рынок с
    # той же стороной считается частью уже скопированной ставки, а не
    # новой. Один ордер трейдера Polymarket проводит несколькими
    # on-chain транзакциями (каждая со своим хэшем) — без этого окна
    # бот копирует каждую транзакцию отдельной ставкой.
    # Окно схлопывания транзакций ОДНОГО ордера, в секундах.
    #
    # Считается по МЕТКАМ ВРЕМЕНИ САМИХ СДЕЛОК (шкала API), а не по
    # часам бота: две транзакции одного ордера всегда лежат в пределах
    # пары секунд друг от друга, независимо от того, когда бот успел их
    # увидеть. Поэтому значение можно держать небольшим.
    #
    # Больше значение — надёжнее схлопывание, но перезаходы трейдера
    # быстрее этого интервала будут пропущены.
    copy_dedup_window_seconds: int = 5
    # Максимальный возраст сделки (сек) относительно самой свежей в том
    # же ответе API. Более старые не копируются: цена рынка уже ушла,
    # позиция откроется по исторической цене и мгновенно схлопнется по
    # TP/SL. 0 — отключить проверку.
    # Максимальный возраст сделки относительно самой свежей в том же
    # ответе API. Более старые не копируются: цена рынка уже ушла.
    #
    # 300с, а не 120: пересборка контейнера занимает 1-2 минуты, плюс
    # Data API отдаёт сделки с задержкой. При 120с сделки, сделанные
    # во время деплоя, отбрасывались как устаревшие — и выглядело это
    # как "бот не видит часть ставок".
    max_trade_age_seconds: int = 300
    # Смещение для времени в уведомлениях Telegram (часы от UTC).
    # 3 = Хельсинки летом.
    display_timezone_offset_hours: int = 3
    # Минимум площадки для рыночной покупки. Биржа отклоняет ордера
    # меньше этой суммы: "invalid amount for a marketable BUY order".
    min_order_usdc: Decimal = Decimal("1.0")
    # Поднимать ставку до минимума рынка, если она чуть не дотягивает.
    # Минимум задан в ДОЛЯХ (обычно 5), а в долларах зависит от цены:
    # 5 долей по 0.10 — это $0.50, по 0.90 — уже $4.50. Без этого
    # часть сделок отсекается просто потому, что рынок дорогой.
    # Подъём ограничен MAX_BET_AMOUNT.
    auto_bump_to_min_order: bool = True
    # Через сколько секунд после открытия сверить позицию с биржей.
    # Ответ на ордер может сообщать о принятии, а не об исполнении —
    # тогда позиция есть в базе, но её нет на кошельке. Данные на
    # бирже появляются не мгновенно, поэтому сверяем с задержкой.
    position_verify_delay_seconds: int = 12
    # На сколько секунд назад догонять сделки после перезапуска бота.
    # Покрывает пересборку контейнера (1-2 минуты) с запасом. Сделки
    # старше — неактуальны: трейдер мог давно из них выйти.
    resume_catchup_seconds: int = 600
    # Сколько секунд считать части одного ЛИМИТНОГО ордера трейдера
    # одной ставкой. Лимитка исполняется кусками в течение нескольких
    # минут, все куски — ровно по её цене. Сделки по ТОЙ ЖЕ цене в этом
    # окне не копируются повторно; по другой цене — это перезаход.
    maker_fill_window_seconds: int = 900
    # Не копировать сделку, если бот увидел её позже, чем через N
    # секунд после трейдера. 0 = копировать всё.
    #
    # На быстрых рынках (BTC Up/Down на 5 минут) цена за минуту
    # уходит так далеко, что поздняя копия почти гарантированно
    # убыточна: трейдер вошёл по одной цене, бот — по совсем другой.
    max_detection_lag_seconds: int = 0
    # Отклонение НЕ в нашу пользу (покупаем дороже / продаём дешевле).
    # Торговать, даже если проверку региона выполнить не удалось.
    # По умолчанию выключено: без подтверждения региона боевая
    # торговля останавливается.
    geoblock_fail_open: bool = False
    # Ограничения риска (раньше их не было вовсе: лимит применялся
    # только к ОДНОЙ ставке в percent-режиме)
    # Сколько сделок максимум копировать за ОДИН опрос.
    #
    # Защита от аномальных пачек: если из-за сбоя в дедупликации или
    # смены кошелька бот вдруг увидит десятки "новых" сделок, он не
    # должен вывалить их все на биржу и опустошить баланс.
    # Дневной лимит убытка в USDC. При достижении копирование
    # останавливается до следующих суток. 0 = без лимита.
    #
    # Последний рубеж: срабатывает независимо от причины потерь —
    # будь то ошибка в коде, сбой биржи или просто плохая серия.
    daily_loss_limit: Decimal = Decimal("0")
    # 0 = без ограничения: копируются ВСЕ сделки трейдера после Старта.
    #
    # Ограничение вводилось после сбоя, когда из-за ошибки в ключе кэша
    # бот скопировал историю кошелька разом. С курсором такой сценарий
    # невозможен в принципе: история до Старта не копируется никогда.
    # Реальную защиту денег дают DAILY_LOSS_LIMIT и лимиты экспозиции.
    max_copies_per_poll: int = 0
    max_open_positions: int = 20
    max_total_exposure: Decimal = Decimal("200.0")

    # Simulation
    simulation_mode: bool = True

    # Reconciliation
    reconciliation_sweep_interval_seconds: float = 60.0

    @property
    def allowed_ids(self) -> set[int]:
        return {int(x) for x in self.allowed_user_ids.split(",") if x.strip()}


settings = Settings()