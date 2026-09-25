from datetime import datetime
from decimal import Decimal
from sqlalchemy import BigInteger, String, Numeric, Boolean, DateTime, Float, func
from sqlalchemy.orm import Mapped, mapped_column
from models.base import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)  # telegram id
    target_wallet: Mapped[str | None] = mapped_column(String(64), nullable=True)

    bet_amount: Mapped[Decimal] = mapped_column(Numeric(18, 4), default=Decimal("10"))
    bet_mode: Mapped[str] = mapped_column(String(16), default="fixed")  # fixed|percent
    bet_percent: Mapped[float] = mapped_column(Float, default=5.0)

    tp_percent: Mapped[float | None] = mapped_column(Float, nullable=True)
    sl_percent: Mapped[float | None] = mapped_column(Float, nullable=True)

    is_active: Mapped[bool] = mapped_column(Boolean, default=False)

    # Докупать ли вслед за трейдером на уже открытом рынке.
    #   True  — копируются все покупки трейдера на рынке, включая
    #           перезаходы и усреднения;
    #   False — на рынок заходим ОДНОЙ ставкой; повторные покупки
    #           трейдера, пока наша позиция открыта, пропускаются.
    # Продажи копируются всегда, независимо от этой настройки.
    # По умолчанию True — чтобы поведение не изменилось само собой.
    allow_reentries: Mapped[bool] = mapped_column(
        Boolean, default=True, server_default="true"
    )

    # proxy_wallet — Polymarket funder-адрес ПОЛЬЗОВАТЕЛЯ (тот, что виден
    # в Settings на polymarket.com). На нём лежат его деньги и позиции.
    proxy_wallet: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # --- Учётные данные пользователя для торговли ---
    # Хранятся ТОЛЬКО в зашифрованном виде (core/crypto.py).
    # Приватный ключ нужен, потому что ордера Polymarket подписываются
    # EIP-712 подписью кошелька — одних CLOB-кредов недостаточно.
    signer_address: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )  # EOA-адрес, которым подписываются ордера
    private_key_enc: Mapped[str | None] = mapped_column(
        String(512), nullable=True
    )
    clob_api_key_enc: Mapped[str | None] = mapped_column(
        String(512), nullable=True
    )
    clob_api_secret_enc: Mapped[str | None] = mapped_column(
        String(512), nullable=True
    )
    clob_api_passphrase_enc: Mapped[str | None] = mapped_column(
        String(512), nullable=True
    )
    # SAFE (Polymarket UI) или EOA
    proxy_wallet_type: Mapped[str] = mapped_column(
        String(16), default="SAFE"
    )

    # Докупать ли вслед за трейдером на том же рынке.
    #   True  — копируются все входы трейдера, включая докупки/перезаходы
    #   False — один вход на рынок; пока позиция открыта, докупки трейдера
    #           пропускаются. После закрытия позиции следующий вход
    #           трейдера копируется как новый.
    # Продажи от этой настройки не зависят.
    allow_reentry: Mapped[bool] = mapped_column(Boolean, default=True)

    # Минимальный размер ПОКУПКИ трейдера (в USDC), которую стоит
    # копировать. Пусто или 0 — копировать любые.
    #
    # Крупные трейдеры часто делают мелкие сделки: хвосты своих
    # лимитных ордеров, "айсберг" — скрытый набор позиции мелкими
    # частями, а иногда и приманку для копировщиков. При фиксированной
    # ставке бот превращал доллар трейдера в свою полную ставку.
    # Продажи фильтр НЕ затрагивает — иначе бот не выходил бы вслед за
    # трейдером при частичных выходах.
    min_trader_trade_usdc: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 6), nullable=True
    )

    # Пользователь завершил мастер настройки
    setup_completed: Mapped[bool] = mapped_column(Boolean, default=False)
    # Согласился с предупреждением о рисках хранения ключа
    risk_accepted: Mapped[bool] = mapped_column(Boolean, default=False)

    def has_trading_credentials(self) -> bool:
        return bool(
            self.proxy_wallet
            and self.private_key_enc
            and self.setup_completed
        )

    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )