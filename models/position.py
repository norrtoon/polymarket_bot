from datetime import datetime
from decimal import Decimal
from sqlalchemy import (
    String, Numeric, DateTime, ForeignKey, func, Float, Index
)
from sqlalchemy.orm import Mapped, mapped_column
from models.base import Base


class Position(Base):
    __tablename__ = "positions"
    # Индексы под запросы в критическом пути копирования.
    # Раньше их не было вообще: _count_open_positions (номер
    # перезахода) и поиск открытых позиций при SELL делали полный
    # скан таблицы на КАЖДОЙ сделке. Пока позиций десятки — это
    # незаметно, но таблица растёт с каждой скопированной ставкой,
    # и скан дорожает линейно.
    __table_args__ = (
        Index(
            "ix_positions_user_token_status",
            "user_id", "token_id", "status",
        ),
        Index("ix_positions_status", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))

    market_id: Mapped[str] = mapped_column(String(128))     # condition_id
    outcome_id: Mapped[str] = mapped_column(String(128))
    token_id: Mapped[str] = mapped_column(String(128))       # asset_id

    entry_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    amount_usdc: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    shares_bought: Mapped[Decimal] = mapped_column(Numeric(18, 6), default=Decimal("0"))

    tx_hash_copy: Mapped[str] = mapped_column(String(128))
    tx_hash_ours: Mapped[str | None] = mapped_column(String(128), nullable=True)

    status: Mapped[str] = mapped_column(String(24), default="open")
    # open | closed | tp_hit | sl_hit | resolved_won | resolved_lost

    tp_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    sl_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    current_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    pnl_percent: Mapped[float | None] = mapped_column(Float, nullable=True)

    opened_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    closed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)