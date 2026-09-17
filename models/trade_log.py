from datetime import datetime
from sqlalchemy import String, DateTime, ForeignKey, JSON, func
from sqlalchemy.orm import Mapped, mapped_column
from models.base import Base


class TradeLog(Base):
    __tablename__ = "trade_logs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    position_id: Mapped[int | None] = mapped_column(ForeignKey("positions.id"), nullable=True)
    action: Mapped[str] = mapped_column(String(48))
    details: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())