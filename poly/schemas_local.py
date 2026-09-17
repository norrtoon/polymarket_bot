from dataclasses import dataclass
from decimal import Decimal


@dataclass
class Trade:
    tx_hash: str
    market_id: str
    outcome_id: str
    token_id: str
    side: str
    price: Decimal
    size: Decimal
    usdc_amount: Decimal
    timestamp: int


@dataclass
class OrderResult:
    success: bool
    tx_hash: str | None
    filled_price: Decimal | None
    filled_size: Decimal | None
    error: str | None = None


@dataclass
class PositionInfo:
    proxy_wallet: str
    asset: str
    condition_id: str
    size: Decimal
    avg_price: Decimal
    initial_value: Decimal
    current_value: Decimal
    cash_pnl: Decimal
    percent_pnl: float
    cur_price: Decimal
    redeemable: bool
    mergeable: bool
    title: str
    outcome: str
    negative_risk: bool