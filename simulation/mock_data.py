import random
import time
from decimal import Decimal
from dataclasses import dataclass


@dataclass
class MockTrade:
    tx_hash: str
    token_id: str
    side: str
    price: Decimal
    size: Decimal
    timestamp: int


def generate_mock_trades(count: int = 50) -> list[MockTrade]:
    trades = []
    tokens = [f"token_{i}" for i in range(5)]
    ts = int(time.time())
    for i in range(count):
        token = random.choice(tokens)
        side = random.choice(["BUY", "SELL"])
        price = Decimal(str(round(random.uniform(0.1, 0.9), 3)))
        size = Decimal(str(round(random.uniform(5, 50), 2)))
        trades.append(MockTrade(
            tx_hash=f"mock_{i}", token_id=token, side=side,
            price=price, size=size, timestamp=ts + i * 30,
        ))
    return trades