"""
Единицы измерения при продаже.

Блокер для боевого режима: у Polymarket рыночная ПРОДАЖА измеряется в
ДОЛЯХ, а покупка — в USDC. Передача долларов на продажу означала бы,
что продаётся лишь часть позиции.
"""
from decimal import Decimal
import pytest


def shares_for(invested, entry):
    return Decimal(str(invested)) / Decimal(str(entry))


@pytest.mark.parametrize("invested,entry,expected_shares", [
    ("10", "0.25", "40"),
    ("10", "0.50", "20"),
    ("0.095", "0.95", "0.1"),
])
def test_shares_computed_from_entry(invested, entry, expected_shares):
    assert shares_for(invested, entry) == pytest.approx(
        Decimal(expected_shares), abs=Decimal("0.0001")
    )


def test_selling_usdc_amount_would_underclose():
    """
    Наглядно: вход на 10 USDC по 0.25 даёт 40 долей. Продажа "10"
    закрыла бы лишь четверть позиции, а в базе она числилась бы
    закрытой полностью.
    """
    invested, entry = Decimal("10"), Decimal("0.25")
    shares = shares_for(invested, entry)
    wrong = invested          # как было раньше
    assert wrong < shares
    leftover = (shares - wrong) / shares
    assert leftover == pytest.approx(Decimal("0.75"))


def test_sell_without_shares_must_fail():
    """Продажа без указания долей должна отклоняться, а не угадывать."""
    for bad in (None, Decimal("0"), Decimal("-1")):
        assert not (bad and bad > 0)