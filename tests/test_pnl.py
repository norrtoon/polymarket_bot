"""Расчёт итогов по позиции."""
from decimal import Decimal
import pytest


def compute(entry, invested, reason, exit_price=None, won=None,
            shares=None):
    entry = Decimal(str(entry))
    invested = Decimal(str(invested))
    shares = Decimal(str(shares)) if shares else Decimal("0")
    if shares <= 0 and entry > 0:
        shares = invested / entry
    if reason == "resolved":
        payout = shares if won else Decimal("0")
    else:
        payout = shares * Decimal(str(exit_price or entry))
    pnl = payout - invested
    pct = (pnl / invested * 100) if invested > 0 else Decimal("0")
    return payout, pnl, pct


def test_resolved_win_pays_one_per_share():
    """Выигравший исход гасится по 1 USDC за долю."""
    payout, pnl, pct = compute("0.25", "0.095", "resolved", won=True)
    assert payout == pytest.approx(Decimal("0.38"), abs=Decimal("0.001"))
    assert pnl > 0
    assert pct == pytest.approx(Decimal("300"), abs=Decimal("0.1"))


def test_resolved_loss_is_total():
    payout, pnl, pct = compute("0.25", "0.095", "resolved", won=False)
    assert payout == 0
    assert pnl == Decimal("-0.095")
    assert pct == pytest.approx(Decimal("-100"))


def test_take_profit_is_positive():
    _, pnl, _ = compute("0.92", "0.095", "tp", exit_price="0.999")
    assert pnl > 0


def test_stop_loss_is_negative():
    _, pnl, _ = compute("0.3669", "0.095", "sl", exit_price="0.2201")
    assert pnl < 0


def test_zero_entry_does_not_divide_by_zero():
    payout, pnl, pct = compute("0", "10", "sell", exit_price="0.5")
    assert payout == 0 and pct == pytest.approx(Decimal("-100"))