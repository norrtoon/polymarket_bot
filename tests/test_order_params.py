"""
Параметры боевого ордера.

Реальный баг: в SDK OrderSide и OrderType — это Literal-алиасы, а не
enum. Обращение OrderSide.BUY давало AttributeError со строкой 'BUY',
и боевой ордер падал ещё до отправки на биржу (в логе это выглядело
как "place_market_order failed: BUY").
"""
from typing import Literal
import pytest


def test_literal_alias_has_no_enum_attribute():
    """Воспроизводит саму причину: у Literal нет атрибута .BUY."""
    OrderSide = Literal["BUY", "SELL"]
    with pytest.raises(AttributeError) as exc:
        _ = OrderSide.BUY
    assert str(exc.value) == "BUY", (
        "именно эта строка и попадала в лог как текст ошибки"
    )


def build_order_kwargs(side: str, amount_usdc, shares):
    """Как формируются параметры сейчас."""
    order_side = "BUY" if side == "BUY" else "SELL"
    if order_side == "SELL":
        if shares is None or shares <= 0:
            return None
        return {"side": "SELL", "shares": float(shares)}
    return {
        "side": "BUY",
        "amount": float(amount_usdc),
        "max_spend": float(amount_usdc),
    }


def test_buy_uses_amount_not_shares():
    """BUY в SDK принимает amount (сколько потратить)."""
    kw = build_order_kwargs("BUY", 5, None)
    assert kw["side"] == "BUY"
    assert kw["amount"] == 5
    assert "shares" not in kw, "shares — параметр только для SELL"


def test_sell_uses_shares_not_amount():
    """SELL в SDK принимает shares (сколько долей продать)."""
    kw = build_order_kwargs("SELL", 5, 40)
    assert kw["side"] == "SELL"
    assert kw["shares"] == 40
    assert "amount" not in kw, "amount — параметр только для BUY"
    assert "max_spend" not in kw


def test_sell_without_shares_rejected():
    for bad in (None, 0, -1):
        assert build_order_kwargs("SELL", 5, bad) is None


def test_side_is_plain_string():
    """Строка, а не enum — иначе SDK не примет."""
    for side in ("BUY", "SELL"):
        kw = build_order_kwargs(side, 5, 40)
        assert isinstance(kw["side"], str)
        assert kw["side"] in ("BUY", "SELL")