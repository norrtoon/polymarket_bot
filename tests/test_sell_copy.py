"""
Копирование продажи вслед за трейдером.

Реальный баг: бот покупал за кошельком, но не продавал. Причины было
две, и обе срабатывали до отправки ордера.
"""
from decimal import Decimal
import pytest


# ---------------------------------------------------------------- #
# 1. Проверки покупки применялись к продаже
# ---------------------------------------------------------------- #

def preflight(side, amount, free, open_count, max_open):
    """Упрощённая модель проверок."""
    if side != "BUY":
        return None                      # для продажи они не применяются
    if free > 0 and free < amount:
        return "недостаточно средств"
    if open_count >= max_open:
        return "лимит открытых позиций"
    return None


def test_sell_not_blocked_by_position_limit():
    """
    Главная причина: при MAX_OPEN_POSITIONS=1 и одной открытой позиции
    лимит отклонял ЛЮБУЮ следующую сделку — включая продажу, которая
    эту позицию и закрыла бы. Бот запирал сам себя.
    """
    assert preflight("SELL", Decimal("3"), Decimal("0"), 1, 1) is None
    # для покупки лимит по-прежнему работает
    assert preflight("BUY", Decimal("3"), Decimal("10"), 1, 1) is not None


def test_sell_not_blocked_by_balance():
    """При продаже свободные USDC не нужны — деньги мы получаем."""
    assert preflight("SELL", Decimal("3"), Decimal("0.1"), 0, 5) is None
    assert preflight("BUY", Decimal("3"), Decimal("0.1"), 0, 5) is not None


# ---------------------------------------------------------------- #
# 2. Продажа уходила без количества долей
# ---------------------------------------------------------------- #

class Pos:
    def __init__(self, shares, amount="3", entry="0.5"):
        self.shares_bought = Decimal(shares) if shares is not None else None
        self.amount_usdc = Decimal(amount)
        self.entry_price = Decimal(entry)


def open_shares(positions):
    total = Decimal("0")
    for p in positions:
        sh = Decimal(str(p.shares_bought or 0))
        if sh <= 0 and p.entry_price > 0:
            sh = p.amount_usdc / p.entry_price
        total += sh
    return total


def test_sell_uses_actual_shares():
    assert open_shares([Pos("6")]) == Decimal("6")


def test_reentries_are_summed():
    """Перезаходы создают несколько позиций — при выходе трейдера
    закрывать нужно весь объём."""
    assert open_shares([Pos("6"), Pos("4"), Pos("2")]) == Decimal("12")


def test_shares_recovered_when_missing():
    """Если shares_bought потерялось, считаем из суммы и цены входа."""
    assert open_shares([Pos(None, amount="3", entry="0.5")]) == Decimal("6")


def test_no_position_means_nothing_to_copy():
    assert open_shares([]) == Decimal("0")


# ---------------------------------------------------------------- #
# 3. Сторона сделки не угадывается
# ---------------------------------------------------------------- #

def require_side(item):
    raw = (item.get("side") or "").strip().upper()
    if raw in ("BUY", "SELL"):
        return raw
    raise ValueError("нет корректного side")


def test_side_parsed_strictly():
    assert require_side({"side": "sell"}) == "SELL"
    assert require_side({"side": "BUY"}) == "BUY"


def test_missing_side_raises_instead_of_defaulting_to_buy():
    """
    Подстановка "BUY" по умолчанию превращала бы продажу трейдера в
    покупку: бот докупал бы там, где нужно выходить.
    """
    for bad in ({}, {"side": None}, {"side": ""}, {"side": "???"}):
        with pytest.raises(ValueError):
            require_side(bad)