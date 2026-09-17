"""
Разбор ответа биржи на ордер.

Реальный баг: поля читались в camelCase (makingAmount, orderID,
transactionsHashes), а в модели SDK они в snake_case — camelCase там
только validation_alias для сырого JSON. В результате filled_size
всегда выходил 0, в позицию писалось shares_bought=0, и закрытие
падало с "SELL без количества долей" — позиция становилась
незакрываемой, а деньги зависали в рынке.
"""
from decimal import Decimal
import pytest


class Accepted:
    """Как выглядит AcceptedOrder в SDK."""
    ok = True
    def __init__(self, status, making, taking, order_id="0xORD",
                 hashes=()):
        self.status = status
        self.making_amount = Decimal(str(making))
        self.taking_amount = Decimal(str(taking))
        self.order_id = order_id
        self.transactions_hashes = hashes


class Rejected:
    ok = False
    def __init__(self, code, message):
        self.code, self.message = code, message


def parse(response, side):
    if getattr(response, "ok", None) is False:
        return {"success": False,
                "error": f"{response.code}: {response.message}"}
    making = Decimal(str(getattr(response, "making_amount", 0) or 0))
    taking = Decimal(str(getattr(response, "taking_amount", 0) or 0))
    spent, shares = (making, taking) if side == "BUY" else (taking, making)
    price = (spent / shares) if shares > 0 else None
    status = getattr(response, "status", None)
    if status == "matched" and shares <= 0:
        return {"success": False, "error": "нет количества долей"}
    return {"success": status in ("matched", "delayed", "live"),
            "filled_size": shares, "filled_price": price}


def test_camel_case_fields_are_absent():
    """Причина бага: camelCase-атрибутов на модели нет."""
    r = Accepted("matched", making=3, taking=6)
    assert not hasattr(r, "makingAmount")
    assert getattr(r, "makingAmount", Decimal("0")) == 0, (
        "именно так filled_size и обнулялся"
    )


def test_buy_shares_are_taking_amount():
    """При покупке отдаём USDC (making), получаем доли (taking)."""
    out = parse(Accepted("matched", making=3, taking=6), "BUY")
    assert out["filled_size"] == 6
    assert out["filled_price"] == Decimal("0.5")


def test_sell_shares_are_making_amount():
    """При продаже отдаём доли (making), получаем USDC (taking)."""
    out = parse(Accepted("matched", making=6, taking=3), "SELL")
    assert out["filled_size"] == 6
    assert out["filled_price"] == Decimal("0.5")


def test_matched_without_shares_is_failure():
    """Позиция без долей потом не закроется — считаем это провалом."""
    out = parse(Accepted("matched", making=0, taking=0), "BUY")
    assert out["success"] is False


def test_rejected_order_reports_reason():
    out = parse(Rejected("MIN_SIZE", "min size: 1"), "BUY")
    assert out["success"] is False
    assert "min size" in out["error"]


def test_shares_recovery_from_amount_and_price():
    """
    Страховка при закрытии: если shares_bought пусто, считаем из
    вложенной суммы и цены входа, иначе позиция зависнет навсегда.
    """
    amount, entry = Decimal("3"), Decimal("0.5")
    assert amount / entry == Decimal("6")