import pytest
from decimal import Decimal
from services.trader import TraderService
from models.user import User


def make_user(**kwargs) -> User:
    defaults = dict(
        id=1, bet_amount=Decimal("10"), bet_mode="fixed", bet_percent=5.0,
        tp_percent=15.0, sl_percent=8.0, is_active=True, proxy_wallet=None,
    )
    defaults.update(kwargs)
    return User(**defaults)


@pytest.mark.asyncio
async def test_calculate_amount_fixed():
    trader = TraderService()
    user = make_user(bet_mode="fixed", bet_amount=Decimal("25"))
    result = await trader._calculate_amount(user)
    assert result == Decimal("25")


@pytest.mark.asyncio
async def test_calculate_amount_percent_no_proxy_wallet():
    trader = TraderService()
    user = make_user(bet_mode="percent", bet_percent=10.0, proxy_wallet=None)
    result = await trader._calculate_amount(user)
    assert result == user.bet_amount


def test_calculate_tp_sl_prices():
    trader = TraderService()
    user = make_user(tp_percent=20.0, sl_percent=10.0)
    entry = Decimal("0.5")
    tp, sl = trader._calculate_tp_sl_prices(entry, user)
    assert tp == Decimal("0.6")
    assert sl == Decimal("0.45")


def test_calculate_tp_sl_none():
    trader = TraderService()
    user = make_user(tp_percent=None, sl_percent=None)
    tp, sl = trader._calculate_tp_sl_prices(Decimal("0.5"), user)
    assert tp is None and sl is None