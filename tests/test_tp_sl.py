from decimal import Decimal
from services.tp_sl_monitor import TpSlMonitor


def test_pnl_percent_positive():
    m = TpSlMonitor()
    assert abs(m._calculate_pnl_percent(Decimal("0.5"), Decimal("0.6")) - 20.0) < 0.001


def test_pnl_percent_negative():
    m = TpSlMonitor()
    assert abs(m._calculate_pnl_percent(Decimal("0.5"), Decimal("0.4")) - (-20.0)) < 0.001


def test_pnl_percent_zero_entry():
    m = TpSlMonitor()
    assert m._calculate_pnl_percent(Decimal("0"), Decimal("0.5")) == 0.0