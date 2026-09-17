import pytest
from decimal import Decimal
from simulation.simulator import TradingSimulator


@pytest.mark.asyncio
async def test_simulation_basic():
    sim = TradingSimulator(seed=42)
    result = await sim.run_simulation(bet_amount=Decimal("10"), tp_percent=15.0, sl_percent=8.0, num_trades=100)
    assert result.total_trades > 0
    assert result.avg_delay_ms < 3000
    assert isinstance(result.total_pnl_usdc, Decimal)
    assert 0 <= result.win_rate <= 100


@pytest.mark.asyncio
async def test_simulation_no_tp_sl():
    sim = TradingSimulator(seed=1)
    result = await sim.run_simulation(bet_amount=Decimal("10"), tp_percent=None, sl_percent=None, num_trades=30)
    assert result.tp_hits == 0
    assert result.sl_hits == 0