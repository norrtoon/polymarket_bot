import random
from dataclasses import dataclass
from decimal import Decimal

from simulation.mock_data import generate_mock_trades


@dataclass
class SimulationResult:
    total_trades: int
    winning_trades: int
    losing_trades: int
    tp_hits: int
    sl_hits: int
    total_pnl_usdc: Decimal
    total_pnl_percent: float
    max_drawdown: float
    avg_delay_ms: float
    win_rate: float


class TradingSimulator:
    def __init__(self, seed: int | None = None):
        if seed is not None:
            random.seed(seed)

    async def run_simulation(
        self, bet_amount: Decimal, tp_percent: float | None, sl_percent: float | None, num_trades: int = 50,
    ) -> SimulationResult:
        trades = generate_mock_trades(num_trades)

        total_pnl = Decimal("0")
        wins = losses = tp_hits = sl_hits = 0
        delays = []
        equity_curve = [Decimal("0")]

        for t in trades:
            if t.side != "BUY":
                continue
            entry_price = t.price
            delays.append(random.uniform(300, 2500))

            price = entry_price
            outcome_price = entry_price
            for _ in range(random.randint(1, 30)):
                change = Decimal(str(random.uniform(-0.05, 0.05)))
                price = max(Decimal("0.01"), min(Decimal("0.99"), price + change))
                pnl_pct = float((price - entry_price) / entry_price * 100)

                if tp_percent and pnl_pct >= tp_percent:
                    outcome_price = entry_price * (1 + Decimal(str(tp_percent)) / 100)
                    tp_hits += 1
                    break
                if sl_percent and pnl_pct <= -sl_percent:
                    outcome_price = entry_price * (1 - Decimal(str(sl_percent)) / 100)
                    sl_hits += 1
                    break
                outcome_price = price

            pnl_usdc = (outcome_price - entry_price) / entry_price * bet_amount
            total_pnl += pnl_usdc
            equity_curve.append(equity_curve[-1] + pnl_usdc)
            wins += 1 if pnl_usdc > 0 else 0
            losses += 1 if pnl_usdc <= 0 else 0

        total_trades = wins + losses
        max_dd = self._max_drawdown(equity_curve)
        win_rate = (wins / total_trades * 100) if total_trades else 0.0
        avg_delay = sum(delays) / len(delays) if delays else 0.0
        total_pnl_percent = float(total_pnl / (bet_amount * total_trades) * 100) if total_trades else 0.0

        return SimulationResult(
            total_trades=total_trades, winning_trades=wins, losing_trades=losses,
            tp_hits=tp_hits, sl_hits=sl_hits, total_pnl_usdc=total_pnl,
            total_pnl_percent=total_pnl_percent, max_drawdown=max_dd,
            avg_delay_ms=avg_delay, win_rate=win_rate,
        )

    def _max_drawdown(self, equity_curve: list[Decimal]) -> float:
        peak = equity_curve[0]
        max_dd = Decimal("0")
        for value in equity_curve:
            if value > peak:
                peak = value
            dd = peak - value
            if dd > max_dd:
                max_dd = dd
        return float(max_dd)