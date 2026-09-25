"""
Несколько пользователей на ОДНОМ отслеживаемом кошельке.

Проверяется: общий опрос (один запрос на кошелёк, а не на пользователя),
у каждого свои дедупликация, фильтр мелких сделок, режим докупки и
ставка — и данные одного не утекают к другому.

Отдельно — регрессия: сумма, поднятая до минимума рынка, хранилась по
ключу "рынок" без пользователя, и при одновременных сделках один
пользователь мог забрать чужую сумму.
"""
import asyncio
from decimal import Decimal as D


class Model:
    def __init__(self):
        self.subs, self.polls = {}, 0
        self.seen, self.orders = {}, []
        self.bumped, self.positions, self.claims = {}, {}, {}

    def subscribe(self, user, wallet, **st):
        self.subs.setdefault(wallet, {})[user] = st

    async def poll(self, wallet, trades):
        self.polls += 1
        await asyncio.gather(*[
            self._dispatch(u, wallet, trades, st)
            for u, st in self.subs[wallet].items()
        ])

    async def _dispatch(self, u, wallet, trades, st):
        seen = self.seen.setdefault((u, wallet), set())
        for tx, token, usdc in trades:
            if tx in seen:
                continue
            seen.add(tx)
            await self._execute(u, token, D(usdc), st)

    async def _execute(self, u, token, trader_usdc, st):
        if st["min_trade"] and trader_usdc < st["min_trade"]:
            return
        k = (u, token)
        if not st["reentry"]:
            if k in self.claims or self.positions.get(k, 0):
                return
            self.claims[k] = asyncio.current_task()
        try:
            amount = st["bet"]
            if amount < 5:
                self.bumped[k] = D(5)          # ключ (пользователь, рынок)
            await asyncio.sleep(0.002)
            amount = self.bumped.pop(k, amount)
            self.orders.append((u, token, amount))
            self.positions[k] = self.positions.get(k, 0) + 1
        finally:
            if self.claims.get(k) is asyncio.current_task():
                self.claims.pop(k)


TRADES = [("tx1", "BTC", 500), ("tx2", "BTC", 1),
          ("tx3", "ETH", 800), ("tx4", "BTC", 300)]


def _run():
    m = Model()
    m.subscribe("A", "0xw", bet=D(10), reentry=True, min_trade=D(0))
    m.subscribe("B", "0xw", bet=D(3), reentry=False, min_trade=D(20))

    async def go():
        await m.poll("0xw", TRADES)
        await m.poll("0xw", TRADES)
    asyncio.run(go())
    return m


def test_one_request_per_wallet_not_per_user():
    assert _run().polls == 2


def test_no_duplicates_on_repeated_poll():
    m = _run()
    assert len([o for o in m.orders if o[0] == "A"]) == 4


def test_min_trade_filter_is_per_user():
    b = [o for o in _run().orders if o[0] == "B"]
    # долларовая докупка tx2 отсечена фильтром только у B
    assert all(o[1] in ("BTC", "ETH") for o in b)
    assert len(b) == 2


def test_reentry_mode_is_per_user():
    m = _run()
    assert sum(1 for o in m.orders if o[0] == "A" and o[1] == "BTC") == 3
    assert sum(1 for o in m.orders if o[0] == "B" and o[1] == "BTC") == 1


def test_bumped_amount_does_not_leak_between_users():
    m = _run()
    assert all(o[2] == D(10) for o in m.orders if o[0] == "A"), \
        "ставка A не должна подменяться чужой поднятой суммой"
    assert all(o[2] == D(5) for o in m.orders if o[0] == "B")


def test_no_leftover_claims():
    assert _run().claims == {}