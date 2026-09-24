"""
Режим «без докупки»: один вход на рынок.

Главный риск — параллельные входы. Трейдер часто делает несколько
покупок почти одновременно (в одном случае было 64 сделки на одном
рынке за 5 минут). Без атомарной отметки все параллельные обработчики
увидели бы "позиции ещё нет" и вошли бы.

Отдельно проверяется ошибка, найденная при разработке: отметку снимал
любой завершившийся обработчик, в том числе тот, которому отказали. На
пяти одновременных входах это давало ТРИ ордера вместо одного.
"""
import asyncio


class SingleEntry:
    """Та же логика, что в TraderService._claim_single_entry."""

    def __init__(self):
        self.claims, self.db, self.orders = {}, {}, 0

    async def _count(self, u, t):
        await asyncio.sleep(0.001)
        return self.db.get((u, t), 0)

    async def _claim(self, u, t):
        k = (u, t)
        if k in self.claims:
            return False
        self.claims[k] = asyncio.current_task()
        if await self._count(u, t) > 0:
            self.claims.pop(k, None)
            return False
        return True

    def _release(self, u, t):
        k = (u, t)
        if self.claims.get(k) is asyncio.current_task():
            self.claims.pop(k, None)

    async def execute(self, u, t, ok=True):
        try:
            if not await self._claim(u, t):
                return
            await asyncio.sleep(0.005)
            if ok:
                self.orders += 1
                self.db[(u, t)] = self.db.get((u, t), 0) + 1
        finally:
            self._release(u, t)


def run(coro):
    return asyncio.run(coro)


def test_burst_gives_exactly_one_entry():
    for n in (2, 5, 20, 64):
        m = SingleEntry()

        async def go():
            await asyncio.gather(*[m.execute(1, "mkt") for _ in range(n)])
        run(go())
        assert m.orders == 1, f"{n} одновременных входов дали {m.orders} ордеров"


def test_rejected_handler_does_not_release_foreign_claim():
    """Регрессия: без проверки владельца 5 входов давали 3 ордера."""
    m = SingleEntry()

    async def go():
        await asyncio.gather(*[m.execute(1, "mkt") for _ in range(5)])
    run(go())
    assert m.orders == 1


def test_failed_order_does_not_lock_market():
    m = SingleEntry()

    async def go():
        await m.execute(1, "mkt", ok=False)
        await m.execute(1, "mkt", ok=True)
    run(go())
    assert m.orders == 1, "после неудачного ордера следующий вход должен пройти"


def test_new_entry_after_position_closed():
    m = SingleEntry()

    async def go():
        await m.execute(1, "mkt")
        m.db[(1, "mkt")] = 0          # позиция закрыта
        await m.execute(1, "mkt")
    run(go())
    assert m.orders == 2


def test_markets_and_users_independent():
    m = SingleEntry()

    async def go():
        await asyncio.gather(
            m.execute(1, "A"), m.execute(1, "B"), m.execute(2, "A"),
        )
    run(go())
    assert m.orders == 3


def test_no_claims_leak():
    """После всех операций отметок не остаётся — иначе рынки запирались бы."""
    m = SingleEntry()

    async def go():
        await asyncio.gather(*[m.execute(1, "mkt") for _ in range(10)])
    run(go())
    assert m.claims == {}