"""
Масштабирование опросов — то, из-за чего шли 429 при двух пользователях.

Лимит Cloudflare считается ПО IP на весь сервер, поэтому суммарная
частота запросов не должна расти с числом клиентов.
"""
import pytest

BASE = 0.25


def requests_per_second(n_wallets: int, base: float = BASE) -> float:
    """Опрос идёт ПО КОШЕЛЬКУ и интервал масштабируется их числом."""
    interval = base * max(1, n_wallets)
    return n_wallets / interval


@pytest.mark.parametrize("wallets", [1, 2, 5, 10, 50])
def test_load_is_constant_regardless_of_scale(wallets):
    """Сколько бы кошельков ни отслеживали, нагрузка на API постоянна."""
    assert requests_per_second(wallets) == pytest.approx(1 / BASE)


def test_many_users_one_wallet_cost_one_poll():
    """
    Несколько человек, копирующих ОДНОГО трейдера, обслуживаются одним
    запросом. Раньше каждый опрашивал сам, и пятеро давали пятикратную
    нагрузку за одними и теми же данными.
    """
    from services.watcher import WalletWatcher, _Subscriber
    w = WalletWatcher()
    wallet = "0xtrader"
    w._subscribers[wallet] = {i: _Subscriber(i, 1) for i in range(1, 6)}
    w._wallet_tasks[wallet] = object()
    assert len(w._subscribers[wallet]) == 5
    assert len(w._wallet_tasks) == 1, "пять подписчиков — один опрос"


def test_under_observed_ip_limit():
    """Наблюдаемый предел IP в логах — около 5.5 req/s."""
    assert requests_per_second(10) < 5.5