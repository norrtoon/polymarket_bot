"""
Пропорциональная продажа вслед за трейдером.

Бот продаёт ту же ДОЛЮ своей позиции, что и трейдер своей. Главные
риски, которые здесь проверяются:
  * выход частями не должен выбрасывать бота на первой же продаже;
  * после продажи не должна оставаться "пыль" меньше минимума рынка —
    её уже никогда не продать, и она повиснет до разрешения рынка.
"""
from decimal import Decimal as D

MIN = D("5")


def proportional(our_total, trader_sold, trader_after, min_shares=MIN):
    """Та же логика, что в TraderService._proportional_sell_shares."""
    if trader_after is None:
        return our_total
    before = trader_after + trader_sold
    fraction = trader_sold / before if before > 0 else D("1")
    fraction = min(max(fraction, D("0")), D("1"))
    if fraction >= D("0.97") or trader_after < D("0.01"):
        return our_total
    target = (our_total * fraction).quantize(D("0.01"))
    sell = max(target, min_shares)
    if our_total - sell < min_shares:
        return our_total
    return sell


def test_user_example_exit_in_three_parts():
    """Трейдер 150 долей выходит тремя частями по 50, у нас 20."""
    ours, trader = D("20"), D("150")
    sold_seq = []
    for _ in range(3):
        after = trader - D("50")
        s = proportional(ours, D("50"), after)
        sold_seq.append(s)
        ours -= s
        trader = after
    assert sold_seq[0] < D("20"), "на первой частичной продаже НЕ выходим целиком"
    assert ours == D("0"), "когда трейдер вышел полностью — вышли и мы"


def test_first_partial_sell_is_a_third():
    s = proportional(D("30"), D("50"), D("100"))
    assert s == D("10.00")


def test_full_exit():
    assert proportional(D("20"), D("150"), D("0")) == D("20")


def test_near_full_exit_treated_as_full():
    # 98% — выходим целиком, иначе останется крошечный непродаваемый хвост
    assert proportional(D("20"), D("98"), D("2")) == D("20")


def test_no_unsellable_dust_small_position():
    # позиция 8, треть: продали бы 5, осталось бы 3 < 5 -> продаём всё
    s = proportional(D("8"), D("50"), D("100"))
    left = D("8") - s
    assert left == 0 or left >= MIN


def test_no_unsellable_dust_general_case():
    # позиция 12, 60%: продали бы 7.2, осталось бы 4.8 < 5 -> продаём всё
    s = proportional(D("12"), D("60"), D("40"))
    left = D("12") - s
    assert left == 0 or left >= MIN


def test_tiny_fraction_rounded_up_to_minimum():
    # позиция 100, трейдер продал 2% -> 2 доли < минимума -> продаём 5
    assert proportional(D("100"), D("3"), D("147")) == MIN


def test_balance_unreadable_sells_everything():
    # Безопаснее выйти, чем остаться в рынке, из которого трейдер,
    # возможно, уже вышел.
    assert proportional(D("20"), D("50"), None) == D("20")


def test_never_sells_more_than_we_have():
    for our in (D("5"), D("7"), D("20"), D("500")):
        for frac_pct in (1, 10, 33, 50, 90, 99):
            sold = D(frac_pct)
            after = D(100 - frac_pct)
            assert proportional(our, sold, after) <= our


def fifo(positions, sold):
    """Распределение проданного по позициям от старых к новым."""
    rem, out = sold, []
    for pid, sh in positions:
        if rem <= 0:
            out.append((pid, sh, "open"))
        elif rem >= sh:
            out.append((pid, D("0"), "closed"))
            rem -= sh
        else:
            out.append((pid, sh - rem, "open"))
            rem = D("0")
    return out


def test_fifo_partial_does_not_close_everything():
    """
    Раньше продажа закрывала ВСЕ позиции по рынку — при частичной
    продаже оставшиеся доли повисли бы без TP/SL.
    """
    res = dict((p, (left, st)) for p, left, st in
               fifo([(1, D("6")), (2, D("4")), (3, D("10"))], D("8")))
    assert res[1] == (D("0"), "closed")
    assert res[2] == (D("2"), "open")
    assert res[3] == (D("10"), "open"), "новая позиция не тронута"