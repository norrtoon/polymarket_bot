"""
Отсечка устаревших сделок.

Реальный баг: после рестарта Data API отдал картину пятиминутной
давности, бот скопировал сделки по историческим ценам, и они мгновенно
(за 0.07-0.5с) закрылись по TP/SL с разрывом цены до 40%.
"""
import pytest

MAX_AGE = 120


def is_stale(trade_ts: int, batch_newest: int, max_age: int = MAX_AGE) -> bool:
    """Сравниваем ТОЛЬКО метки API между собой — локальные часы могут
    расходиться с сервером (у Docker Desktop на macOS это обычное дело)."""
    if not max_age:
        return False
    return (batch_newest - trade_ts) > max_age


@pytest.mark.parametrize("age,expected", [
    (0, False),      # только что
    (30, False),
    (120, False),    # ровно на границе
    (121, True),
    (313, True),     # реальный случай из лога: рестарт с 5-минутным лагом
    (600, True),
])
def test_staleness_threshold(age, expected):
    newest = 1_700_000_000
    assert is_stale(newest - age, newest) is expected


def test_clock_skew_does_not_affect_decision():
    """
    Часы контейнера у пользователя убегали на 65 минут. Решение не
    должно от этого зависеть вообще — сравниваются только метки API.
    """
    newest = 1_700_000_000
    trade = newest - 60
    # какое бы ни было локальное время, результат один
    assert is_stale(trade, newest) is False


def test_disabled_when_zero():
    assert is_stale(1, 10_000_000, max_age=0) is False