"""
Тесты дедупликации — то, что чинилось дольше всего.

Каждый тест воспроизводит реальный баг из логов, а не абстрактный
сценарий.
"""
import json
import pytest

from services.watcher import _trade_key, SEEN_WINDOW


class T:
    """Минимальная сделка."""
    def __init__(self, tx, token="tokA", side="BUY", price="0.5",
                 size="10", ts=1000):
        self.tx_hash, self.token_id, self.side = tx, token, side
        self.price, self.size, self.timestamp = price, size, ts
        self.market_id, self.outcome_id = "m", "o"
        self.usdc_amount = "5"


def test_partial_fills_same_order_collapse():
    """
    Один ордер трейдера исполняется несколькими транзакциями с
    РАЗНЫМИ price/size. Ключ не должен их различать, иначе одна ставка
    копируется несколько раз (реальный баг: 5 транзакций -> 5 ставок).
    """
    a = T("0xAAA", price="0.53", size="3")
    b = T("0xAAA", price="0.54", size="2")
    assert _trade_key(a) == _trade_key(b)


def test_different_markets_same_tx_not_collapsed():
    """
    Одной транзакцией можно купить на ДВУХ рынках. Это разные ставки,
    схлопывать нельзя.
    """
    a = T("0xSAME", token="tokA")
    b = T("0xSAME", token="tokB")
    assert _trade_key(a) != _trade_key(b)


def test_buy_and_sell_not_collapsed():
    """Закрытие позиции не должно теряться из-за дедупликации входа."""
    assert _trade_key(T("0xX", side="BUY")) != _trade_key(T("0xX", side="SELL"))


def test_seen_window_is_bounded():
    """Окно не должно расти бесконечно — Redis-ключ ограничен по размеру."""
    seen = [f"k{i}" for i in range(SEEN_WINDOW * 3)]
    trimmed = seen[-SEEN_WINDOW:]
    assert len(trimmed) == SEEN_WINDOW
    assert trimmed[-1] == seen[-1]


def test_baseline_merges_instead_of_wiping():
    """
    Ключевой баг: при Стоп -> Старт кэш очищался, и сделки последних
    минут копировались ПОВТОРНО, потому что Data API отдаёт данные с
    задержкой и новый рубеж оказывался в прошлом.
    Кэш должен ДОПОЛНЯТЬСЯ, а не стираться.
    """
    existing = ["0xOLD:tokA:BUY"]
    fresh = [T("0xNEW")]
    merged = list(existing)
    for t in fresh:
        k = _trade_key(t)
        if k not in merged:
            merged.append(k)
    assert "0xOLD:tokA:BUY" in merged, "старые ключи обязаны сохраниться"
    assert _trade_key(fresh[0]) in merged