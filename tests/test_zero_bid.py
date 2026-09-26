"""
Ложные стоп-лоссы при закрытии короткого рынка.

Реальный случай: несколько стопов подряд ровно на границах пятиминуток
(17:14:59, 23:54:59 — два в одну секунду на разных рынках), в отчёте
"выход = вход, 0%". Причина: при закрытии рынка все заявки на покупку
снимаются, и биржа передаёт лучший бид как "0". Бот читал это как
"цена упала до нуля" — 0 меньше любого стоп-уровня.
"""
from decimal import Decimal

from services.tp_sl_monitor import _extract_exit_price


def test_zero_bid_is_not_a_price_best_bid_ask():
    ev = {"event_type": "best_bid_ask", "best_bid": "0", "best_ask": "1"}
    assert _extract_exit_price(ev) is None, "пустой бид — не цена 0"


def test_zero_bid_is_not_a_price_price_change():
    ev = {"event_type": "price_change",
          "price_changes": [{"best_bid": "0", "best_ask": "0.99"}]}
    assert _extract_exit_price(ev) is None


def test_empty_book_is_not_a_price():
    assert _extract_exit_price({"event_type": "book", "bids": []}) is None


def test_zero_bids_in_book_ignored():
    ev = {"event_type": "book", "bids": [{"price": "0"}, {"price": "0.41"}]}
    assert _extract_exit_price(ev) == Decimal("0.41")


def test_real_bid_still_works():
    ev = {"event_type": "best_bid_ask", "best_bid": "0.43", "best_ask": "0.45"}
    assert _extract_exit_price(ev) == Decimal("0.43")


def test_real_low_price_still_triggers():
    # Настоящее падение до 0.01 — это цена, и стоп должен сработать
    ev = {"event_type": "best_bid_ask", "best_bid": "0.01", "best_ask": "0.03"}
    assert _extract_exit_price(ev) == Decimal("0.01")


# ---------------------------------------------------------------------
# Очистка стакана в начале спортивного матча
# ---------------------------------------------------------------------
from services.tp_sl_monitor import _bid_side_emptied


def test_emptied_book_detected_best_bid_ask():
    assert _bid_side_emptied({"event_type": "best_bid_ask", "best_bid": "0"})


def test_emptied_book_detected_book_event():
    assert _bid_side_emptied({"event_type": "book", "bids": []})


def test_normal_bid_is_not_emptied():
    assert not _bid_side_emptied(
        {"event_type": "best_bid_ask", "best_bid": "0.42"}
    )


def test_missing_field_is_not_emptied():
    # Событие без поля бида — это "нет информации", а не "бид пропал"
    assert not _bid_side_emptied({"event_type": "best_bid_ask"})