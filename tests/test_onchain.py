"""
Расшифровка события OrderFilled из блокчейна.

Ошибка в любом правиле здесь означает, что бот ПОКУПАЕТ, когда трейдер
ПРОДАЁТ. Поэтому проверены все четыре комбинации:
    трейдер мейкер/тейкер  x  сторона мейкера BUY/SELL
"""
from decimal import Decimal

from services.onchain_watcher import (
    ORDER_FILLED_TOPIC, decode_order_filled, _addr_topic,
)

WHALE = "0x1111111111111111111111111111111111111111"
OTHER = "0x2222222222222222222222222222222222222222"
TOKEN = 123456789


def word(n: int) -> str:
    return format(n, "064x")


def make_log(maker, taker, side, maker_amt, taker_amt, removed=False):
    data = "0x" + "".join([
        word(side), word(TOKEN),
        word(int(maker_amt * 10**6)), word(int(taker_amt * 10**6)),
        word(0), word(0), word(0),        # fee, builder, metadata
    ])
    return {
        "topics": [ORDER_FILLED_TOPIC, "0x" + "ab" * 32,
                   _addr_topic(maker), _addr_topic(taker)],
        "data": data,
        "transactionHash": "0xdeadbeef",
        "removed": removed,
    }


def test_topic_is_v2_not_v1():
    # Хэш V1-сигнатуры — частая ошибка в статьях. С ним подписка молчит.
    assert ORDER_FILLED_TOPIC == (
        "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
    )


def test_whale_maker_buying():
    # Мейкер покупает: платит pUSD (makerAmount), получает доли
    t = decode_order_filled(make_log(WHALE, OTHER, 0, 5, 10), WHALE)
    assert t.side == "BUY"
    assert t.size == Decimal("10")
    assert t.price == Decimal("0.5")


def test_whale_maker_selling():
    # Мейкер продаёт: отдаёт доли (makerAmount), получает pUSD
    t = decode_order_filled(make_log(WHALE, OTHER, 1, 10, 5), WHALE)
    assert t.side == "SELL"
    assert t.size == Decimal("10")
    assert t.price == Decimal("0.5")


def test_whale_taker_against_seller():
    # Мейкер ПРОДАЁТ -> трейдер-тейкер ПОКУПАЕТ (противоположная сторона)
    t = decode_order_filled(make_log(OTHER, WHALE, 1, 10, 5), WHALE)
    assert t.side == "BUY", "тейкер против продавца — это покупка"
    assert t.size == Decimal("10")


def test_whale_taker_against_buyer():
    # Мейкер ПОКУПАЕТ -> трейдер-тейкер ПРОДАЁТ
    t = decode_order_filled(make_log(OTHER, WHALE, 0, 5, 10), WHALE)
    assert t.side == "SELL", "тейкер против покупателя — это продажа"


def test_unrelated_wallet_ignored():
    log = make_log(OTHER, OTHER, 0, 5, 10)
    assert decode_order_filled(log, WHALE) is None


def test_reorged_block_ignored():
    log = make_log(WHALE, OTHER, 0, 5, 10, removed=True)
    assert decode_order_filled(log, WHALE) is None


def test_impossible_price_rejected():
    # Цена > 1 невозможна — значит расшифровка разошлась с реальностью
    log = make_log(WHALE, OTHER, 0, 50, 10)   # 5.0 за долю
    assert decode_order_filled(log, WHALE) is None


def test_address_case_insensitive():
    t = decode_order_filled(
        make_log(WHALE.upper().replace("0X", "0x"), OTHER, 0, 5, 10),
        WHALE,
    )
    assert t is not None


# ---------------------------------------------------------------------
# Чеканка и сжигание комплекта: события с ПРОТИВОПОЛОЖНЫМ исходом
# ---------------------------------------------------------------------
#
# Реальный случай из лога: одна транзакция дала два события —
#   трейдер BUY  Up   по 0.52 (его собственный ордер, мейкер)
#   трейдер SELL Down по 0.48 (чужой ордер, трейдер в нём тейкер)
# 0.52 + 0.48 = 1.00: биржа свела покупку Up с чужой покупкой Down,
# выпустив полный комплект. Реальная сделка трейдера — только первая.
# Поэтому обработчик берёт ТОЛЬКО события, где кошелёк — мейкер.

UP, DOWN = 111, 222


def make_log_token(maker, taker, side, token, maker_amt, taker_amt):
    data = "0x" + "".join([
        word(side), word(token),
        word(int(maker_amt * 10**6)), word(int(taker_amt * 10**6)),
        word(0), word(0), word(0),
    ])
    return {
        "topics": [ORDER_FILLED_TOPIC, "0x" + "cd" * 32,
                   _addr_topic(maker), _addr_topic(taker)],
        "data": data, "transactionHash": "0xmint", "removed": False,
    }


def maker_only(logs, wallet):
    """То, что делает обработчик: берём событие, только если wallet — мейкер."""
    out = []
    for lg in logs:
        maker = "0x" + lg["topics"][2][-40:]
        if maker == wallet.lower():
            t = decode_order_filled(lg, wallet)
            if t:
                out.append((t.side, t.token_id))
    return out


EXCHANGE = "0xE111180000d2663C0091e4f400237545B87B996B"


def test_mint_does_not_create_phantom_sell():
    """Трейдер купил Up рыночным, биржа свела с чужой покупкой Down."""
    logs = [
        make_log_token(WHALE, EXCHANGE, 0, UP, 0.52, 1.0),   # свой ордер
        make_log_token(OTHER, WHALE, 0, DOWN, 0.48, 1.0),    # чужой, WHALE — тейкер
    ]
    assert maker_only(logs, WHALE) == [("BUY", str(UP))], (
        "должна быть только покупка Up, без фантомной продажи Down"
    )


def test_burn_does_not_create_phantom_buy_of_opposite():
    """
    Самый опасный случай: трейдер ПРОДАЛ Up, биржа свела с чужой
    продажей Down через сжигание. Старое правило давало фантомную
    ПОКУПКУ Down — бот купил бы противоположный исход.
    """
    logs = [
        make_log_token(WHALE, EXCHANGE, 1, UP, 1.0, 0.52),   # свой: SELL Up
        make_log_token(OTHER, WHALE, 1, DOWN, 1.0, 0.48),    # чужой: SELL Down
    ]
    result = maker_only(logs, WHALE)
    assert ("BUY", str(DOWN)) not in result, "фантомная покупка противоположного исхода!"
    assert result == [("SELL", str(UP))]