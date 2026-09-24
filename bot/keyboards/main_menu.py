from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardMarkup
from models.user import User


def main_menu_kb(user: User) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="💼 Кошелёк", callback_data="menu:wallet")
    b.button(text="💰 Ставка", callback_data="menu:amount")
    b.button(text="🎯 TP/SL", callback_data="menu:tpsl")
    # Кнопки "Позиции" и "История" убраны намеренно: итоги по каждой
    # сыгравшей ставке бот присылает сам, отдельным сообщением.
    b.button(text="⚖️ Баланс", callback_data="menu:balance")
    # Переключатель докупки: состояние видно прямо на кнопке
    reentry_on = getattr(user, "allow_reentry", True)
    b.button(
        text="🔁 Докупка: ВКЛ" if reentry_on else "1️⃣ Докупка: ВЫКЛ",
        callback_data="menu:toggle_reentry",
    )
    if user.is_active:
        b.button(text="⏹️ Стоп", callback_data="menu:stop")
    else:
        b.button(text="▶️ Старт", callback_data="menu:startcopy")
    b.adjust(2)
    return b.as_markup()


def status_text(user: User) -> str:
    status = "✅ Активен" if user.is_active else "⏸️ Остановлен"
    wallet = user.target_wallet or "не установлен"
    tp = f"{user.tp_percent}%" if user.tp_percent else "—"
    sl = f"{user.sl_percent}%" if user.sl_percent else "—"
    mode = f"{user.bet_amount} USDC" if user.bet_mode == "fixed" else f"{user.bet_percent}% от капитала"
    reentry = (
        "включена — копируются все входы трейдера"
        if getattr(user, "allow_reentry", True)
        else "выключена — один вход на рынок"
    )
    return (
        f"📊 <b>Polymarket Copy Bot</b>\n"
        f"Статус: {status}\n"
        f"Кошелёк: <code>{wallet}</code>\n"
        f"Ставка: {mode}\n"
        f"TP: {tp} | SL: {sl}\n"
        f"Докупка: {reentry}"
    )