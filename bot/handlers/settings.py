from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from decimal import Decimal, InvalidOperation

from core.database import async_session
from bot.keyboards.main_menu import main_menu_kb, status_text
from bot.utils_user import get_or_create_user

router = Router()


class AmountFSM(StatesGroup):
    waiting_mode = State()
    waiting_value = State()


class MinTradeFSM(StatesGroup):
    waiting_value = State()


class TpSlFSM(StatesGroup):
    waiting_tp = State()
    waiting_sl = State()



@router.callback_query(F.data == "menu:amount")
async def ask_amount_mode(call: CallbackQuery, state: FSMContext):
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    b = InlineKeyboardBuilder()
    b.button(text="Фиксированная сумма (USDC)", callback_data="amount_mode:fixed")
    b.button(text="Процент от капитала (%)", callback_data="amount_mode:percent")
    b.adjust(1)
    await call.message.answer("Выберите режим ставки:", reply_markup=b.as_markup())
    await call.answer()


@router.callback_query(F.data.startswith("amount_mode:"))
async def choose_amount_mode(call: CallbackQuery, state: FSMContext):
    mode = call.data.split(":")[1]
    await state.update_data(mode=mode)
    prompt = "Введите сумму ставки в USDC (например: 10):" if mode == "fixed" else "Введите процент от капитала (например: 5):"
    await call.message.answer(prompt)
    await state.set_state(AmountFSM.waiting_value)
    await call.answer()


@router.message(AmountFSM.waiting_value)
async def set_amount_value(message: Message, state: FSMContext):
    data = await state.get_data()
    mode = data.get("mode", "fixed")
    try:
        value = Decimal(message.text.strip())
        assert value > 0
    except (InvalidOperation, AssertionError):
        await message.answer("❌ Введите положительное число.")
        return

    async with async_session() as session:
        user = await get_or_create_user(session, message.from_user.id)
        user.bet_mode = mode
        if mode == "fixed":
            user.bet_amount = value
        else:
            user.bet_percent = float(value)
        await session.commit()
        await message.answer(
            f"✅ Режим ставки обновлён: {mode} = {value}{'%' if mode=='percent' else ' USDC'}",
            reply_markup=main_menu_kb(user),
        )
    await state.clear()


@router.callback_query(F.data == "menu:tpsl")
async def ask_tp(call: CallbackQuery, state: FSMContext):
    await call.message.answer("Введите Take Profit % (или 0, чтобы отключить):")
    await state.set_state(TpSlFSM.waiting_tp)
    await call.answer()


@router.message(TpSlFSM.waiting_tp)
async def set_tp(message: Message, state: FSMContext):
    try:
        tp = float(message.text.strip())
    except ValueError:
        await message.answer("❌ Введите число.")
        return
    await state.update_data(tp=tp)
    await message.answer("Теперь введите Stop Loss % (или 0, чтобы отключить):")
    await state.set_state(TpSlFSM.waiting_sl)


@router.message(TpSlFSM.waiting_sl)
async def set_sl(message: Message, state: FSMContext):
    try:
        sl = float(message.text.strip())
    except ValueError:
        await message.answer("❌ Введите число.")
        return
    data = await state.get_data()
    tp = data.get("tp", 0)

    async with async_session() as session:
        user = await get_or_create_user(session, message.from_user.id)
        user.tp_percent = tp if tp > 0 else None
        user.sl_percent = sl if sl > 0 else None
        await session.commit()
        await message.answer(status_text(user), reply_markup=main_menu_kb(user), parse_mode="HTML")
    await state.clear()


# ----------------------------------------------------------------------
# Докупка вслед за трейдером
# ----------------------------------------------------------------------

@router.callback_query(F.data == "menu:toggle_reentry")
async def toggle_reentry(call: CallbackQuery):
    """
    Переключить режим докупки одним нажатием.

    Меню перерисовывается на месте (edit_text), а не новым сообщением:
    иначе чат засорялся бы копиями меню при каждом переключении.
    """
    async with async_session() as session:
        user = await get_or_create_user(session, call.from_user.id)
        user.allow_reentry = not bool(getattr(user, "allow_reentry", True))
        await session.commit()
        enabled = user.allow_reentry
        text = status_text(user)
        kb = main_menu_kb(user)

    await call.answer(
        "Докупка включена: копируются все входы трейдера"
        if enabled else
        "Докупка выключена: один вход на рынок",
        show_alert=False,
    )
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        # Сообщение могло быть слишком старым для редактирования —
        # тогда просто присылаем новое меню.
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")



# ----------------------------------------------------------------------
# Минимальный размер сделки трейдера
# ----------------------------------------------------------------------

@router.callback_query(F.data == "menu:min_trade")
async def ask_min_trade(call: CallbackQuery, state: FSMContext):
    await call.answer()
    async with async_session() as session:
        user = await get_or_create_user(session, call.from_user.id)
        current = getattr(user, "min_trader_trade_usdc", None) or 0

    now = f"{float(current):g} USDC" if current else "не задан — копируются любые"
    await call.message.answer(
        "🔍 <b>Минимальный размер сделки трейдера</b>\n\n"
        f"Сейчас: {now}\n\n"
        "Бот не будет копировать ПОКУПКИ трейдера меньше этой суммы.\n\n"
        "Зачем: крупные трейдеры часто делают мелкие сделки — хвосты "
        "своих лимитных ордеров, скрытый набор позиции мелкими частями, "
        "а иногда и приманку для копировщиков. При фиксированной ставке "
        "доллар трейдера превращается в вашу полную ставку.\n\n"
        "Продажи фильтр не затрагивает — бот всегда выходит вслед за "
        "трейдером.\n\n"
        "Введите сумму в USDC (например: 20) или <code>0</code>, чтобы "
        "копировать любые:",
        parse_mode="HTML",
    )
    await state.set_state(MinTradeFSM.waiting_value)


@router.message(MinTradeFSM.waiting_value)
async def set_min_trade(message: Message, state: FSMContext):
    raw = (message.text or "").strip().replace(",", ".").rstrip("$").strip()
    try:
        value = Decimal(raw)
        if value < 0 or value > Decimal("1000000"):
            raise InvalidOperation
    except (InvalidOperation, ValueError):
        await message.answer(
            "❌ Введите число от 0 (например: 20). 0 — копировать любые."
        )
        return

    async with async_session() as session:
        user = await get_or_create_user(session, message.from_user.id)
        user.min_trader_trade_usdc = value if value > 0 else None
        await session.commit()
        text = status_text(user)
        kb = main_menu_kb(user)

    await state.clear()
    await message.answer(
        f"✅ Теперь копируются покупки трейдера от {value:g} USDC"
        if value > 0 else
        "✅ Фильтр выключен — копируются любые покупки трейдера"
    )
    await message.answer(text, reply_markup=kb, parse_mode="HTML")