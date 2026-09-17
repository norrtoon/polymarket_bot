from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from decimal import Decimal, InvalidOperation

from core.config import settings
from core.database import async_session
from models.user import User
from bot.keyboards.main_menu import main_menu_kb, status_text
from bot.utils_user import get_or_create_user

router = Router()


class AmountFSM(StatesGroup):
    waiting_mode = State()
    waiting_value = State()


class TpSlFSM(StatesGroup):
    waiting_tp = State()
    waiting_sl = State()


class SlippageFSM(StatesGroup):
    waiting_adverse = State()
    waiting_favorable = State()


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
    prompt = "Введите сумму ставки в USDC (например: 2):" if mode == "fixed" else "Введите процент от капитала (например: 5):"
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
# Допустимое проскальзывание
# ----------------------------------------------------------------------

@router.callback_query(F.data == "menu:slippage")
async def ask_slippage(call: CallbackQuery, state: FSMContext):
    await call.answer()
    async with async_session() as session:
        user = await get_or_create_user(session, call.from_user.id)
        cur_adv = user.max_slippage_percent
        cur_fav = user.max_favorable_slippage_percent

    adv = f"{cur_adv}%" if cur_adv is not None else \
        f"{settings.max_slippage_percent}% (по умолчанию)"
    fav = f"{cur_fav}%" if cur_fav is not None else \
        f"{settings.max_favorable_slippage_percent}% (по умолчанию)"

    await call.message.answer(
        "📉 <b>Допустимое проскальзывание</b>\n\n"
        "Это разница между ценой, по которой вошёл трейдер, и ценой в "
        "момент, когда бот успевает скопировать сделку.\n\n"
        f"<b>Сейчас не в вашу пользу:</b> {adv}\n"
        f"<b>Сейчас в вашу пользу:</b> {fav}\n\n"
        "<b>Что это меняет:</b> при цене трейдера 0.50 и вашей 0.55 "
        "(это 10%) вы за те же деньги получаете на 9% меньше долей — "
        "риск тот же, выигрыш меньше. Чем ниже лимит, тем ближе ваши "
        "входы к входам трейдера, но тем больше сделок пропускается.\n\n"
        "Ориентиры: 3-5% строго, 6-10% разумный компромисс, "
        "выше 15% защита фактически отключена.\n\n"
        "Введите лимит для входа <b>ХУЖЕ</b> трейдера, в процентах "
        "(например: 6). Или <code>-</code>, чтобы вернуть значение по "
        "умолчанию:",
        parse_mode="HTML",
    )
    await state.set_state(SlippageFSM.waiting_adverse)


@router.message(SlippageFSM.waiting_adverse)
async def set_slippage_adverse(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    if raw == "-":
        value = None
    else:
        try:
            value = float(raw.replace(",", ".").rstrip("%"))
            assert 0 < value <= 100
        except (ValueError, AssertionError):
            await message.answer(
                "❌ Введите число от 0 до 100 (например: 6) "
                "или <code>-</code> для значения по умолчанию.",
                parse_mode="HTML",
            )
            return

    async with async_session() as session:
        user = await get_or_create_user(session, message.from_user.id)
        user.max_slippage_percent = value
        await session.commit()

    await state.set_state(SlippageFSM.waiting_favorable)
    await message.answer(
        "Принято.\n\n"
        "Теперь лимит для входа <b>ЛУЧШЕ</b> трейдера (цена ушла в "
        "вашу пользу). Такие сделки выгодны, поэтому лимит обычно "
        "мягче — но скачок в разы означает, что рынок переоценил "
        "исход, и это уже не та сделка, которую совершил трейдер.\n\n"
        "Введите процент (например: 40) или <code>-</code> для "
        "значения по умолчанию:",
        parse_mode="HTML",
    )


@router.message(SlippageFSM.waiting_favorable)
async def set_slippage_favorable(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    if raw == "-":
        value = None
    else:
        try:
            value = float(raw.replace(",", ".").rstrip("%"))
            assert 0 < value <= 1000
        except (ValueError, AssertionError):
            await message.answer(
                "❌ Введите число больше 0 (например: 40) "
                "или <code>-</code> для значения по умолчанию.",
                parse_mode="HTML",
            )
            return

    async with async_session() as session:
        user = await get_or_create_user(session, message.from_user.id)
        user.max_favorable_slippage_percent = value
        await session.commit()
        adv = user.max_slippage_percent
        text = status_text(user)
        kb = main_menu_kb(user)

    await state.clear()
    adv_txt = f"{adv}%" if adv is not None else "по умолчанию"
    fav_txt = f"{value}%" if value is not None else "по умолчанию"
    await message.answer(
        f"✅ Проскальзывание обновлено\n"
        f"Хуже трейдера: {adv_txt}\n"
        f"Лучше трейдера: {fav_txt}"
    )
    await message.answer(text, reply_markup=kb, parse_mode="HTML")