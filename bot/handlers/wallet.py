from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

from core.database import async_session
from models.user import User
from services.watcher import wallet_watcher
from poly.client import polymarket_client
from bot.keyboards.main_menu import main_menu_kb, status_text
from bot.utils_user import get_or_create_user

router = Router()


class WalletFSM(StatesGroup):
    waiting_address = State()


@router.callback_query(F.data == "menu:wallet")
async def ask_wallet(call: CallbackQuery, state: FSMContext):
    await call.message.answer("Отправьте адрес кошелька для копирования (0x...):")
    await state.set_state(WalletFSM.waiting_address)
    await call.answer()


@router.message(WalletFSM.waiting_address)
async def set_wallet(message: Message, state: FSMContext):
    address = message.text.strip()
    if not (address.startswith("0x") and len(address) == 42):
        await message.answer("❌ Некорректный адрес. Попробуйте снова.")
        return

    traded_count = await polymarket_client.get_traded_count(address) if hasattr(polymarket_client, "get_traded_count") else 0

    async with async_session() as session:
        user = await get_or_create_user(session, message.from_user.id)
        user.target_wallet = address
        await session.commit()

        if user.is_active:
            await wallet_watcher.start_watching(user.id, address)

        extra = f"\nАктивность: {traded_count} рынков" if traded_count else ""
        await message.answer(
            f"✅ Кошелёк установлен: <code>{address}</code>{extra}",
            reply_markup=main_menu_kb(user), parse_mode="HTML",
        )
    await state.clear()