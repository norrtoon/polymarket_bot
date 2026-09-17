from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery

from core.config import settings
from core.database import async_session
from models.user import User
from bot.keyboards.main_menu import main_menu_kb, status_text
from bot.utils_user import get_or_create_user

router = Router()




@router.message(CommandStart())
async def cmd_start(message: Message):
    async with async_session() as session:
        user = await get_or_create_user(session, message.from_user.id)
        await message.answer(
            status_text(user),
            reply_markup=main_menu_kb(user),
            parse_mode="HTML"
        )


@router.callback_query(F.data == "menu:main")
async def cb_main(call: CallbackQuery):
    async with async_session() as session:
        user = await get_or_create_user(session, call.from_user.id)
        await call.message.edit_text(
            status_text(user),
            reply_markup=main_menu_kb(user),
            parse_mode="HTML"
        )
    await call.answer()