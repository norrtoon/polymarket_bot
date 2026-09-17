from loguru import logger
from aiogram import Router, F
from aiogram.types import CallbackQuery
from sqlalchemy import select

from core.database import async_session
from models.user import User
from models.position import Position
from models.trade_log import TradeLog
from services.watcher import wallet_watcher
from poly.client import polymarket_client
from bot.keyboards.main_menu import main_menu_kb, status_text
from bot.utils_user import get_or_create_user

router = Router()


@router.callback_query(F.data == "menu:balance")
async def show_balance(call: CallbackQuery):
    await call.answer()
    async with async_session() as session:
        user = await get_or_create_user(session, call.from_user.id)
        if not user or not user.proxy_wallet:
            await call.message.answer(
                "Proxy-кошелёк ещё не инициализирован."
            )
            return
        wallet = user.proxy_wallet

    # Разбивка, а не одна цифра: если что-то не подтянулось, сразу
    # видно, какая именно часть — стоимость позиций или свободный USDC.
    positions_value = await polymarket_client.get_portfolio_value(wallet)
    free_usdc = await polymarket_client.get_free_usdc_balance(wallet)
    total = positions_value + free_usdc

    # Раз уж сходили в сеть по прямому запросу пользователя — сразу
    # обновим кэш, которым пользуется percent-режим при расчёте ставки.
    try:
        from services.trader import trader_service
        trader_service.set_equity_cache(call.from_user.id, total)
    except Exception as e:
        logger.debug(f"set_equity_cache skipped: {e}")

    await call.message.answer(
        f"⚖️ <b>Капитал</b>\n"
        f"📊 В позициях: {positions_value:.2f} USDC\n"
        f"💵 Свободно: {free_usdc:.2f} USDC\n"
        f"━━━━━━━━━━━━━━\n"
        f"💰 Итого: {total:.2f} USDC\n\n"
        f"<code>{wallet}</code>",
        parse_mode="HTML",
    )


@router.callback_query(F.data == "menu:startcopy")
async def start_copy(call: CallbackQuery):
    # Отвечаем Telegram СРАЗУ, а не после DB-запросов и start_watching.
    # Если ack задерживается, кнопка на экране пользователя продолжает
    # "крутиться" — велик соблазн тапнуть ещё раз, а это второй
    # callback_query и второй параллельный вызов этого хендлера
    # (aiogram обрабатывает апдейты конкурентно). Именно так, судя по
    # логам с двумя идентичными "Watcher started" в одну и ту же
    # миллисекунду, скорее всего дублировался start_watching.
    await call.answer()

    async with async_session() as session:
        user = await get_or_create_user(session, call.from_user.id)
        if not user.target_wallet:
            await call.message.answer("Сначала установите кошелёк!")
            return

        # Идемпотентность: если слежка уже активна, повторный вызов
        # start_watching (пересоздание задачи + сброс asyncio-лока)
        # избыточен — просто освежаем экран.
        if user.is_active:
            await call.message.edit_text(
                status_text(user), reply_markup=main_menu_kb(user),
                parse_mode="HTML",
            )
            return

        user.is_active = True
        await session.commit()
        await wallet_watcher.start_watching(user.id, user.target_wallet)
        await call.message.edit_text(
            status_text(user), reply_markup=main_menu_kb(user),
            parse_mode="HTML",
        )


@router.callback_query(F.data == "menu:stop")
async def stop_copy(call: CallbackQuery):
    async with async_session() as session:
        user = await get_or_create_user(session, call.from_user.id)
        user.is_active = False
        await session.commit()
        await wallet_watcher.stop_watching(user.id)
        await call.message.edit_text(status_text(user), reply_markup=main_menu_kb(user), parse_mode="HTML")
    await call.answer("Копитрейдинг остановлен ⏸️")