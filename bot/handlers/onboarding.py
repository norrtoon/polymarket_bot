"""
Мастер настройки: пользователь вводит свои данные прямо в боте.

Безопасность:
  - сообщение с приватным ключом УДАЛЯЕТСЯ из чата сразу после чтения;
  - ключ шифруется перед записью в БД (core/crypto.py);
  - ключ никогда не логируется и не показывается обратно;
  - перед вводом пользователь явно подтверждает, что понимает риск.
"""
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from loguru import logger

from core import crypto
from core.database import async_session
from bot.utils_user import get_or_create_user
from bot.keyboards.main_menu import main_menu_kb, status_text

router = Router()


class Setup(StatesGroup):
    risk = State()
    proxy_wallet = State()
    private_key = State()
    clob_key = State()
    clob_secret = State()
    clob_passphrase = State()


def _is_addr(v: str) -> bool:
    v = v.strip()
    return v.startswith("0x") and len(v) == 42


def _is_pk(v: str) -> bool:
    v = v.strip().removeprefix("0x")
    if len(v) != 64:
        return False
    try:
        int(v, 16)
        return True
    except ValueError:
        return False


async def _safe_delete(message: Message):
    """Удалить сообщение с секретом из чата."""
    try:
        await message.delete()
    except Exception as e:
        logger.debug(f"не удалось удалить сообщение с секретом: {e}")


async def _verify_wallet_matches_key(user_id: int) -> str | None:
    """
    Проверить, что введённый ключ управляет введённым адресом Polymarket.
    Возвращает текст ошибки для пользователя или None, если всё в порядке.
    """
    from core.config import settings as _settings
    if _settings.simulation_mode:
        return None     # в симуляции ордера не уходят — проверять нечего

    from poly.client import polymarket_client
    async with async_session() as session:
        user = await get_or_create_user(session, user_id)
        try:
            await polymarket_client.secure_for_user(user)
            return None
        except Exception as e:
            err = str(e)

    await polymarket_client.drop_user_client(user_id)

    if "не принадлежит" in err or "does not match the signer" in err:
        return (
            "❌ <b>Ключ не подходит к этому аккаунту Polymarket</b>\n\n"
            "Адрес из шага 1 не управляется введённым приватным ключом.\n\n"
            "<b>Если аккаунт Polymarket создан через Google или почту</b> — "
            "ключ MetaMask ему не подходит. У таких аккаунтов свой ключ, "
            "его хранит сервис Magic. Экспортировать его можно так:\n"
            "1. Войдите на polymarket.com через Google\n"
            "2. Откройте <code>reveal.magic.link/polymarket</code>\n"
            "3. Нажмите Reveal Private Key и скопируйте ключ\n\n"
            "<b>Если аккаунт через MetaMask</b> — проверьте, что адрес "
            "скопирован из polymarket.com → Settings того же аккаунта, "
            "которым вы входите в MetaMask.\n\n"
            "На шагах CLOB API отправляйте <code>-</code>: бот сам получит "
            "креды из ключа. Креды, скопированные с сайта, часто относятся "
            "к другому кошельку.\n\n"
            "Пройдите /setup заново."
        )
    return (
        f"⚠️ Не удалось проверить кошелёк: <code>{err[:200]}</code>\n"
        f"Проверьте данные и пройдите /setup заново."
    )


@router.callback_query(F.data == "menu:setup")
@router.message(Command("setup"))
async def setup_start(event, state: FSMContext):
    msg = event.message if isinstance(event, CallbackQuery) else event
    if isinstance(event, CallbackQuery):
        await event.answer()

    if not crypto.is_enabled():
        await msg.answer(
            "⚠️ <b>Настройка недоступна</b>\n\n"
            "Не задан <code>SECRETS_ENCRYPTION_KEY</code>. Без него ваш "
            "приватный ключ пришлось бы хранить незашифрованным, а это "
            "недопустимо.\n\n"
            "Администратору: сгенерируйте ключ и добавьте в .env.",
            parse_mode="HTML",
        )
        return

    b = InlineKeyboardBuilder()
    b.button(text="✅ Понимаю риск, продолжить", callback_data="setup:risk_ok")
    b.button(text="❌ Отмена", callback_data="setup:cancel")
    b.adjust(1)

    await state.set_state(Setup.risk)
    await msg.answer(
        "🔐 <b>Настройка торгового доступа</b>\n\n"
        "Чтобы бот мог копировать сделки, ему нужен приватный ключ вашего "
        "кошелька — ордера Polymarket подписываются подписью кошелька, "
        "иначе биржа их не примет.\n\n"
        "<b>Что важно понимать:</b>\n"
        "• ключ даёт полный контроль над средствами кошелька\n"
        "• он будет храниться на сервере в зашифрованном виде\n"
        "• при компрометации сервера средства могут быть потеряны\n\n"
        "<b>Настоятельная рекомендация:</b> заведите ОТДЕЛЬНЫЙ кошелёк "
        "только для копитрейдинга и держите на нём лишь ту сумму, которую "
        "готовы потерять. Не используйте основной кошелёк.\n\n"
        "Продолжаем?",
        reply_markup=b.as_markup(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "setup:cancel")
async def setup_cancel(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    await call.message.answer("Настройка отменена.")


@router.callback_query(F.data == "setup:risk_ok", Setup.risk)
async def setup_risk_ok(call: CallbackQuery, state: FSMContext):
    await call.answer()
    async with async_session() as session:
        user = await get_or_create_user(session, call.from_user.id)
        user.risk_accepted = True
        await session.commit()

    await state.set_state(Setup.proxy_wallet)
    await call.message.answer(
        "<b>Шаг 1 из 5 — адрес Polymarket</b>\n\n"
        "Откройте polymarket.com → профиль → <b>Settings</b> и скопируйте "
        "адрес кошелька (начинается с 0x).\n\n"
        "Это НЕ адрес из MetaMask — это отдельный адрес, на котором "
        "Polymarket держит ваши средства.",
        parse_mode="HTML",
    )


@router.message(Setup.proxy_wallet)
async def setup_proxy(message: Message, state: FSMContext):
    addr = (message.text or "").strip()
    if not _is_addr(addr):
        await message.answer(
            "Это не похоже на адрес. Нужен формат 0x… длиной 42 символа. "
            "Попробуйте ещё раз."
        )
        return

    async with async_session() as session:
        user = await get_or_create_user(session, message.from_user.id)
        user.proxy_wallet = addr
        await session.commit()

    await state.set_state(Setup.private_key)
    await message.answer(
        "<b>Шаг 2 из 5 — приватный ключ</b>\n\n"
        "Пришлите приватный ключ кошелька, которым вы входите в Polymarket "
        "(64 символа, можно с префиксом 0x).\n\n"
        "<b>Как вы входите в Polymarket?</b>\n"
        "• <b>Через MetaMask</b> — ключ из MetaMask: Account details → "
        "Show private key.\n"
        "• <b>Через Google или почту</b> — ключ MetaMask НЕ подойдёт. "
        "Войдите на polymarket.com и откройте "
        "<code>reveal.magic.link/polymarket</code> → Reveal Private Key.\n\n"
        "🔒 Сообщение будет <b>удалено сразу</b> после обработки, ключ "
        "сохранится зашифрованным.",
        parse_mode="HTML",
    )


@router.message(Setup.private_key)
async def setup_pk(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    await _safe_delete(message)  # удаляем ДО любой обработки

    if not _is_pk(raw):
        await message.answer(
            "Ключ не распознан (нужно 64 hex-символа). Пришлите ещё раз."
        )
        return

    pk = raw if raw.startswith("0x") else f"0x{raw}"
    try:
        enc = crypto.encrypt(pk)
    except RuntimeError as e:
        await message.answer(f"⚠️ {e}")
        await state.clear()
        return

    async with async_session() as session:
        user = await get_or_create_user(session, message.from_user.id)
        user.private_key_enc = enc
        await session.commit()

    await state.set_state(Setup.clob_key)
    await message.answer(
        "✅ Ключ сохранён и зашифрован, сообщение удалено.\n\n"
        "<b>Шаг 3 из 5 — CLOB API Key</b>\n\n"
        "Пришлите ваш CLOB API Key. Если его нет — отправьте "
        "<code>-</code>, бот попробует создать креды автоматически "
        "по вашему ключу.",
        parse_mode="HTML",
    )


async def _save_secret(user_id: int, field: str, value: str | None):
    async with async_session() as session:
        user = await get_or_create_user(session, user_id)
        setattr(user, field, crypto.encrypt(value) if value else None)
        await session.commit()


@router.message(Setup.clob_key)
async def setup_clob_key(message: Message, state: FSMContext):
    val = (message.text or "").strip()
    await _safe_delete(message)
    await _save_secret(
        message.from_user.id, "clob_api_key_enc",
        None if val == "-" else val,
    )
    await state.set_state(Setup.clob_secret)
    await message.answer(
        "<b>Шаг 4 из 5 — CLOB API Secret</b>\n"
        "(или <code>-</code>, если пропускаете)",
        parse_mode="HTML",
    )


@router.message(Setup.clob_secret)
async def setup_clob_secret(message: Message, state: FSMContext):
    val = (message.text or "").strip()
    await _safe_delete(message)
    await _save_secret(
        message.from_user.id, "clob_api_secret_enc",
        None if val == "-" else val,
    )
    await state.set_state(Setup.clob_passphrase)
    await message.answer(
        "<b>Шаг 5 из 5 — CLOB API Passphrase</b>\n"
        "(или <code>-</code>, если пропускаете)",
        parse_mode="HTML",
    )


@router.message(Setup.clob_passphrase)
async def setup_clob_passphrase(message: Message, state: FSMContext):
    val = (message.text or "").strip()
    await _safe_delete(message)
    await _save_secret(
        message.from_user.id, "clob_api_passphrase_enc",
        None if val == "-" else val,
    )

    async with async_session() as session:
        user = await get_or_create_user(session, message.from_user.id)
        user.setup_completed = True
        await session.commit()

    # Клиент биржи кэшируется на пользователя и иначе жил бы со СТАРЫМ
    # адресом и ключом до перезапуска бота: поправленные в /setup данные
    # не применились бы, и ордера продолжали бы уходить не с того
    # кошелька. Сбрасываем — следующий ордер создаст клиента заново.
    try:
        from poly.client import polymarket_client
        await polymarket_client.drop_user_client(message.from_user.id)
    except Exception:
        pass

    # ПРОВЕРКА СРАЗУ: принадлежит ли кошелёк введённому ключу.
    #
    # Раньше несовпадение обнаруживалось только на первой сделке — и то
    # непонятной ошибкой биржи вроде "the order signer address has to be
    # the address of the API KEY". Типичный случай: аккаунт Polymarket
    # создан через Google или почту, а подключают ключ MetaMask. У таких
    # аккаунтов свой ключ, который хранит сервис Magic, и ключ MetaMask
    # ими не управляет.
    check_error = await _verify_wallet_matches_key(message.from_user.id)
    if check_error:
        async with async_session() as session:
            user = await get_or_create_user(session, message.from_user.id)
            user.setup_completed = False
            await session.commit()
        await state.clear()
        await message.answer(check_error, parse_mode="HTML")
        return

    async with async_session() as session:
        user = await get_or_create_user(session, message.from_user.id)
        text = status_text(user)
        kb = main_menu_kb(user)

    await state.clear()
    await message.answer(
        "✅ <b>Настройка завершена</b>\n\n"
        "Осталось указать кошелёк, за которым следим, и сумму ставки — "
        "и можно запускать копирование.\n\n"
        "Данные можно перезаписать в любой момент командой /setup, "
        "а удалить — командой /forget_keys.",
        parse_mode="HTML",
    )
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@router.message(Command("approve"))
async def approve_allowances(message: Message):
    """
    Разовая выдача разрешений контрактам биржи.
    Без неё боевые ордера отклоняются.
    """
    from poly.client import polymarket_client

    async with async_session() as session:
        user = await get_or_create_user(session, message.from_user.id)
        if not user.has_trading_credentials():
            await message.answer(
                "Сначала пройдите настройку: /setup"
            )
            return

    await message.answer("⏳ Выдаю разрешения контрактам биржи...")
    ok, detail = await polymarket_client.ensure_trading_approvals(user)
    if ok:
        await message.answer(
            f"✅ Готово: {detail}\n\n"
            f"Теперь бот может размещать ордера от вашего имени."
        )
    else:
        # Подсказку про газ даём, только если ошибка действительно о нём.
        # Раньше она добавлялась всегда — а кошельки аккаунтов через
        # Google/почту и Safe выдают разрешения без газа, через
        # релейер Polymarket, и совет про MATIC только сбивал с толку.
        gas_hint = ""
        low = detail.lower()
        if "gas" in low and "gasless" not in low or "insufficient funds" in low:
            gas_hint = "\n\nПохоже, на кошельке не хватает POL (MATIC) на газ."
        await message.answer(
            f"❌ Не удалось выдать разрешения:\n{detail}{gas_hint}",
            parse_mode="HTML",
        )


@router.message(Command("forget_keys"))
async def forget_keys(message: Message, state: FSMContext):
    """Удалить все торговые секреты пользователя."""
    await state.clear()
    async with async_session() as session:
        user = await get_or_create_user(session, message.from_user.id)
        user.private_key_enc = None
        user.clob_api_key_enc = None
        user.clob_api_secret_enc = None
        user.clob_api_passphrase_enc = None
        user.setup_completed = False
        user.is_active = False
        await session.commit()

    # Сбрасываем закэшированный клиент биржи с удалёнными ключами
    try:
        from poly.client import polymarket_client
        await polymarket_client.drop_user_client(message.from_user.id)
    except Exception:
        pass

    try:
        from services.watcher import wallet_watcher
        await wallet_watcher.stop_watching(message.from_user.id)
    except Exception as e:
        logger.debug(f"stop_watching при forget_keys: {e}")

    await message.answer(
        "🗑 Все торговые данные удалены, копирование остановлено."
    )