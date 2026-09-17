from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject
from loguru import logger

from core.config import settings
from core.database import async_session
from models.user import User


class EnsureUserMiddleware(BaseMiddleware):
    """
    Гарантирует, что запись пользователя существует в БД ДО того, как
    отработает любой хендлер.

    Зачем: запись создавалась только в /start (get_or_create_user), а
    все остальные хендлеры делали session.get(User, id) и сразу
    обращались к полям результата. Если записи нет — session.get()
    возвращает None и хендлер падает с
    'NoneType' object has no attribute 'target_wallet' / 'is_active'.

    Записи может не быть в трёх реальных случаях:
      1) переключили DATABASE_URL с SQLite на Postgres — новая база
         пустая, старые пользователи туда не переехали;
      2) второй пользователь нажал кнопку, ни разу не вызвав /start;
      3) базу пересоздали, а у пользователя в чате осталось старое
         сообщение с кнопками, и он жмёт их.
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        tg_user = data.get("event_from_user")
        if tg_user:
            try:
                async with async_session() as session:
                    user = await session.get(User, tg_user.id)
                    if not user:
                        user = User(
                            id=tg_user.id,
                            proxy_wallet=(
                                settings.my_proxy_wallet_address or None
                            ),
                        )
                        session.add(user)
                        await session.commit()
                        logger.info(
                            f"Создана запись пользователя {tg_user.id} "
                            f"(её не было в БД)"
                        )
                    elif (
                        not user.proxy_wallet
                        and settings.my_proxy_wallet_address
                    ):
                        user.proxy_wallet = settings.my_proxy_wallet_address
                        await session.commit()
            except Exception as e:
                # Не роняем обработку апдейта из-за проблем с БД —
                # хендлеры теперь и сами устойчивы к отсутствию записи.
                logger.error(
                    f"EnsureUserMiddleware error для {tg_user.id}: "
                    f"{type(e).__name__}: {e}"
                )

        return await handler(event, data)