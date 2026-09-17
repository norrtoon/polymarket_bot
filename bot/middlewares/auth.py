from aiogram import BaseMiddleware
from aiogram.types import TelegramObject
from core.config import settings
from typing import Callable, Awaitable, Any


class AuthMiddleware(BaseMiddleware):
    async def __call__(
        self, handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject, data: dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if user and settings.allowed_ids and user.id not in settings.allowed_ids:
            return
        return await handler(event, data)