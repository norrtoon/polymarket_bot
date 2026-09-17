from core.config import settings
from models.user import User


async def get_or_create_user(session, user_id: int) -> User:
    """
    Возвращает пользователя, создавая запись, если её нет.

    Раньше эта функция жила только в bot/handlers/start.py, а остальные
    хендлеры делали session.get(User, id) напрямую и падали с
    'NoneType' object has no attribute ...', если записи не было —
    например, после переключения DATABASE_URL с SQLite на Postgres
    (новая база пустая) или когда второй пользователь жал кнопки, ни
    разу не вызвав /start.
    """
    user = await session.get(User, user_id)
    if not user:
        user = User(
            id=user_id,
            proxy_wallet=settings.my_proxy_wallet_address or None,
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)
    elif not user.proxy_wallet and settings.my_proxy_wallet_address:
        user.proxy_wallet = settings.my_proxy_wallet_address
        await session.commit()
        await session.refresh(user)
    return user