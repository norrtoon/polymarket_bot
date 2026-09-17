from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from loguru import logger

from core.config import settings
from models.base import Base

# Параметры пула подобраны под характер нагрузки: много коротких
# запросов, часть из них — в критическом пути копирования сделки.
_engine_kwargs = {"echo": False}
if not settings.database_url.startswith("sqlite"):
    _engine_kwargs.update(
        # По умолчанию пул — 5 соединений. Сделки обрабатываются
        # конкурентно (create_task), плюс фоновые задачи: прогрев
        # капитала, TP/SL монитор, сверка. На пике запросы вставали в
        # очередь за свободным соединением уже внутри критического пути.
        pool_size=20,
        max_overflow=10,
        # Не ждать вечно, если пул исчерпан: лучше явная ошибка в логе,
        # чем зависший ордер.
        pool_timeout=10,
        # Postgres рвёт неактивные соединения. Без pre_ping протухшее
        # соединение обнаруживалось только в момент запроса — то есть
        # ровно тогда, когда копируется сделка.
        pool_pre_ping=True,
        # Переоткрываем соединения раньше, чем их закроет сервер
        pool_recycle=1800,
    )

engine = create_async_engine(settings.database_url, **_engine_kwargs)
async_session = async_sessionmaker(engine, expire_on_commit=False)


def _sql_type(column, dialect) -> str:
    """Тип столбца в синтаксисе конкретной СУБД."""
    return column.type.compile(dialect=dialect)


def _add_missing_columns(sync_conn):
    """
    Досоздать столбцы, появившиеся в моделях после создания таблиц.

    Зачем: Base.metadata.create_all() создаёт только ОТСУТСТВУЮЩИЕ
    таблицы и никогда не изменяет существующие. Поэтому каждое новое
    поле в модели роняло бота на живой базе с
    "column ... does not exist", и приходилось либо вручную писать
    ALTER TABLE, либо сносить базу вместе с ключами пользователей и
    историей позиций.

    Здесь делается только безопасная операция — добавление
    недостающих столбцов (все они nullable или с DEFAULT). Столбцы
    НЕ удаляются и НЕ меняют тип: это потребовало бы полноценных
    миграций (Alembic), а молчаливое изменение типа на боевой базе
    опаснее явной ошибки.
    """
    inspector = inspect(sync_conn)
    dialect = sync_conn.dialect
    existing_tables = set(inspector.get_table_names())

    for table in Base.metadata.sorted_tables:
        if table.name not in existing_tables:
            continue  # create_all создаст её целиком

        have = {c["name"] for c in inspector.get_columns(table.name)}
        for column in table.columns:
            if column.name in have:
                continue

            if not column.nullable and column.server_default is None \
                    and column.default is None:
                logger.error(
                    f"Столбец {table.name}.{column.name} обязателен и без "
                    f"значения по умолчанию — автодобавление небезопасно. "
                    f"Добавьте его вручную через ALTER TABLE."
                )
                continue

            ddl = (
                f'ALTER TABLE {table.name} '
                f'ADD COLUMN IF NOT EXISTS {column.name} '
                f'{_sql_type(column, dialect)}'
            )
            if column.default is not None and \
                    getattr(column.default, "is_scalar", False):
                value = column.default.arg
                literal = (
                    "true" if value is True else
                    "false" if value is False else
                    f"'{value}'" if isinstance(value, str) else str(value)
                )
                ddl += f" DEFAULT {literal}"

            try:
                sync_conn.execute(text(ddl))
                logger.info(
                    f"Миграция: добавлен столбец "
                    f"{table.name}.{column.name}"
                )
            except Exception as e:
                logger.error(
                    f"Не удалось добавить {table.name}.{column.name}: "
                    f"{type(e).__name__}: {e}"
                )


async def init_models():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Затем догоняем столбцы, появившиеся в моделях позже
        await conn.run_sync(_add_missing_columns)