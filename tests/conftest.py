from collections.abc import AsyncGenerator, Generator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from redis import Redis
from sqlalchemy import Engine, TextClause, create_engine, text
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKey, TmpBase
from retry_tasks_lib.enums import RetryTaskStatuses, TaskParamsKeyTypes
from tests.db import POSTGRES_DB, REDIS_URL, AsyncSessionMaker, SyncSessionMaker, sync_engine


def get_postgres_db_url_from_db_url(db_url: str | URL) -> URL:
    return make_url(db_url)._replace(database="postgres")


def _get_scalar_result(engine: Engine, sql: TextClause) -> Any:  # noqa: ANN401
    with engine.connect() as conn:
        return conn.scalar(sql)


def database_exists(url: str | URL) -> bool:
    url = make_url(url)
    database = url.database
    if not database:
        raise ValueError("No database found in URL")
    postgres_url = get_postgres_db_url_from_db_url(url)
    engine = create_engine(postgres_url)
    dialect = engine.dialect
    quoted_database = dialect.preparer(dialect).quote(database)
    try:
        return bool(
            _get_scalar_result(
                engine,
                text("SELECT 1 FROM pg_database WHERE datname = :quoted_database").bindparams(
                    quoted_database=quoted_database
                ),
            )
        )
    except (ProgrammingError, OperationalError):
        return False
    finally:
        if engine:
            engine.dispose()


def drop_database(url: str | URL) -> None:
    url = make_url(url)
    database = url.database
    if not database:
        raise ValueError("No database found in URL")
    postgres_url = get_postgres_db_url_from_db_url(url)
    engine = create_engine(postgres_url, isolation_level="AUTOCOMMIT")
    dialect = engine.dialect
    quoted_database = dialect.preparer(dialect).quote(database)
    with engine.begin() as conn:
        # Disconnect all users from the database we are dropping.
        stmt = """
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = :quoted_database
        AND pid <> pg_backend_pid();
        """
        conn.execute(text(stmt).bindparams(quoted_database=quoted_database))
        # Drop the database.
        stmt = f"DROP DATABASE {quoted_database}"
        conn.execute(text(stmt))


def create_database(url: str | URL) -> None:
    url = make_url(url)
    database = url.database
    if not database:
        raise ValueError("No database found in URL")
    postgres_url = get_postgres_db_url_from_db_url(url)
    engine = create_engine(postgres_url, isolation_level="AUTOCOMMIT")
    dialect = engine.dialect
    quoter = dialect.preparer(dialect)
    with engine.begin() as conn:
        stmt = f"CREATE DATABASE {quoter.quote(database)} ENCODING 'utf8' TEMPLATE template1"
        conn.execute(text(stmt))


@pytest.fixture(scope="function")
def redis() -> Generator:
    rds = Redis.from_url(
        REDIS_URL,
        socket_connect_timeout=3,
        socket_keepalive=True,
        retry_on_timeout=False,
    )
    rds.flushdb()
    yield rds
    rds.flushdb()


@pytest.fixture(scope="function")
def mock_async_db_session() -> AsyncMock:
    return AsyncMock(spec=AsyncSession)


@pytest.fixture(scope="function")
def mock_sync_db_session() -> MagicMock:
    return MagicMock(spec=Session)


@pytest.fixture(scope="session", autouse=True)
def setup_db() -> Generator:
    if sync_engine.url.database != POSTGRES_DB:
        raise ValueError(f"Unsafe attempt to recreate database: {sync_engine.url.database}")

    if database_exists(sync_engine.url):
        drop_database(sync_engine.url)
    create_database(sync_engine.url)

    yield

    drop_database(sync_engine.url)


@pytest.fixture(scope="function", autouse=True)
def setup_tables() -> Generator:
    """
    autouse set to True so will be run before each test function, to set up tables
    and tear them down after each test runs
    """
    TmpBase.metadata.create_all(bind=sync_engine)

    yield

    # Drop all tables after each test
    TmpBase.metadata.drop_all(bind=sync_engine)


@pytest.fixture(scope="function", name="sync_db_session")
def sync_db_session_fixture() -> Generator["Session", None, None]:
    with SyncSessionMaker() as db_session:
        yield db_session


@pytest.fixture(scope="function", name="async_db_session")
async def async_db_session_fixture() -> AsyncGenerator["AsyncSession", None]:
    async with AsyncSessionMaker() as db_session:
        yield db_session


@pytest.fixture(scope="session", name="task_type_keys")
def task_type_keys_fixture() -> list[tuple[str, TaskParamsKeyTypes]]:
    return [
        ("lookup-val", TaskParamsKeyTypes.STRING),
        ("task-type-key-str", TaskParamsKeyTypes.STRING),
        ("task-type-key-int", TaskParamsKeyTypes.INTEGER),
        ("task-type-key-float", TaskParamsKeyTypes.FLOAT),
        ("task-type-key-bool", TaskParamsKeyTypes.BOOLEAN),
        ("task-type-key-date", TaskParamsKeyTypes.DATE),
        ("task-type-key-datetime", TaskParamsKeyTypes.DATETIME),
        ("task-type-key-json", TaskParamsKeyTypes.JSON),
    ]


@pytest.fixture(scope="function", name="task_type_with_keys_sync")
def task_type_with_keys_sync_fixture(
    sync_db_session: Session, task_type_keys: list[tuple[str, TaskParamsKeyTypes]]
) -> TaskType:
    task_type = TaskType(
        name="task-type", path="path.to.func", queue_name="queue-name", error_handler_path="path.to.error_handler"
    )
    sync_db_session.add(task_type)
    sync_db_session.flush()
    task_type_keys_objs: list[TaskTypeKey] = [
        TaskTypeKey(name=key_name, type=key_type, task_type_id=task_type.task_type_id)
        for key_name, key_type in task_type_keys
    ]
    sync_db_session.add_all(task_type_keys_objs)
    sync_db_session.commit()
    return task_type


@pytest.fixture(scope="function", name="retry_task_sync")
def retry_task_sync_fixture(sync_db_session: Session, task_type_with_keys_sync: TaskType) -> RetryTask:
    task = RetryTask(
        task_type_id=task_type_with_keys_sync.task_type_id,
        status=RetryTaskStatuses.PENDING,
        attempts=0,
        audit_data=[],
        next_attempt_time=None,
    )
    sync_db_session.add(task)
    sync_db_session.commit()
    return task


@pytest.fixture(scope="function", name="task_type_with_keys_async")
async def task_type_with_keys_async_fixture(
    async_db_session: AsyncSession, task_type_keys: list[tuple[str, TaskParamsKeyTypes]]
) -> TaskType:
    task_type = TaskType(
        name="task-type", path="path.to.func", queue_name="queue-name", error_handler_path="path.to.error_handler"
    )
    async_db_session.add(task_type)
    await async_db_session.flush()
    task_type_keys_objs: list[TaskTypeKey] = [
        TaskTypeKey(name=key_name, type=key_type, task_type_id=task_type.task_type_id)
        for key_name, key_type in task_type_keys
    ]

    async_db_session.add_all(task_type_keys_objs)
    await async_db_session.commit()
    return task_type


@pytest.fixture(scope="function")
async def retry_task_async(async_db_session: AsyncSession, task_type_with_keys_async: TaskType) -> RetryTask:
    task = RetryTask(
        task_type_id=task_type_with_keys_async.task_type_id,
        status=RetryTaskStatuses.PENDING,
        attempts=0,
        audit_data=[],
        next_attempt_time=None,
    )
    async_db_session.add(task)
    await async_db_session.commit()
    return task


@pytest.fixture(scope="function")
def task_type_with_keys_and_cleanup_handler_path(
    sync_db_session: Session, task_type_keys: list[tuple[str, TaskParamsKeyTypes]]
) -> TaskType:
    task_type = TaskType(
        name="task-type-with-cleanup",
        path="path.to.func",
        queue_name="queue-name",
        error_handler_path="path.to.error_handler",
        cleanup_handler_path="tests.conftest.mock_cleanup_hanlder_fn",
    )
    sync_db_session.add(task_type)
    sync_db_session.flush()
    task_type_keys_objs: list[TaskTypeKey] = [
        TaskTypeKey(name=key_name, type=key_type, task_type_id=task_type.task_type_id)
        for key_name, key_type in task_type_keys
    ]
    sync_db_session.add_all(task_type_keys_objs)
    sync_db_session.commit()
    return task_type


@pytest.fixture(scope="function")
def retry_task_sync_with_cleanup(
    sync_db_session: Session, task_type_with_keys_and_cleanup_handler_path: TaskType
) -> RetryTask:
    task = RetryTask(
        task_type_id=task_type_with_keys_and_cleanup_handler_path.task_type_id,
        status=RetryTaskStatuses.CLEANUP,
        attempts=0,
        audit_data=[],
        next_attempt_time=None,
    )
    sync_db_session.add(task)
    sync_db_session.commit()
    return task
