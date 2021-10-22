from typing import AsyncGenerator, Generator, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from sqlalchemy_utils import create_database, database_exists, drop_database

from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKey, TmpBase
from retry_tasks_lib.enums import RetryTaskStatuses, TaskParamsKeyTypes
from tests.db import POSTGRES_DB, AsyncSessionMaker, SyncSessionMaker, sync_engine


@pytest.fixture(scope="function")
def mock_async_db_session() -> AsyncMock:
    return AsyncMock(spec=AsyncSession)


@pytest.fixture(scope="function")
def mock_sync_db_session() -> AsyncMock:
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


@pytest.fixture(scope="function")
def sync_db_session() -> Generator["Session", None, None]:
    with SyncSessionMaker() as db_session:
        yield db_session


@pytest.fixture(scope="function")
async def async_db_session() -> AsyncGenerator["AsyncSession", None]:
    async with AsyncSessionMaker() as db_session:
        yield db_session


@pytest.fixture(scope="session")
def task_type_keys() -> list[Tuple[str, TaskParamsKeyTypes]]:
    return [
        ("task-type-key-str", TaskParamsKeyTypes.STRING),
        ("task-type-key-int", TaskParamsKeyTypes.INTEGER),
    ]


@pytest.fixture(scope="function")
def task_type_with_keys_sync(
    sync_db_session: "Session", task_type_keys: list[Tuple[str, TaskParamsKeyTypes]]
) -> TaskType:
    tt = TaskType(name="task-type", path="path.to.func")
    sync_db_session.add(tt)
    sync_db_session.flush()
    ttks: list[TaskTypeKey] = [
        TaskTypeKey(name=key_name, type=key_type, task_type_id=tt.task_type_id) for key_name, key_type in task_type_keys
    ]
    sync_db_session.add_all(ttks)
    sync_db_session.commit()
    return tt


@pytest.fixture(scope="function")
def retry_task_sync(sync_db_session: "Session", task_type_with_keys_sync: TaskType) -> RetryTask:
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


@pytest.fixture(scope="function")
async def task_type_with_keys_async(
    async_db_session: "Session", task_type_keys: list[Tuple[str, TaskParamsKeyTypes]]
) -> TaskType:
    tt = TaskType(name="task-type", path="path.to.func")
    async_db_session.add(tt)
    await async_db_session.flush()
    ttks: list[TaskTypeKey] = [
        TaskTypeKey(name=key_name, type=key_type, task_type_id=tt.task_type_id) for key_name, key_type in task_type_keys
    ]

    async_db_session.add_all(ttks)
    await async_db_session.commit()
    return tt


@pytest.fixture(scope="function")
async def retry_task_async(async_db_session: "Session", task_type_with_keys_async: TaskType) -> RetryTask:
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
