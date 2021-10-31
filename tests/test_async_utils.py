from typing import TYPE_CHECKING
from unittest import mock

import pytest
import rq

from pytest_mock import MockerFixture
from redis import Redis
from sqlalchemy.orm.exc import NoResultFound

from retry_tasks_lib.db.models import RetryTask, TaskType
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.asynchronous import (
    _get_pending_retry_task,
    _get_pending_retry_tasks,
    async_create_task,
    enqueue_many_retry_tasks,
    enqueue_retry_task,
)
from tests.db import AsyncSessionMaker

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_enqueue_retry_task(
    async_db_session: "AsyncSession",
    retry_task_async: RetryTask,
    redis: Redis,
) -> None:

    q = rq.Queue(retry_task_async.task_type.queue_name, connection=redis)
    assert len(q.jobs) == 0
    await enqueue_retry_task(
        async_db_session,
        retry_task_id=retry_task_async.retry_task_id,
        connection=redis,
    )
    await async_db_session.refresh(retry_task_async)
    assert retry_task_async.status == RetryTaskStatuses.IN_PROGRESS
    job = q.jobs[0]
    assert job.kwargs == {"retry_task_id": 1}
    assert job.func_name == retry_task_async.task_type.path
    assert job.meta == {"error_handler_path": "path.to.error_handler"}


@pytest.mark.asyncio
async def test_enqueue_retry_task_failed_enqueue(
    mocker: MockerFixture, async_db_session: "AsyncSession", retry_task_async: RetryTask
) -> None:
    mock_sentry = mocker.patch("retry_tasks_lib.utils.asynchronous.sentry_sdk")
    MockQueue = mocker.patch("rq.Queue")
    mock_queue = MockQueue.return_value
    mock_queue.enqueue.side_effect = Exception("test enqueue exception")

    # Provide a different session here as the exception rolls back the fixture
    # session making it unusable in an async context
    async with AsyncSessionMaker() as session:
        await enqueue_retry_task(
            session,
            retry_task_id=retry_task_async.retry_task_id,
            connection=mock.MagicMock(),
        )
    await async_db_session.refresh(retry_task_async)
    assert retry_task_async.status == RetryTaskStatuses.PENDING
    assert MockQueue.call_args[0] == (retry_task_async.task_type.queue_name,)
    mock_queue.enqueue.assert_called_once_with(
        retry_task_async.task_type.path,
        retry_task_id=retry_task_async.retry_task_id,
        failure_ttl=604800,
        meta={"error_handler_path": "path.to.error_handler"},
    )
    mock_sentry.capture_exception.assert_called_once()


@pytest.mark.asyncio
async def test_enqueue_many_retry_tasks(
    async_db_session: "AsyncSession", retry_task_async: RetryTask, redis: Redis
) -> None:
    q = rq.Queue(retry_task_async.task_type.queue_name, connection=redis)
    assert len(q.jobs) == 0

    await enqueue_many_retry_tasks(
        async_db_session,
        retry_tasks_ids=[retry_task_async.retry_task_id],
        connection=redis,
    )

    await async_db_session.refresh(retry_task_async)
    assert retry_task_async.status == RetryTaskStatuses.IN_PROGRESS
    assert len(q.get_job_ids()) == 1
    job = q.jobs[0]
    assert job.kwargs == {"retry_task_id": 1}
    assert job.func_name == retry_task_async.task_type.path
    assert job.meta == {"error_handler_path": "path.to.error_handler"}


@pytest.mark.asyncio
async def test_enqueue_many_retry_tasks_failed_enqueue(
    mocker: MockerFixture, async_db_session: "AsyncSession", retry_task_async: RetryTask
) -> None:
    mock_sentry = mocker.patch("retry_tasks_lib.utils.asynchronous.sentry_sdk")
    MockQueue = mocker.patch("rq.Queue")
    mock_queue = MockQueue.return_value
    mock_queue.enqueue_many.side_effect = Exception("test enqueue exception")
    mock_query = mocker.patch("retry_tasks_lib.utils.asynchronous._get_pending_retry_tasks")
    mock_query.return_value = [retry_task_async]

    # Provide a different session here as the exception rolls back the fixture
    # session making it unusable in an async context
    async with AsyncSessionMaker() as session:
        await enqueue_many_retry_tasks(
            session,
            retry_tasks_ids=[retry_task_async.retry_task_id],
            connection=mock.MagicMock(),
        )
    await async_db_session.refresh(retry_task_async)
    assert retry_task_async.status == RetryTaskStatuses.PENDING
    assert MockQueue.call_args[0] == (retry_task_async.task_type.queue_name,)
    mock_queue.enqueue_many.assert_called_once()
    MockQueue.prepare_data.assert_called_once_with(
        retry_task_async.task_type.path,
        kwargs={"retry_task_id": retry_task_async.retry_task_id},
        failure_ttl=604800,
        meta={"error_handler_path": "path.to.error_handler"},
    )
    mock_sentry.capture_exception.assert_called_once()


@pytest.mark.asyncio
async def test__get_pending_retry_task(async_db_session: mock.AsyncMock, retry_task_async: RetryTask) -> None:
    assert retry_task_async.status == RetryTaskStatuses.PENDING
    with pytest.raises(NoResultFound):
        await _get_pending_retry_task(async_db_session, 101)
    await _get_pending_retry_task(async_db_session, retry_task_async.retry_task_id)


@pytest.mark.asyncio
async def test__get_pending_retry_tasks(
    mocker: MockerFixture, async_db_session: mock.AsyncMock, retry_task_async: RetryTask
) -> None:
    mock_log = mocker.patch("retry_tasks_lib.utils.asynchronous.logger")
    assert retry_task_async.status == RetryTaskStatuses.PENDING
    with pytest.raises(ValueError):
        await _get_pending_retry_tasks(async_db_session, [101])

    await _get_pending_retry_tasks(async_db_session, [retry_task_async.retry_task_id])
    mock_log.error.assert_not_called()

    unexpected_id = retry_task_async.retry_task_id + 1
    await _get_pending_retry_tasks(async_db_session, [retry_task_async.retry_task_id, unexpected_id])
    mock_log.error.assert_called_once_with(
        f"Error fetching some RetryTasks requested for enqueuing. Missing RetryTaks ids: {set([unexpected_id])}"
    )


@pytest.mark.asyncio
async def test_async_create_task(async_db_session: "AsyncSession", task_type_with_keys_async: TaskType) -> None:
    params = {"task-type-key-str": "astring", "task-type-key-int": 42}
    retry_task = await async_create_task(
        db_session=async_db_session,
        task_type_name="task-type",
        params=params,
    )
    await async_db_session.commit()
    await async_db_session.refresh(retry_task)
    assert retry_task.get_params() == params
