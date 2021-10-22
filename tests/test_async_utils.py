from typing import TYPE_CHECKING
from unittest import mock

import pytest

from pytest_mock import MockerFixture
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

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_enqueue_retry_task(
    mocker: MockerFixture,
    async_db_session: "AsyncSession",
    retry_task_async: RetryTask,
) -> None:
    MockQueue = mocker.patch("rq.Queue")
    mock_queue = MockQueue.return_value
    action = mock.MagicMock(name="action")
    queue = "test_queue"

    await enqueue_retry_task(
        async_db_session,
        retry_task_id=retry_task_async.retry_task_id,
        action=action,
        queue=queue,
        connection=mock.MagicMock(),
    )
    await async_db_session.refresh(retry_task_async)
    assert retry_task_async.status == RetryTaskStatuses.IN_PROGRESS
    assert MockQueue.call_args[0] == (queue,)
    mock_queue.enqueue.assert_called_once_with(
        action,
        retry_task_id=retry_task_async.retry_task_id,
        failure_ttl=604800,
    )


@pytest.mark.asyncio
async def test_enqueue_retry_task_failed_enqueue(
    mocker: MockerFixture, async_db_session: "AsyncSession", retry_task_async: RetryTask
) -> None:
    mock_sentry = mocker.patch("retry_tasks_lib.utils.asynchronous.sentry_sdk")
    MockQueue = mocker.patch("rq.Queue")
    mock_queue = MockQueue.return_value
    mock_queue.enqueue.side_effect = Exception("test enqueue exception")
    action = mock.MagicMock(name="action")
    queue = "test_queue"

    await enqueue_retry_task(
        async_db_session,
        retry_task_id=retry_task_async.retry_task_id,
        action=action,
        queue=queue,
        connection=mock.MagicMock(),
    )
    await async_db_session.refresh(retry_task_async)
    assert retry_task_async.status == RetryTaskStatuses.PENDING
    assert MockQueue.call_args[0] == (queue,)
    mock_queue.enqueue.assert_called_once_with(
        action,
        retry_task_id=retry_task_async.retry_task_id,
        failure_ttl=604800,
    )
    mock_sentry.capture_exception.assert_called_once()


@pytest.mark.asyncio
async def test_enqueue_many_retry_tasks(
    mocker: MockerFixture,
    async_db_session: "AsyncSession",
    retry_task_async: RetryTask,
) -> None:
    MockQueue = mocker.patch("rq.Queue")
    mock_queue = MockQueue.return_value
    action = mock.MagicMock(name="action")
    queue = "test_queue"
    mock_query = mocker.patch("retry_tasks_lib.utils.asynchronous._get_pending_retry_tasks")
    mock_query.return_value = [retry_task_async]

    await enqueue_many_retry_tasks(
        async_db_session,
        retry_tasks_ids=[retry_task_async.retry_task_id],
        action=action,
        queue=queue,
        connection=mock.MagicMock(),
    )

    await async_db_session.refresh(retry_task_async)
    assert retry_task_async.status == RetryTaskStatuses.IN_PROGRESS
    assert MockQueue.call_args[0] == (queue,)
    mock_queue.enqueue_many.assert_called_once()
    mock_queue.prepare_data.assert_called_once_with(
        action,
        retry_task_id=retry_task_async.retry_task_id,
        failure_ttl=604800,
    )


@pytest.mark.asyncio
async def test_enqueue_many_retry_tasks_failed_enqueue(
    mocker: MockerFixture, async_db_session: "AsyncSession", retry_task_async: RetryTask
) -> None:
    mock_sentry = mocker.patch("retry_tasks_lib.utils.asynchronous.sentry_sdk")
    MockQueue = mocker.patch("rq.Queue")
    mock_queue = MockQueue.return_value
    mock_queue.enqueue_many.side_effect = Exception("test enqueue exception")
    action = mock.MagicMock(name="action")
    queue = "test_queue"
    mock_query = mocker.patch("retry_tasks_lib.utils.asynchronous._get_pending_retry_tasks")
    mock_query.return_value = [retry_task_async]

    await enqueue_many_retry_tasks(
        async_db_session,
        retry_tasks_ids=[retry_task_async.retry_task_id],
        action=action,
        queue=queue,
        connection=mock.MagicMock(),
    )
    await async_db_session.refresh(retry_task_async)
    assert retry_task_async.status == RetryTaskStatuses.PENDING
    assert MockQueue.call_args[0] == (queue,)
    mock_queue.enqueue_many.assert_called_once()
    mock_queue.prepare_data.assert_called_once_with(
        action,
        retry_task_id=retry_task_async.retry_task_id,
        failure_ttl=604800,
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
