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
    _get_enqueuable_retry_task,
    _get_enqueuable_retry_tasks,
    async_create_many_tasks,
    async_create_task,
    enqueue_many_retry_tasks,
    enqueue_retry_task,
)

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
    assert retry_task_async.status == RetryTaskStatuses.PENDING
    job = q.jobs[0]
    assert job.kwargs == {"retry_task_id": 1}
    assert job.func_name == retry_task_async.task_type.path
    assert job.meta == {"error_handler_path": "path.to.error_handler"}


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
    assert retry_task_async.status == RetryTaskStatuses.PENDING
    assert len(q.get_job_ids()) == 1
    job = q.jobs[0]
    assert job.kwargs == {"retry_task_id": 1}
    assert job.func_name == retry_task_async.task_type.path
    assert job.meta == {"error_handler_path": "path.to.error_handler"}


@pytest.mark.asyncio
@mock.patch("retry_tasks_lib.utils.asynchronous.logger")
async def test_enqueue_many_retry_tasks_empty_list(
    mock_logger: mock.Mock, async_db_session: "AsyncSession", retry_task_async: RetryTask, redis: Redis
) -> None:
    q = rq.Queue(retry_task_async.task_type.queue_name, connection=redis)
    assert len(q.jobs) == 0

    await enqueue_many_retry_tasks(
        async_db_session,
        retry_tasks_ids=[],
        connection=redis,
    )
    assert len(q.jobs) == 0

    mock_logger.warning.assert_called_once_with(
        "async 'enqueue_many_retry_tasks' expects a list of task's ids but received an empty list instead."
    )


@pytest.mark.asyncio
async def test__get_pending_retry_task(async_db_session: mock.AsyncMock, retry_task_async: RetryTask) -> None:
    assert retry_task_async.status == RetryTaskStatuses.PENDING
    with pytest.raises(NoResultFound):
        await _get_enqueuable_retry_task(async_db_session, 101)
    await _get_enqueuable_retry_task(async_db_session, retry_task_async.retry_task_id)


@pytest.mark.asyncio
async def test__get_pending_retry_tasks(
    mocker: MockerFixture, async_db_session: mock.AsyncMock, retry_task_async: RetryTask
) -> None:
    mock_log = mocker.patch("retry_tasks_lib.utils.asynchronous.logger")
    assert retry_task_async.status == RetryTaskStatuses.PENDING
    with pytest.raises(ValueError):
        await _get_enqueuable_retry_tasks(async_db_session, [101])

    await _get_enqueuable_retry_tasks(async_db_session, [retry_task_async.retry_task_id])
    mock_log.error.assert_not_called()

    unexpected_id = retry_task_async.retry_task_id + 1
    unexpected_id_set = {unexpected_id}
    await _get_enqueuable_retry_tasks(async_db_session, [retry_task_async.retry_task_id, unexpected_id])
    mock_log.error.assert_called_once_with(
        f"Error fetching some RetryTasks requested for enqueuing. Missing RetryTask ids: {unexpected_id_set!r}"
    )


@pytest.mark.asyncio
async def test_async_create_task(async_db_session: "AsyncSession", task_type_with_keys_async: TaskType) -> None:
    params = {"task-type-key-str": "astring", "task-type-key-int": 42, "task-type-key-json": ["list", "of", "stuff"]}
    retry_task = await async_create_task(
        db_session=async_db_session,
        task_type_name="task-type",
        params=params,
    )
    await async_db_session.commit()
    await async_db_session.refresh(retry_task)
    assert retry_task.get_params() == params


@pytest.mark.asyncio
async def test_async_create_many_tasks(async_db_session: "AsyncSession", task_type_with_keys_async: TaskType) -> None:
    params_list = [
        {"task-type-key-str": "astring1", "task-type-key-int": 42, "task-type-key-json": ["list", "of", "ones"]},
        {"task-type-key-str": "astring2", "task-type-key-int": 43, "task-type-key-json": ["list", "of", "twos"]},
    ]
    retry_tasks = await async_create_many_tasks(
        db_session=async_db_session,
        task_type_name="task-type",
        params_list=params_list,
    )
    await async_db_session.commit()
    expected_params = []
    for retry_task in retry_tasks:
        await async_db_session.refresh(retry_task)
        expected_params.append(retry_task.get_params())

    assert expected_params == params_list
