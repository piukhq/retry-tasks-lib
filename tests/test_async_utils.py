from unittest import mock

import pytest

from pytest_mock import MockerFixture

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.asynchronous import enqueue_retry_task


@pytest.mark.asyncio
async def test_enqueue_retry_task(
    mocker: MockerFixture,
    async_db_session: mock.AsyncMock,
    retry_task: RetryTask,
) -> None:
    MockQueue = mocker.patch("rq.Queue")
    mock_queue = MockQueue.return_value
    action = mock.MagicMock(name="action")
    queue = "test_queue"
    mock_query = mocker.patch("retry_tasks_lib.utils.asynchronous._get_retry_task")
    mock_query.return_value = retry_task

    await enqueue_retry_task(async_db_session, retry_task.retry_task_id, action, queue, mock.MagicMock)

    assert retry_task.status == RetryTaskStatuses.IN_PROGRESS
    assert MockQueue.call_args[0] == (queue,)
    mock_queue.enqueue.assert_called_once_with(
        action,
        retry_task_id=retry_task.retry_task_id,
        failure_ttl=604800,
    )
    async_db_session.commit.assert_called_once()


@pytest.mark.asyncio
async def test_enqueue_retry_task_failed_enqueue(
    mocker: MockerFixture, async_db_session: mock.AsyncMock, retry_task: RetryTask
) -> None:
    mock_sentry = mocker.patch("retry_tasks_lib.utils.asynchronous.sentry_sdk")
    MockQueue = mocker.patch("rq.Queue")
    mock_queue = MockQueue.return_value
    mock_queue.enqueue.side_effect = Exception("test enqueue exception")
    action = mock.MagicMock(name="action")
    queue = "test_queue"
    mock_query = mocker.patch("retry_tasks_lib.utils.asynchronous._get_retry_task")
    mock_query.return_value = retry_task

    await enqueue_retry_task(async_db_session, retry_task.retry_task_id, action, queue, mock.MagicMock())
    assert MockQueue.call_args[0] == (queue,)
    mock_queue.enqueue.assert_called_once_with(
        action,
        retry_task_id=retry_task.retry_task_id,
        failure_ttl=604800,
    )
    mock_sentry.capture_exception.assert_called_once()
    async_db_session.rollback.assert_called_once()
