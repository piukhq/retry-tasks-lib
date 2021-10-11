from datetime import datetime, timedelta, timezone
from typing import Generator
from unittest import mock

import pytest

from pytest_mock import MockerFixture
from sqlalchemy.orm import Session

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import enqueue_task, get_retry_task


@pytest.fixture(scope="function")
def fixed_now() -> Generator[datetime, None, None]:
    now = datetime.utcnow()
    with mock.patch("retry_tasks_lib.utils.synchronous.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = now
        yield now


def test_get_retry_task(retry_task: RetryTask, mocker: MockerFixture, db_session: Session) -> None:
    mocker.patch("retry_tasks_lib.utils.synchronous._get_retry_task_query", return_value=retry_task)

    with pytest.raises(ValueError):
        get_retry_task(db_session, retry_task.retry_task_id)

    retry_task.status = RetryTaskStatuses.IN_PROGRESS
    fetched_task = get_retry_task(db_session, retry_task.retry_task_id)
    assert retry_task == fetched_task

    retry_task.status = RetryTaskStatuses.WAITING
    fetched_task = get_retry_task(db_session, retry_task.retry_task_id)
    assert retry_task == fetched_task

    retry_task.status = RetryTaskStatuses.FAILED
    with pytest.raises(ValueError):
        get_retry_task(db_session, retry_task.retry_task_id)

    retry_task.status = RetryTaskStatuses.SUCCESS
    with pytest.raises(ValueError):
        get_retry_task(db_session, retry_task.retry_task_id)


def test_enqueue_task(retry_task: RetryTask, fixed_now: datetime, mocker: MockerFixture) -> None:
    MockQueue = mocker.patch("rq.Queue")
    mock_queue = MockQueue.return_value

    backoff_seconds = 60.0
    queue = "test_queue"
    action = mock.MagicMock(name="action")

    expected_next_attempt_time = fixed_now.replace(tzinfo=timezone.utc) + timedelta(seconds=backoff_seconds)

    next_attempt_time = enqueue_task(
        queue=queue,
        connection=mock.MagicMock(name="connection"),
        action=action,
        retry_task=retry_task,
        backoff_seconds=backoff_seconds,
    )

    assert next_attempt_time == expected_next_attempt_time
    assert MockQueue.call_args[0] == (queue,)
    mock_queue.enqueue_at.assert_called_once_with(
        expected_next_attempt_time,
        action,
        retry_task_id=retry_task.retry_task_id,
        failure_ttl=604800,
    )
