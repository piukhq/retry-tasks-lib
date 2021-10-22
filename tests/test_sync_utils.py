from datetime import datetime, timedelta, timezone
from typing import Generator
from unittest import mock

import pytest

from pytest_mock import MockerFixture
from sqlalchemy.orm import Session

from retry_tasks_lib.db.models import RetryTask, TaskType
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import (
    enqueue_retry_task,
    enqueue_retry_task_delay,
    get_retry_task,
    sync_create_task,
)


@pytest.fixture(scope="function")
def fixed_now() -> Generator[datetime, None, None]:
    now = datetime.utcnow()
    with mock.patch("retry_tasks_lib.utils.synchronous.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = now
        yield now


def test_enqueue_retry_task_delay(retry_task_sync: RetryTask, fixed_now: datetime, mocker: MockerFixture) -> None:
    MockQueue = mocker.patch("rq.Queue")
    mock_queue = MockQueue.return_value

    backoff_seconds = 60.0
    queue = "test_queue"
    action = mock.MagicMock(name="action")

    expected_next_attempt_time = fixed_now.replace(tzinfo=timezone.utc) + timedelta(seconds=backoff_seconds)

    next_attempt_time = enqueue_retry_task_delay(
        queue=queue,
        connection=mock.MagicMock(name="connection"),
        action=action,
        retry_task=retry_task_sync,
        delay_seconds=backoff_seconds,
    )

    assert next_attempt_time == expected_next_attempt_time
    assert MockQueue.call_args[0] == (queue,)
    mock_queue.enqueue_at.assert_called_once_with(
        expected_next_attempt_time,
        action,
        retry_task_id=retry_task_sync.retry_task_id,
        failure_ttl=604800,
    )


def test_enqueue_retry_task(retry_task_sync: RetryTask, mocker: MockerFixture) -> None:
    MockQueue = mocker.patch("rq.Queue")
    mock_queue = MockQueue.return_value

    queue = "test_queue"
    action = mock.MagicMock(name="action")

    enqueue_retry_task(
        queue=queue, connection=mock.MagicMock(name="connection"), action=action, retry_task=retry_task_sync
    )

    assert MockQueue.call_args[0] == (queue,)
    mock_queue.enqueue.assert_called_once_with(
        action,
        retry_task_id=retry_task_sync.retry_task_id,
        failure_ttl=604800,
    )


def test_sync_create_task_and_get_retry_task(sync_db_session: "Session", task_type_with_keys_sync: TaskType) -> None:
    params = {"task-type-key-str": "astring", "task-type-key-int": 42}
    retry_task = sync_create_task(db_session=sync_db_session, task_type_name="task-type", params=params)
    sync_db_session.add(retry_task)
    sync_db_session.commit()

    for status in (RetryTaskStatuses.IN_PROGRESS, RetryTaskStatuses.WAITING):
        retry_task.status = status
        sync_db_session.commit()
        retry_task = get_retry_task(sync_db_session, retry_task.retry_task_id)
        assert retry_task.get_params() == params

    for status in (
        RetryTaskStatuses.PENDING,
        RetryTaskStatuses.FAILED,
        RetryTaskStatuses.SUCCESS,
        RetryTaskStatuses.CANCELLED,
    ):
        retry_task.status = status
        sync_db_session.commit()
        with pytest.raises(ValueError):
            get_retry_task(sync_db_session, retry_task.retry_task_id)


def test_get_retry_task(sync_db_session: "Session", retry_task_sync: RetryTask) -> None:
    retry_task_sync.status = RetryTaskStatuses.IN_PROGRESS
    sync_db_session.commit()
    get_retry_task(db_session=sync_db_session, retry_task_id=retry_task_sync.retry_task_id)
