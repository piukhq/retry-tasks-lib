# pylint: disable=unused-argument, no-value-for-parameter

from datetime import datetime, timedelta, timezone
from typing import Generator
from unittest import mock

import pytest
import rq

from pytest_mock import MockerFixture
from redis import Redis
from sqlalchemy.orm import Session
from sqlalchemy.orm.session import make_transient

from retry_tasks_lib.db.models import RetryTask, TaskType
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.settings import DEFAULT_FAILURE_TTL
from retry_tasks_lib.utils.synchronous import (
    IncorrectRetryTaskStatusError,
    RetryTaskAdditionalQueryData,
    _get_pending_and_waiting_retry_tasks,
    enqueue_many_retry_tasks,
    enqueue_retry_task,
    enqueue_retry_task_delay,
    get_retry_task,
    retryable_task,
    sync_create_many_tasks,
    sync_create_task,
)
from tests.db import SyncSessionMaker


def test_retry_task_decorator_no_task_for_provided_retry_task_id() -> None:
    @retryable_task(db_session_factory=SyncSessionMaker)
    def task_func(retry_task: RetryTask, db_session: Session) -> None:
        pytest.fail("Task function ran when it should not have")

    with pytest.raises(ValueError):
        task_func(9999)  # pylint: disable=no-value-for-parameter


def test_retry_task_decorator_default_query_wrong_task_status(
    retry_task_sync: RetryTask, sync_db_session: Session
) -> None:
    @retryable_task(db_session_factory=SyncSessionMaker)
    def task_func(retry_task: RetryTask, db_session: Session) -> None:
        pytest.fail("Task function ran when it should not have")

    # only PENDING, RETRYING or WAITING should be allowed to run CANCELLED will quietely remove the task from the queue.
    for status in [
        s
        for s in RetryTaskStatuses
        if s
        not in (
            RetryTaskStatuses.PENDING,
            RetryTaskStatuses.RETRYING,
            RetryTaskStatuses.WAITING,
            RetryTaskStatuses.CANCELLED,
        )
    ]:
        retry_task_sync.status = status
        sync_db_session.commit()

        with pytest.raises(IncorrectRetryTaskStatusError):
            task_func(retry_task_sync.retry_task_id)

        sync_db_session.refresh(retry_task_sync)
        assert retry_task_sync.status == status


@mock.patch("retry_tasks_lib.utils.synchronous.logger")
def test_retry_task_decorator_default_query_cancelled_task(
    mock_logger: mock.Mock, retry_task_sync: RetryTask, sync_db_session: Session
) -> None:
    @retryable_task(db_session_factory=SyncSessionMaker)
    def task_func(retry_task: RetryTask, db_session: Session) -> None:
        pytest.fail("Task function ran when it should not have")

    retry_task_sync.status = RetryTaskStatuses.CANCELLED
    sync_db_session.commit()

    task_func(retry_task_sync.retry_task_id)

    sync_db_session.refresh(retry_task_sync)
    assert retry_task_sync.status == RetryTaskStatuses.CANCELLED
    mock_logger.info.assert_called_once_with(
        f"Task with retry_task_id {retry_task_sync.retry_task_id} has been cancelled. Removing from queue."
    )


def test_retry_task_decorator_default_query(retry_task_sync: RetryTask, sync_db_session: Session) -> None:
    assert retry_task_sync.status == RetryTaskStatuses.PENDING

    @retryable_task(db_session_factory=SyncSessionMaker)
    def task_func(retry_task: RetryTask, db_session: Session) -> None:
        assert retry_task.retry_task_id == retry_task_sync.retry_task_id

    task_func(retry_task_sync.retry_task_id)

    sync_db_session.refresh(retry_task_sync)
    assert retry_task_sync.status == RetryTaskStatuses.IN_PROGRESS


def test_retry_task_decorator_default_with_sub_query(
    task_type_with_keys_sync: TaskType, sync_db_session: Session, redis: Redis
) -> None:
    task1, task2, task3 = sync_create_many_tasks(
        sync_db_session,
        task_type_name=task_type_with_keys_sync.name,
        params_list=[
            {"task-type-key-str": "duplicated", "task-type-key-int": 42},
            {"task-type-key-str": "duplicated", "task-type-key-int": 42},
            {"task-type-key-str": "different", "task-type-key-int": 42},
        ],
    )

    for status in (RetryTaskStatuses.IN_PROGRESS, RetryTaskStatuses.FAILED):
        task1.status = RetryTaskStatuses.PENDING
        task2.status = status
        task3.status = RetryTaskStatuses.PENDING
        sync_db_session.commit()

        @retryable_task(
            db_session_factory=SyncSessionMaker,
            exclusive_constraints=[
                RetryTaskAdditionalQueryData(
                    matching_val_keys=["task-type-key-str", "task-type-key-int"],
                    additional_statuses=[RetryTaskStatuses.FAILED],
                )
            ],
            redis_connection=redis,
        )
        def task_func(retry_task: RetryTask, db_session: Session) -> None:
            pytest.fail("Task function ran when it should not have")

        task_func(task1.retry_task_id)

        sync_db_session.refresh(task1)
        sync_db_session.refresh(task2)
        assert task1.status == RetryTaskStatuses.PENDING

    q = rq.Queue(task1.task_type.queue_name, connection=redis)
    assert len(q.scheduled_job_registry.get_job_ids()) == 2
    job_ids = q.scheduled_job_registry.get_job_ids()
    job1 = rq.job.Job.fetch(job_ids[0], connection=redis)
    assert job1.kwargs == {"retry_task_id": task1.retry_task_id}
    assert job1.func_name == task1.task_type.path
    job2 = rq.job.Job.fetch(job_ids[1], connection=redis)
    assert job2.kwargs == {"retry_task_id": task1.retry_task_id}
    assert job2.func_name == task1.task_type.path


def test_retry_task_decorator_default_with_sub_query_different_task_type(
    task_type_with_keys_sync: TaskType, sync_db_session: Session, redis: Redis
) -> None:
    task1, task2 = sync_create_many_tasks(
        sync_db_session,
        task_type_name=task_type_with_keys_sync.name,
        params_list=[
            {"task-type-key-str": "duplicated", "task-type-key-int": 42},
            {"task-type-key-str": "duplicated", "task-type-key-int": 42},
        ],
    )

    # make a new task type using task_type_with_keys_sync as a template
    sync_db_session.expunge(task_type_with_keys_sync)
    make_transient(task_type_with_keys_sync)
    task_type_with_keys_sync.task_type_id = None
    task_type_with_keys_sync.name = "new-task-type"
    sync_db_session.add(task_type_with_keys_sync)
    sync_db_session.flush()

    task2.task_type_id = task_type_with_keys_sync.task_type_id
    sync_db_session.commit()

    assert task1.task_type_id != task2.task_type_id

    assert task1.status == task2.status == RetryTaskStatuses.PENDING

    # Both tasks have the same params
    assert task1.get_params() == task2.get_params()

    @retryable_task(
        db_session_factory=SyncSessionMaker,
        exclusive_constraints=[
            RetryTaskAdditionalQueryData(
                matching_val_keys=["task-type-key-str", "task-type-key-int"],
                additional_statuses=[RetryTaskStatuses.FAILED],
            )
        ],
        redis_connection=redis,
    )
    def task_func(retry_task: RetryTask, db_session: Session) -> None:
        assert retry_task.retry_task_id == task1.retry_task_id

    task_func(task1.retry_task_id)


@pytest.fixture(scope="function")
def fixed_now() -> Generator[datetime, None, None]:
    now = datetime.now(tz=timezone.utc)
    with mock.patch("retry_tasks_lib.utils.synchronous.datetime") as mock_datetime:
        mock_datetime.now.return_value = now
        yield now


def test_enqueue_retry_task_delay(retry_task_sync: RetryTask, fixed_now: datetime, redis: Redis) -> None:
    backoff_seconds = 60.0
    expected_next_attempt_time = fixed_now + timedelta(seconds=backoff_seconds)

    q = rq.Queue(retry_task_sync.task_type.queue_name, connection=redis)
    assert len(q.scheduled_job_registry.get_job_ids()) == 0

    next_attempt_time = enqueue_retry_task_delay(
        connection=redis,
        retry_task=retry_task_sync,
        delay_seconds=backoff_seconds,
    )
    assert next_attempt_time == expected_next_attempt_time
    assert len(q.scheduled_job_registry.get_job_ids()) == 1
    job = rq.job.Job.fetch(q.scheduled_job_registry.get_job_ids()[0], connection=redis)
    assert job.kwargs == {"retry_task_id": 1}
    assert job.func_name == retry_task_sync.task_type.path


def test_enqueue_retry_task(retry_task_sync: RetryTask, redis: Redis) -> None:
    q = rq.Queue(retry_task_sync.task_type.queue_name, connection=redis)
    assert len(q.jobs) == 0
    enqueue_retry_task(connection=redis, retry_task=retry_task_sync, at_front=True)
    assert len(q.jobs) == 1
    job = q.jobs[0]
    assert job.kwargs == {"retry_task_id": 1}
    assert job.failure_ttl == 604800
    assert job.func_name == retry_task_sync.task_type.path


@mock.patch("retry_tasks_lib.utils.synchronous.rq.Queue")
def test_enqueue_retry_task_at_front(
    mock_queue_class: mock.MagicMock, retry_task_sync: RetryTask, redis: Redis
) -> None:
    mock_queue = mock.MagicMock()
    mock_queue_class.return_value = mock_queue
    enqueue_retry_task(connection=redis, retry_task=retry_task_sync, at_front=True)
    mock_queue.enqueue.assert_called_with(
        retry_task_sync.task_type.path,
        retry_task_id=retry_task_sync.retry_task_id,
        failure_ttl=DEFAULT_FAILURE_TTL,
        at_front=True,
        meta={"error_handler_path": retry_task_sync.task_type.error_handler_path},
    )


def test_enqueue_many_retry_tasks(sync_db_session: "Session", retry_task_sync: RetryTask, redis: Redis) -> None:
    q = rq.Queue(retry_task_sync.task_type.queue_name, connection=redis)
    assert len(q.jobs) == 0
    task_ids = [retry_task_sync.retry_task_id]

    # duplicate this task to make another
    sync_db_session.expunge(retry_task_sync)
    make_transient(retry_task_sync)
    retry_task_sync.retry_task_id = None
    sync_db_session.add(retry_task_sync)
    sync_db_session.commit()
    task_ids.append(retry_task_sync.retry_task_id)

    enqueue_many_retry_tasks(
        sync_db_session,
        retry_tasks_ids=task_ids,
        connection=redis,
    )

    sync_db_session.refresh(retry_task_sync)
    assert len(q.get_job_ids()) == 2
    for i in range(2):
        job = q.jobs[i]
        assert job.kwargs == {"retry_task_id": i + 1}
        assert job.func_name == retry_task_sync.task_type.path
        assert job.meta == {"error_handler_path": "path.to.error_handler"}


@mock.patch("retry_tasks_lib.utils.synchronous.rq.Queue")
def test_enqueue_many_retry_tasks_at_front(
    mock_queue_class: mock.MagicMock,
    sync_db_session: "Session",
    retry_task_sync: RetryTask,
) -> None:
    mock_q = mock.MagicMock()
    mock_queue_class.return_value = mock_q
    mock_connection, mock_pipeline = mock.MagicMock(), mock.MagicMock()
    mock_connection.pipeline.return_value = mock_pipeline

    mock_enqueue_data = mock.MagicMock()
    mock_queue_class.prepare_data.return_value = mock_enqueue_data

    enqueue_many_retry_tasks(
        sync_db_session,
        retry_tasks_ids=[retry_task_sync.retry_task_id],
        connection=mock_connection,
        at_front=True,
    )
    mock_queue_class.prepare_data.assert_called_once_with(
        retry_task_sync.task_type.path,
        kwargs={"retry_task_id": retry_task_sync.retry_task_id},
        meta={"error_handler_path": retry_task_sync.task_type.error_handler_path},
        failure_ttl=DEFAULT_FAILURE_TTL,
        at_front=True,
    )

    mock_q.enqueue_many.assert_called_with([mock_enqueue_data], pipeline=mock_pipeline)


def test__get_pending_retry_tasks(mocker: MockerFixture, sync_db_session: Session, retry_task_sync: RetryTask) -> None:
    mock_log = mocker.patch("retry_tasks_lib.utils.synchronous.logger")
    assert retry_task_sync.status == RetryTaskStatuses.PENDING
    with pytest.raises(ValueError):
        _get_pending_and_waiting_retry_tasks(sync_db_session, [101])

    _get_pending_and_waiting_retry_tasks(sync_db_session, [retry_task_sync.retry_task_id])
    mock_log.error.assert_not_called()

    unexpected_id = retry_task_sync.retry_task_id + 1
    _get_pending_and_waiting_retry_tasks(sync_db_session, [retry_task_sync.retry_task_id, unexpected_id])
    mock_log.error.assert_called_once_with(
        f"Error fetching some RetryTasks requested for enqueuing. Missing RetryTask ids: {set([unexpected_id])}"
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
        RetryTaskStatuses.FAILED,
        RetryTaskStatuses.SUCCESS,
        RetryTaskStatuses.CANCELLED,
    ):
        retry_task.status = status
        sync_db_session.commit()
        with pytest.raises(ValueError):
            get_retry_task(sync_db_session, retry_task.retry_task_id)


def test_sync_create_many_tasks_and_get_retry_task(
    sync_db_session: "Session", task_type_with_keys_sync: TaskType
) -> None:
    params_list = [
        {"task-type-key-str": "a_string", "task-type-key-int": 42},
        {"task-type-key-str": "b_string", "task-type-key-int": 43},
        {"task-type-key-str": "c_string", "task-type-key-int": 44},
    ]
    retry_tasks = sync_create_many_tasks(
        db_session=sync_db_session, task_type_name="task-type", params_list=params_list
    )
    sync_db_session.add_all(retry_tasks)
    sync_db_session.commit()

    for task, params in zip(retry_tasks, params_list):
        for status in (RetryTaskStatuses.IN_PROGRESS, RetryTaskStatuses.WAITING):
            task.status = status
            sync_db_session.commit()
            task = get_retry_task(sync_db_session, task.retry_task_id)
            assert task.get_params() == params


def test_get_retry_task(sync_db_session: "Session", retry_task_sync: RetryTask) -> None:
    retry_task_sync.status = RetryTaskStatuses.IN_PROGRESS
    sync_db_session.commit()
    get_retry_task(db_session=sync_db_session, retry_task_id=retry_task_sync.retry_task_id)
