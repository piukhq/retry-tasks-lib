from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable

import rq

from sqlalchemy.future import select
from sqlalchemy.orm import Session

from retry_tasks_lib.db.models import RetryTask, TaskType
from retry_tasks_lib.db.retry_query import sync_run_query
from retry_tasks_lib.enums import RetryTaskStatuses

from . import logger


def sync_create_task(
    db_session: Session,
    *,
    task_type_name: str,
    params: dict[str, Any],
) -> RetryTask:
    """Create an uncommited RetryTask object

    The function is intended to be called in the context of a sync_run_query
    style database wrapper function. It is up to the caller to commit() the
    transaction/session once the object has been returned.
    """
    task_type: TaskType = (
        db_session.execute(select(TaskType).where(TaskType.name == task_type_name)).unique().scalar_one()
    )
    retry_task = RetryTask(task_type_id=task_type.task_type_id)
    db_session.add(retry_task)
    db_session.flush()
    key_ids_by_name = task_type.get_key_ids_by_name()

    db_session.bulk_save_objects(
        retry_task.get_task_type_key_values([(key_ids_by_name[key], str(val)) for key, val in params.items()])
    )

    db_session.flush()
    return retry_task


def _get_retry_task_query(db_session: "Session", retry_task_id: int) -> RetryTask:  # pragma: no cover
    return db_session.execute(select(RetryTask).where(RetryTask.retry_task_id == retry_task_id)).unique().scalar_one()


def get_retry_task(db_session: "Session", retry_task_id: int) -> RetryTask:
    retry_task: RetryTask = sync_run_query(
        _get_retry_task_query, db_session, rollback_on_exc=False, retry_task_id=retry_task_id
    )
    if retry_task.status not in ([RetryTaskStatuses.IN_PROGRESS, RetryTaskStatuses.WAITING]):
        raise ValueError(f"Incorrect state: {retry_task.status}")

    return retry_task


def enqueue_retry_task_delay(
    *, queue: str, connection: Any, action: Callable, retry_task: RetryTask, delay_seconds: float
) -> datetime:
    q = rq.Queue(queue, connection=connection)
    next_attempt_time = datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(seconds=delay_seconds)
    job = q.enqueue_at(  # requires rq worker --with-scheduler
        next_attempt_time,
        action,
        retry_task_id=retry_task.retry_task_id,
        failure_ttl=60 * 60 * 24 * 7,  # 1 week
    )

    logger.info(f"Enqueued task for execution at {next_attempt_time.isoformat()}: {job}")
    return next_attempt_time


def enqueue_retry_task(*, queue: str, connection: Any, action: Callable, retry_task: RetryTask) -> None:
    q = rq.Queue(queue, connection=connection)
    job = q.enqueue(
        action,
        retry_task_id=retry_task.retry_task_id,
        failure_ttl=60 * 60 * 24 * 7,  # 1 week
    )

    logger.info(f"Enqueued task for execution: {job}")
