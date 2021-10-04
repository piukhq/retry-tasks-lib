from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Tuple

import rq
from retry_task_lib.db.models import RetryTask
from retry_task_lib.db.retry_query import sync_run_query
from retry_task_lib.enums import QueuedRetryStatuses
from sqlalchemy.future import select
from sqlalchemy.orm.attributes import flag_modified

from . import logger

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.orm import Session


def get_retry_task_and_params(
    db_session: "Session", retry_task_id: int, fetch_task_params: bool = True
) -> Tuple[RetryTask, dict]:
    def _query() -> RetryTask:
        return db_session.execute(select(RetryTask).where(RetryTask.retry_task_id == retry_task_id)).scalars().one()

    retry_task: RetryTask = sync_run_query(_query, db_session, rollback_on_exc=False)
    if retry_task.retry_status not in ([QueuedRetryStatuses.IN_PROGRESS, QueuedRetryStatuses.WAITING]):
        raise ValueError(f"Incorrect state: {retry_task.retry_status}")

    task_params: dict = {}
    if fetch_task_params:
        for value in retry_task.task_type_key_values:
            key = value.task_type_key
            task_params[key.name] = key.type.value(value.value)

    return retry_task, task_params


# move this to be a method of the RetryTask model
def update_task(
    db_session: "Session",
    retry_task: RetryTask,
    *,
    response_audit: dict = None,
    status: QueuedRetryStatuses = None,
    next_attempt_time: datetime = None,
    increase_attempts: bool = False,
    clear_next_attempt_time: bool = False,
) -> None:
    def _query() -> None:
        if response_audit is not None:
            retry_task.response_data.append(response_audit)
            flag_modified(retry_task, "response_data")

        if status is not None:
            retry_task.retry_status = status  # type: ignore

        if increase_attempts:
            retry_task.attempts += 1

        if clear_next_attempt_time or next_attempt_time is not None:
            retry_task.next_attempt_time = next_attempt_time

        db_session.commit()

    sync_run_query(_query, db_session)


def enqueue_task(
    *, queue: str, connection: Any, action: Callable, retry_task: RetryTask, backoff_seconds: float
) -> datetime:
    q = rq.Queue(queue, connection=connection)
    next_attempt_time = datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(seconds=backoff_seconds)
    job = q.enqueue_at(  # requires rq worker --with-scheduler
        next_attempt_time,
        action,
        retry_task_id=retry_task.retry_task_id,
        failure_ttl=60 * 60 * 24 * 7,  # 1 week
    )

    logger.info(f"Requeued task for execution at {next_attempt_time.isoformat()}: {job}")
    return next_attempt_time


def send_request(retry_task: RetryTask) -> dict:
    raise NotImplementedError()
