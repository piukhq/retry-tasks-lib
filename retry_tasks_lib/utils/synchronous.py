from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import rq

from rq.queue import EnqueueData
from sqlalchemy.future import select
from sqlalchemy.orm import Session, noload, selectinload

from retry_tasks_lib import logger
from retry_tasks_lib.db.models import RetryTask, TaskType
from retry_tasks_lib.db.retry_query import sync_run_query
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.settings import DEFAULT_FAILURE_TTL


def _get_task_type(db_session: Session, *, task_type_name: str) -> TaskType:
    return db_session.execute(select(TaskType).where(TaskType.name == task_type_name)).unique().scalar_one()


def sync_create_task(db_session: Session, *, task_type_name: str, params: dict[str, Any]) -> RetryTask:
    """Create an uncommited RetryTask object

    The function is intended to be called in the context of a sync_run_query
    style database wrapper function. It is up to the caller to commit() the
    transaction/session once the object has been returned.
    """
    task_type = _get_task_type(db_session, task_type_name=task_type_name)
    retry_task = RetryTask(task_type_id=task_type.task_type_id)
    db_session.add(retry_task)
    db_session.flush()
    key_ids_by_name = task_type.get_key_ids_by_name()

    db_session.bulk_save_objects(
        retry_task.get_task_type_key_values([(key_ids_by_name[key], str(val)) for key, val in params.items()])
    )

    db_session.flush()
    return retry_task


def sync_create_many_tasks(
    db_session: Session, *, task_type_name: str, params_list: list[dict[str, Any]]
) -> list[RetryTask]:
    """Create many uncommited RetryTask objects

    The function is intended to be called in the context of a sync_run_query
    style database wrapper function. It is up to the caller to commit() the
    transaction/session once the objects have been returned.
    """
    task_type = _get_task_type(db_session, task_type_name=task_type_name)
    keys = task_type.get_key_ids_by_name()
    retry_tasks = [RetryTask(task_type_id=task_type.task_type_id) for _ in params_list]
    db_session.add_all(retry_tasks)
    db_session.flush()

    for retry_task, params in zip(retry_tasks, params_list):
        values = [(keys[k], str(v)) for k, v in params.items()]
        db_session.bulk_save_objects(retry_task.get_task_type_key_values(values))

    db_session.flush()
    return retry_tasks


def _get_retry_task_query(db_session: Session, retry_task_id: int) -> RetryTask:  # pragma: no cover
    return db_session.execute(select(RetryTask).where(RetryTask.retry_task_id == retry_task_id)).unique().scalar_one()


def get_retry_task(db_session: Session, retry_task_id: int) -> RetryTask:
    retry_task: RetryTask = sync_run_query(
        _get_retry_task_query, db_session, rollback_on_exc=False, retry_task_id=retry_task_id
    )
    if retry_task.status not in ([RetryTaskStatuses.PENDING, RetryTaskStatuses.IN_PROGRESS, RetryTaskStatuses.WAITING]):
        raise ValueError(f"Incorrect state: {retry_task.status}")

    return retry_task


def enqueue_retry_task_delay(
    *,
    connection: Any,
    retry_task: RetryTask,
    delay_seconds: float,
    failure_ttl: int = DEFAULT_FAILURE_TTL,
) -> datetime:
    q = rq.Queue(retry_task.task_type.queue_name, connection=connection)
    next_attempt_time = datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(seconds=delay_seconds)
    job = q.enqueue_at(  # requires rq worker --with-scheduler
        next_attempt_time,
        retry_task.task_type.path,
        retry_task_id=retry_task.retry_task_id,
        failure_ttl=failure_ttl,
        meta={"error_handler_path": retry_task.task_type.error_handler_path},
    )

    logger.info(f"Enqueued task for execution at {next_attempt_time.isoformat()}: {job}")
    return next_attempt_time


def enqueue_retry_task(*, connection: Any, retry_task: RetryTask, failure_ttl: int = DEFAULT_FAILURE_TTL) -> rq.job.Job:
    q = rq.Queue(retry_task.task_type.queue_name, connection=connection)
    job = q.enqueue(
        retry_task.task_type.path,
        retry_task_id=retry_task.retry_task_id,
        failure_ttl=failure_ttl,
        meta={"error_handler_path": retry_task.task_type.error_handler_path},
    )

    logger.info(f"Enqueued task for execution: {job}")
    return job


def _get_pending_retry_tasks(db_session: Session, retry_tasks_ids: list[int]) -> list[RetryTask]:
    retry_tasks_ids_set = set(retry_tasks_ids)
    retry_tasks = (
        (
            db_session.execute(
                select(RetryTask)
                .options(selectinload(RetryTask.task_type), noload(RetryTask.task_type_key_values))
                .with_for_update()
                .where(
                    RetryTask.retry_task_id.in_(retry_tasks_ids_set),
                    RetryTask.status == RetryTaskStatuses.PENDING,
                )
            )
        )
        .scalars()
        .all()
    )
    missing_ids = retry_tasks_ids_set - {retry_task.retry_task_id for retry_task in retry_tasks}
    if missing_ids == retry_tasks_ids_set:
        raise ValueError(
            f"Error fetching all the RetryTasks requested for enqueuing. Requested RetryTask ids: {retry_tasks_ids_set}"
        )

    if missing_ids:
        logger.error(f"Error fetching some RetryTasks requested for enqueuing. Missing RetryTask ids: {missing_ids}")

    return retry_tasks


def enqueue_many_retry_tasks(db_session: Session, *, retry_tasks_ids: list[int], connection: Any) -> None:
    retry_tasks: list[RetryTask] = sync_run_query(
        _get_pending_retry_tasks, db_session, rollback_on_exc=False, retry_tasks_ids=retry_tasks_ids
    )

    tasks_by_queue: defaultdict[str, list[RetryTask]] = defaultdict(list)
    for task in retry_tasks:
        tasks_by_queue[task.task_type.queue_name].append(task)

    pipeline = connection.pipeline()
    queued_jobs: list[rq.job.Job] = []
    for queue_name, tasks in tasks_by_queue.items():
        q = rq.Queue(queue_name, connection=connection)
        prepared: list[EnqueueData] = []
        for task in tasks:
            prepared.append(
                rq.Queue.prepare_data(
                    task.task_type.path,
                    kwargs={"retry_task_id": task.retry_task_id},
                    meta={"error_handler_path": task.task_type.error_handler_path},
                    failure_ttl=60 * 60 * 24 * 7,  # 1 week
                )
            )

        jobs = q.enqueue_many(prepared, pipeline=pipeline)
        queued_jobs.extend(jobs)

    pipeline.execute()
    logger.info(f"Queued {len(queued_jobs)} jobs.")
