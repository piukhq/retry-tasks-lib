import asyncio

from collections import defaultdict
from collections.abc import Sequence
from typing import TYPE_CHECKING

import rq

from rq.queue import EnqueueData
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload, noload, selectinload

from retry_tasks_lib import logger
from retry_tasks_lib.db.models import RetryTask, TaskType
from retry_tasks_lib.db.retry_query import async_run_query
from retry_tasks_lib.enums import RetryTaskStatuses

if TYPE_CHECKING:
    from redis import Redis


async def _get_task_type(db_session: AsyncSession, *, task_type_name: str) -> TaskType:
    return (
        (
            await db_session.execute(
                select(TaskType).options(joinedload(TaskType.task_type_keys)).where(TaskType.name == task_type_name)
            )
        )
        .unique()
        .scalar_one()
    )


async def async_create_task(db_session: AsyncSession, *, task_type_name: str, params: dict) -> RetryTask:
    """Create an uncommited RetryTask object

    The function is intended to be called in the context of a sync_run_query
    style database wrapper function. It is up to the caller to commit() the
    transaction/session once the object has been returned.
    """
    task_type = await _get_task_type(db_session, task_type_name=task_type_name)
    retry_task = RetryTask(task_type_id=task_type.task_type_id)
    db_session.add(retry_task)
    await db_session.flush()
    key_ids_by_name = task_type.get_key_ids_by_name()
    db_session.add_all(
        retry_task.get_task_type_key_values([(key_ids_by_name[key], val) for (key, val) in params.items()])
    )
    await db_session.flush()
    return retry_task


async def async_create_many_tasks(
    db_session: AsyncSession, *, task_type_name: str, params_list: list[dict]
) -> list[RetryTask]:
    """Create many uncommited RetryTask objects

    The function is intended to be called in the context of a sync_run_query
    style database wrapper function. It is up to the caller to commit() the
    transaction/session once the objects have been returned.
    """
    task_type = await _get_task_type(db_session, task_type_name=task_type_name)
    keys = task_type.get_key_ids_by_name()
    retry_tasks = [RetryTask(task_type_id=task_type.task_type_id) for _ in params_list]
    db_session.add_all(retry_tasks)
    await db_session.flush()

    for retry_task, params in zip(retry_tasks, params_list, strict=True):
        values = [(keys[k], v) for k, v in params.items()]
        db_session.add_all(retry_task.get_task_type_key_values(values))

    await db_session.flush()
    return retry_tasks


async def _get_pending_retry_task(db_session: AsyncSession, retry_task_id: int) -> RetryTask:  # pragma: no cover
    return (
        await db_session.execute(
            select(RetryTask)
            .options(selectinload(RetryTask.task_type), noload(RetryTask.task_type_key_values))
            .with_for_update()
            .where(
                RetryTask.retry_task_id == retry_task_id,
                RetryTask.status == RetryTaskStatuses.PENDING,
            )
        )
    ).scalar_one()


async def _get_pending_retry_tasks(db_session: AsyncSession, retry_tasks_ids: list[int]) -> Sequence[RetryTask]:
    retry_tasks_ids_set = set(retry_tasks_ids)
    retry_tasks = (
        (
            await db_session.execute(
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


async def enqueue_retry_task(db_session: AsyncSession, *, retry_task_id: int, connection: "Redis") -> None:
    retry_task = await async_run_query(
        _get_pending_retry_task, db_session, rollback_on_exc=False, retry_task_id=retry_task_id
    )

    def blocking_io() -> None:
        q = rq.Queue(retry_task.task_type.queue_name, connection=connection)
        q.enqueue(
            retry_task.task_type.path,
            retry_task_id=retry_task.retry_task_id,
            failure_ttl=60 * 60 * 24 * 7,  # 1 week
            meta={"error_handler_path": retry_task.task_type.error_handler_path},
        )

    try:
        await asyncio.to_thread(blocking_io)
    except Exception:  # pylint: disable=broad-except
        logger.exception("Failed to enqueue task (retry_task_id=%d)", retry_task_id)


async def enqueue_many_retry_tasks(
    db_session: AsyncSession, *, retry_tasks_ids: list[int], connection: "Redis"
) -> None:
    if not retry_tasks_ids:
        logger.warning(
            "async 'enqueue_many_retry_tasks' expects a list of task's ids but received an empty list instead."
        )
        return

    retry_tasks: list[RetryTask] = await async_run_query(
        _get_pending_retry_tasks, db_session, rollback_on_exc=False, retry_tasks_ids=retry_tasks_ids
    )

    tasks_by_queue: defaultdict[str, list[RetryTask]] = defaultdict(list)
    for task in retry_tasks:
        tasks_by_queue[task.task_type.queue_name].append(task)

    def blocking_io() -> None:
        pipeline = connection.pipeline()
        queued_jobs: list[rq.job.Job] = []
        for queue_name, tasks in tasks_by_queue.items():
            q = rq.Queue(queue_name, connection=connection)
            prepared: list[EnqueueData] = [
                rq.Queue.prepare_data(
                    task.task_type.path,
                    kwargs={"retry_task_id": task.retry_task_id},
                    meta={"error_handler_path": task.task_type.error_handler_path},
                    failure_ttl=60 * 60 * 24 * 7,  # 1 week
                )
                for task in tasks
            ]

            jobs = q.enqueue_many(prepared, pipeline=pipeline)
            queued_jobs.extend(jobs)

        pipeline.execute()
        logger.info(f"Queued {len(queued_jobs)} jobs.")

    try:
        await asyncio.to_thread(blocking_io)
    except Exception:  # pylint: disable=broad-except
        logger.exception("Failed to enqueue tasks (retry_task_ids=%s)", retry_tasks_ids)
