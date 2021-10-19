from typing import Any, Callable

import rq
import sentry_sdk

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.db.retry_query import async_run_query
from retry_tasks_lib.enums import RetryTaskStatuses

from . import logger


async def _get_retry_task(db_session: AsyncSession, retry_task_id: int) -> RetryTask:  # pragma: no cover
    return (
        (
            await db_session.execute(
                select(RetryTask)
                .with_for_update()
                .where(
                    RetryTask.retry_task_id == retry_task_id,
                    RetryTask.status == RetryTaskStatuses.PENDING,
                )
            )
        )
        .scalars()
        .first()
    )


async def _get_retry_tasks(db_session: AsyncSession, retry_tasks_ids: list[int]) -> list[RetryTask]:  # pragma: no cover
    retry_tasks_ids_set = set(retry_tasks_ids)
    retry_tasks = (
        (
            await db_session.execute(
                select(RetryTask)
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
            f"Error fetching all the RetryTasks requested for enqueuing. Requested RetryTaks ids: {retry_tasks_ids_set}"
        )

    if missing_ids:
        logger.error(f"Error fetching some RetryTasks requested for enqueuing. Missing RetryTaks ids: {missing_ids}")

    return retry_tasks


async def _update_status_and_flush(db_session: AsyncSession, retry_task: RetryTask) -> None:
    retry_task.status = RetryTaskStatuses.IN_PROGRESS
    await db_session.flush()


async def _update_many_statuses_and_flush(db_session: AsyncSession, retry_tasks: list[RetryTask]) -> None:

    # updating statuses with a loop instead of using db_session.execute(update(...)) to take advantage of
    # the .with_for_update() option we used when fetching these object from the db.
    for retry_task in retry_tasks:
        retry_task.status = RetryTaskStatuses.IN_PROGRESS

    await db_session.flush()


async def _commit(db_session: AsyncSession) -> None:
    await db_session.commit()


async def _rollback(db_session: AsyncSession) -> None:
    await db_session.rollback()


async def enqueue_retry_task(
    db_session: AsyncSession, retry_task_id: int, action: Callable, queue: str, connection: Any
) -> None:

    try:
        q = rq.Queue(queue, connection=connection)
        retry_task = await async_run_query(
            _get_retry_task, db_session, rollback_on_exc=False, retry_task_id=retry_task_id
        )
        await async_run_query(_update_status_and_flush, db_session, retry_task=retry_task)
        q.enqueue(
            action,
            retry_task_id=retry_task_id,
            failure_ttl=60 * 60 * 24 * 7,  # 1 week
        )
        await async_run_query(_commit, db_session, rollback_on_exc=False)
    except Exception as ex:
        sentry_sdk.capture_exception(ex)
        await async_run_query(_rollback, db_session, rollback_on_exc=False)


async def enqueue_many_retry_tasks(
    db_session: AsyncSession,
    retry_tasks_ids: list[int],
    action: Callable,
    queue: str,
    connection: Any,
) -> None:
    try:
        q = rq.Queue(queue, connection=connection)
        retry_tasks: list[RetryTask] = await async_run_query(
            _get_retry_tasks, db_session, rollback_on_exc=False, retry_task_id=retry_tasks_ids
        )

        await async_run_query(_update_many_statuses_and_flush, db_session, retry_tasks=retry_tasks)
        q.enqueue_many(
            [
                q.prepare_data(
                    action,
                    retry_task_id=retry_task.retry_task_id,
                    failure_ttl=60 * 60 * 24 * 7,  # 1 week
                )
                for retry_task in retry_tasks
            ]
        )
        await async_run_query(_commit, db_session, rollback_on_exc=False)
    except Exception as ex:
        sentry_sdk.capture_exception(ex)
        await async_run_query(_rollback, db_session, rollback_on_exc=False)
