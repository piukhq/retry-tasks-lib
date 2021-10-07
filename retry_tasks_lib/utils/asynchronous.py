from typing import Any, Callable

import rq
import sentry_sdk

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.db.retry_query import async_run_query
from retry_tasks_lib.enums import RetryTaskStatuses


async def _get_retry_task(db_session: AsyncSession, retry_task_id: int) -> RetryTask:  # pragma: no cover
    return (
        (
            await db_session.execute(
                select(RetryTask)
                .with_for_update()
                .where(
                    RetryTask.retry_task_id == retry_task_id,
                    RetryTask.retry_status == RetryTaskStatuses.PENDING,
                )
            )
        )
        .scalars()
        .first()
    )


async def _update_status_and_flush(db_session: AsyncSession, retry_task: RetryTask) -> None:
    retry_task.retry_status = RetryTaskStatuses.IN_PROGRESS
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
