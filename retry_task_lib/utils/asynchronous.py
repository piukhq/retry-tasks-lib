from typing import Any, Callable

import rq
import sentry_sdk
from retry_task_lib.db.models import RetryTask
from retry_task_lib.db.retry_query import async_run_query
from retry_task_lib.enums import QueuedRetryStatuses
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select


async def enqueue_retry_task(
    async_db_session: AsyncSession, retry_task_id: int, action: Callable, queue: str, connection: Any
) -> None:
    async def _get_retry_task() -> RetryTask:
        return (
            (
                await async_db_session.execute(
                    select(RetryTask)
                    .with_for_update()
                    .where(
                        RetryTask.retry_task_id == retry_task_id,
                        RetryTask.retry_status == QueuedRetryStatuses.PENDING,
                    )
                )
            )
            .scalars()
            .first()
        )

    async def _update_status_and_flush() -> None:
        retry_task.retry_status = QueuedRetryStatuses.IN_PROGRESS
        await async_db_session.flush()

    async def _commit() -> None:
        await async_db_session.commit()

    async def _rollback() -> None:
        await async_db_session.rollback()

    try:
        q = rq.Queue(queue, connection=connection)
        retry_task = await async_run_query(_get_retry_task, async_db_session, rollback_on_exc=False)
        await async_run_query(_update_status_and_flush, async_db_session)
        q.enqueue(
            action,
            retry_task_id=retry_task_id,
            failure_ttl=60 * 60 * 24 * 7,  # 1 week
        )
        await async_run_query(_commit, async_db_session, rollback_on_exc=False)
    except Exception as ex:
        sentry_sdk.capture_exception(ex)
        await async_run_query(_rollback, async_db_session, rollback_on_exc=False)
