import logging

from collections.abc import Callable
from typing import Any

import sentry_sdk

from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from retry_tasks_lib.settings import DB_CONNECTION_RETRY_TIMES

logger = logging.getLogger("sqlalchemy-queries")


# based on the following stackoverflow answer:
# https://stackoverflow.com/a/30004941
def sync_run_query(
    fn: Callable,
    db_session: Session,
    *,
    attempts: int = DB_CONNECTION_RETRY_TIMES,
    rollback_on_exc: bool = True,
    **kwargs: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401

    while attempts > 0:
        attempts -= 1
        try:
            return fn(db_session=db_session, **kwargs)
        except DBAPIError as ex:
            logger.debug(f"Attempt failed: {type(ex).__name__} {ex}")
            if rollback_on_exc:
                db_session.rollback()

            if attempts > 0 and ex.connection_invalidated:
                logger.warning(f"Interrupted transaction: {ex!r}, attempts remaining:{attempts}")
            else:
                sentry_sdk.capture_message(f"Max db connection attempts reached: {ex!r}")
                raise


async def async_run_query(
    fn: Callable,
    db_session: AsyncSession,
    *,
    attempts: int = DB_CONNECTION_RETRY_TIMES,
    rollback_on_exc: bool = True,
    **kwargs: Any,  # noqa: ANN401,
) -> Any:  #   # noqa: ANN401
    while attempts > 0:
        attempts -= 1
        try:
            return await fn(db_session=db_session, **kwargs)
        except DBAPIError as ex:
            logger.debug(f"Attempt failed: {type(ex).__name__} {ex}")
            if rollback_on_exc:
                await db_session.rollback()

            if attempts > 0 and ex.connection_invalidated:
                logger.warning(f"Interrupted transaction: {ex!r}, attempts remaining:{attempts}")
            else:
                sentry_sdk.capture_message(f"Max db connection attempts reached: {ex!r}")
                raise
