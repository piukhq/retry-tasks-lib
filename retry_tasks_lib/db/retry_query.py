import logging

from typing import Any, Callable

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
    session: Session,
    *,
    attempts: int = DB_CONNECTION_RETRY_TIMES,
    rollback_on_exc: bool = True,
    **kwargs: Any,
) -> Any:  # pragma: no cover

    while attempts > 0:
        attempts -= 1
        try:
            return fn(db_session=session, **kwargs)
        except DBAPIError as ex:
            logger.debug(f"Attempt failed: {type(ex).__name__} {ex}")
            if rollback_on_exc:
                session.rollback()

            if attempts > 0 and ex.connection_invalidated:
                logger.warning(f"Interrupted transaction: {repr(ex)}, attempts remaining:{attempts}")
            else:
                sentry_sdk.capture_message(f"Max db connection attempts reached: {repr(ex)}")
                raise
        except Exception as ex:
            print(ex)


async def async_run_query(
    fn: Callable,
    session: "AsyncSession",
    *,
    attempts: int = DB_CONNECTION_RETRY_TIMES,
    rollback_on_exc: bool = True,
    **kwargs: Any,
) -> Any:  # pragma: no cover
    while attempts > 0:
        attempts -= 1
        try:
            return await fn(db_session=session, **kwargs)
        except DBAPIError as ex:
            logger.debug(f"Attempt failed: {type(ex).__name__} {ex}")
            if rollback_on_exc:
                await session.rollback()

            if attempts > 0 and ex.connection_invalidated:
                logger.warning(f"Interrupted transaction: {repr(ex)}, attempts remaining:{attempts}")
            else:
                sentry_sdk.capture_message(f"Max db connection attempts reached: {repr(ex)}")
                raise
