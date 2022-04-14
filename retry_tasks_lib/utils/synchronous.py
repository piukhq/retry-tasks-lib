from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import wraps
from random import randint
from typing import Any, Callable, TypedDict

import rq

from rq.queue import EnqueueData
from sentry_sdk import Hub, start_span
from sentry_sdk.tracing import Span
from sqlalchemy import and_, distinct, func, or_
from sqlalchemy.future import select
from sqlalchemy.orm import Session, joinedload, noload, selectinload, sessionmaker
from sqlalchemy.sql.sqltypes import String

from retry_tasks_lib import logger
from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKey, TaskTypeKeyValue
from retry_tasks_lib.db.retry_query import sync_run_query
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.settings import DEFAULT_FAILURE_TTL, USE_NULL_POOL

DelayRangeType = TypedDict(
    "DelayRangeType",
    {
        "min": int,
        "max": int,
    },
)


class IncorrectRetryTaskStatusError(Exception):
    pass


@dataclass
class RetryTaskAdditionalQueryData:
    """
    Class for encapsulating the data to be used in additional queries to
    identify additional tasks to be found when considering whether the task to
    be run can run in isolation in the context of this task type.

    `matching_val_keys`: Provide this list to match one task with another.
    e.g. providing ["account_holder_uuid", "campaign_slug"] will find other
    tasks of the same type with matching TaskTypeKeyValues for these TaskTypeKey
    names.

    `additional_statuses`: RetryTasks with these additional statuses will be
    considered. PENDING and IN_PROGRESS tasks are considered by default
    """

    matching_val_keys: list[str]
    additional_statuses: list[RetryTaskStatuses] | None = None


def _get_additional_task_ids(
    db_session: "Session",
    retry_task_id: int,
    additional_query_datas: list[RetryTaskAdditionalQueryData],
    sentry_span: Span,
) -> list[int]:
    with sentry_span.start_child(op="build-additional-filters"):

        # Note that this object is not FOR UPDATE locked as it is merely used for its related data
        def _fetch_task_to_run(db_session: Session) -> RetryTask:
            return (
                db_session.execute(
                    select(RetryTask)
                    .options(joinedload(RetryTask.task_type_key_values), joinedload(RetryTask.task_type))
                    .where(RetryTask.retry_task_id == retry_task_id)
                )
                .unique()
                .scalar_one()
            )

        task_to_run: RetryTask = sync_run_query(_fetch_task_to_run, db_session)

        task_ids: list[int] = []
        for query_data in additional_query_datas:
            statuses = set(query_data.additional_statuses or [])
            statuses.update({RetryTaskStatuses.PENDING, RetryTaskStatuses.IN_PROGRESS})

            def _fetch_task_ids(
                db_session: "Session", query_data: RetryTaskAdditionalQueryData, statuses: RetryTaskStatuses
            ) -> list[int]:
                key_expressions = [
                    or_(
                        *[
                            *[
                                and_(
                                    TaskTypeKey.name == k,
                                    TaskTypeKeyValue.value == func.cast(v, String),
                                )
                                for (k, v) in task_to_run.get_params().items()
                                if k in query_data.matching_val_keys
                            ],
                        ],
                    )
                ]
                return (
                    db_session.execute(
                        select(TaskTypeKeyValue.retry_task_id)
                        .where(
                            TaskTypeKeyValue.retry_task_id == RetryTask.retry_task_id,
                            TaskTypeKeyValue.task_type_key_id == TaskTypeKey.task_type_key_id,
                            and_(
                                RetryTask.status.in_(statuses),
                                RetryTask.task_type_id == task_to_run.task_type_id,
                                *key_expressions,
                            ),
                        )
                        .group_by(TaskTypeKeyValue.retry_task_id)
                        .having(func.count(distinct(TaskTypeKeyValue.value)) == len(query_data.matching_val_keys))
                    )
                    .scalars()
                    .all()
                )

            task_ids.extend(
                sync_run_query(
                    _fetch_task_ids,
                    db_session,
                    query_data=query_data,
                    statuses=statuses,
                    rollback_on_exc=False,
                )
            )

    return task_ids


def _route_task_execution(
    db_session: "Session",
    *,
    retry_task_id: int,
    retry_tasks: list[RetryTask],
    redis_connection: Any,
    requeue_delay_range: DelayRangeType,
    sentry_span: Span,
) -> tuple[bool, RetryTask]:

    with sentry_span.start_child(op="can-run-task-now-logic"):
        this_task = None
        can_run = False
        other_non_pending = []
        for task in retry_tasks:
            if task.retry_task_id == retry_task_id:
                this_task = task  # this object is the FOR UPDATE locked one
            elif task.status != RetryTaskStatuses.PENDING:
                other_non_pending.append(task)

        if not this_task:
            raise ValueError("retry_task is unexpectedly None")

        if this_task.status == RetryTaskStatuses.CANCELLED:
            logger.info(f"Task with retry_task_id {retry_task_id} has been cancelled. Removing from queue.")
            return False, this_task

        if this_task.status not in (
            RetryTaskStatuses.PENDING,
            RetryTaskStatuses.RETRYING,
            RetryTaskStatuses.WAITING,
        ):
            raise IncorrectRetryTaskStatusError(
                f"task with retry_task_id {retry_task_id} is in incorrect state ({this_task.status})"
            )

        if other_non_pending:
            logger.info(
                f"Non-PENDING tasks returned for query "
                f"{','.join([str(rt.retry_task_id) for rt in other_non_pending])}: "
                f"Requeueing task with retry_task_id {retry_task_id}"
            )
            next_attempt_time = enqueue_retry_task_delay(
                connection=redis_connection,
                retry_task=this_task,
                delay_seconds=randint(requeue_delay_range["min"], requeue_delay_range["max"]),
            )
            this_task.update_task(db_session, increase_attempts=False, next_attempt_time=next_attempt_time)
        else:
            this_task.update_task(db_session, status=RetryTaskStatuses.IN_PROGRESS, increase_attempts=True)
            can_run = True

    return can_run, this_task


def retryable_task(
    *,
    db_session_factory: sessionmaker,
    exclusive_constraints: list[RetryTaskAdditionalQueryData] | None = None,
    redis_connection: Any = None,
    requeue_delay_range: DelayRangeType | None = None,
) -> Callable:
    """
    Decorator for retry task functions which will move the task to be run into
    IN_PROGRESS state and increment `attempts` prior to running the task
    function and blocking the task from running if other relevant tasks are
    found.

    It wraps the task function accepting a retry_task_id and retrieving the
    relevant RetryTask ORM object and providing that as an argument to the task
    function itself, along with a database session.

    The decorator has two functions:

    1) To change the status of the task to IN_PROGRESS prior to its running
    2) To block the task and requeue it in the future if retry tasks other than
    the one belonging to the retry_task_id passed in are found by means of the
    subquery datas provided in `exclusive_constraints`.

    Parameters:

        `db_session_factory`:
        a SQLAlchemy sessionmaker instance to create the db_session

        `exclusive_constraints`:
        when providing a list of these, each will generate an additional
        RetryTask subquery. If retry tasks other than the one belonging to
        retry_task_id are retrived in non-PENDING state, then the task intended
        to be run is requeued `delay_seconds` in the future

        `redis_connection`:
        an optional Redis connection instance used to re-enqueue the task in the
        future. Provide this when also providing `exclusive_constraints`

        `requeue_delay_range`:
        a tuple of two integers defining an interval between which a task that has been
        blocked from running should be requeued (jitter).
    """
    requeue_delay_range = requeue_delay_range or {"min": 5, "max": 20}
    parent_span = Hub.current.scope.span
    trace: Callable
    if parent_span is None:
        trace = start_span
    else:
        trace = parent_span.start_child

    with trace(op="check-exclusive-constraints"):
        if exclusive_constraints:
            # it is important that we check this due to the .having clause below on the subquery
            for subq_data in exclusive_constraints:
                if len(subq_data.matching_val_keys) != len(set(subq_data.matching_val_keys)):
                    raise ValueError(f"matching_val_keys has duplicates: {subq_data.matching_val_keys}")

    def decorator(task_func: Callable) -> Callable:
        @wraps(task_func)
        def wrapper(retry_task_id: int) -> None:

            with trace(op="rq-tasks-decorator") as span:

                # this is to prevent session pool sharing between parent and forked process:
                # https://docs.sqlalchemy.org/en/14/core/pooling.html#using-connection-pools-with-multiprocessing-or-os-fork
                if not USE_NULL_POOL:
                    db_session_factory.kw["bind"].dispose(close=False)

                with db_session_factory() as db_session:

                    eligible_task_ids: set = {retry_task_id}
                    if exclusive_constraints:
                        suffix = "with-exclusive-constraints"
                        eligible_task_ids.update(
                            _get_additional_task_ids(db_session, retry_task_id, exclusive_constraints, span)
                        )
                    else:
                        suffix = "without-exclusive-constraints"

                    with span.start_child(op=f"fetch-task-data-{suffix}"):

                        retry_tasks: list[RetryTask] = sync_run_query(
                            lambda db_session: (
                                db_session.execute(
                                    select(RetryTask)
                                    .options(selectinload(RetryTask.task_type_key_values))
                                    .where(RetryTask.retry_task_id.in_(eligible_task_ids))
                                    .with_for_update()
                                )
                                .scalars()
                                .all()
                            ),
                            db_session,
                        )

                    can_run, task_to_run = _route_task_execution(
                        db_session=db_session,
                        retry_task_id=retry_task_id,
                        retry_tasks=retry_tasks,
                        redis_connection=redis_connection,
                        requeue_delay_range=requeue_delay_range,  # type: ignore [arg-type]
                        sentry_span=span,
                    )

                if can_run:
                    with trace(op="decorated-retry-task"):
                        task_func(task_to_run, db_session)

        return wrapper

    return decorator


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
    next_attempt_time = datetime.now(tz=timezone.utc) + timedelta(seconds=delay_seconds)
    job = q.enqueue_at(  # requires rq worker --with-scheduler
        next_attempt_time,
        retry_task.task_type.path,
        retry_task_id=retry_task.retry_task_id,
        failure_ttl=failure_ttl,
        meta={"error_handler_path": retry_task.task_type.error_handler_path},
    )

    logger.info(f"Enqueued task for execution at {next_attempt_time.isoformat()}: {job}")
    return next_attempt_time


def enqueue_retry_task(
    *, connection: Any, retry_task: RetryTask, failure_ttl: int = DEFAULT_FAILURE_TTL, at_front: bool = False
) -> rq.job.Job:
    q = rq.Queue(retry_task.task_type.queue_name, connection=connection)
    job = q.enqueue(
        retry_task.task_type.path,
        retry_task_id=retry_task.retry_task_id,
        failure_ttl=failure_ttl,
        at_front=at_front,
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


def enqueue_many_retry_tasks(
    db_session: Session,
    *,
    retry_tasks_ids: list[int],
    connection: Any,
    failure_ttl: int = DEFAULT_FAILURE_TTL,
    at_front: bool = False,
) -> None:

    if not retry_tasks_ids:
        logger.warning("'enqueue_many_retry_tasks' expects a list of task's ids but received an empty list instead.")
        return

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
                    failure_ttl=failure_ttl,
                    at_front=at_front,
                )
            )

        jobs = q.enqueue_many(prepared, pipeline=pipeline)
        queued_jobs.extend(jobs)

    pipeline.execute()
    logger.info(f"Queued {len(queued_jobs)} jobs.")
