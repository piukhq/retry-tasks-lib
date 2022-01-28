from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Callable, Optional

import rq

from rq.queue import EnqueueData
from sqlalchemy import and_, distinct, func, or_
from sqlalchemy.future import select
from sqlalchemy.orm import Session, joinedload, noload, selectinload, sessionmaker
from sqlalchemy.sql.selectable import subquery
from sqlalchemy.sql.sqltypes import String

from retry_tasks_lib import logger
from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKey, TaskTypeKeyValue
from retry_tasks_lib.db.retry_query import sync_run_query
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.settings import DEFAULT_FAILURE_TTL


class IncorrectRetryTaskStatusError(Exception):
    pass


@dataclass
class RetryTaskAdditionalSubqueryData:
    """
    Class for encapsulating the data to be used in additional subqueries to
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
    additional_statuses: Optional[list[RetryTaskStatuses]] = None


def retryable_task(
    *,
    db_session_factory: sessionmaker,
    exclusive_constraints: Optional[list[RetryTaskAdditionalSubqueryData]] = None,
    redis_connection: Optional[Any] = None,
    delay_seconds: int = 60,
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

        `delay_seconds`:
        the number of seconds in the future to requeue a task that has been
        blocked from running.
    """
    if exclusive_constraints:
        # it is important that we check this due to the .having clause below on the subquery
        for subq_data in exclusive_constraints:
            if len(subq_data.matching_val_keys) != len(set(subq_data.matching_val_keys)):
                raise ValueError(f"matching_val_keys has duplicates: {subq_data.matching_val_keys}")

    def get_subqueries(task_to_run: RetryTask, subquery_datas: list[RetryTaskAdditionalSubqueryData]) -> subquery:
        subqueries = []
        for subquery_data in subquery_datas:
            statuses = set(subq_data.additional_statuses or [])
            statuses.update({RetryTaskStatuses.PENDING, RetryTaskStatuses.IN_PROGRESS})
            key_expressions = [
                or_(
                    *[
                        *[
                            and_(
                                TaskTypeKey.name == k,
                                TaskTypeKeyValue.value == func.cast(v, String),
                            )
                            for (k, v) in task_to_run.get_params().items()
                            if k in subquery_data.matching_val_keys
                        ],
                    ],
                )
            ]
            subq = (
                select(TaskTypeKeyValue.retry_task_id)
                .join(TaskTypeKeyValue.task_type_key)
                .join(TaskTypeKey.task_type)
                .join(RetryTask, RetryTask.retry_task_id == TaskTypeKeyValue.retry_task_id)
                .where(
                    and_(
                        RetryTask.status.in_(statuses),
                        RetryTask.task_type_id == task_to_run.task_type_id,
                        *key_expressions,
                    ),
                )
                .group_by(TaskTypeKeyValue.retry_task_id)
                .having(func.count(distinct(TaskTypeKeyValue.value)) == len(subquery_data.matching_val_keys))
                .subquery()
            )
            subqueries.append(subq)
        return subqueries

    def decorater(task_func: Callable) -> Callable:
        @wraps(task_func)
        def wrapper(retry_task_id: int) -> None:
            with db_session_factory() as db_session:
                subqueries = []
                if exclusive_constraints:
                    # Note that this object is not FOR UPDATE locked as it is merely used for its related data
                    task_to_run = (
                        db_session.execute(
                            select(RetryTask)
                            .options(joinedload(RetryTask.task_type_key_values), joinedload(RetryTask.task_type))
                            .where(RetryTask.retry_task_id == retry_task_id)
                        )
                        .unique()
                        .scalar_one()
                    )
                    subqueries = get_subqueries(task_to_run, exclusive_constraints)

                query = (
                    select(RetryTask)
                    .options(selectinload(RetryTask.task_type_key_values))
                    .where(
                        or_(
                            *[RetryTask.retry_task_id.in_(select(sq)) for sq in subqueries]
                            + [RetryTask.retry_task_id == retry_task_id],  # + the task to run
                        )
                    )
                    .with_for_update()
                )

                retry_tasks = db_session.execute(query).unique().scalars().all()

                this_task = None
                other_non_pending = []
                for rt in retry_tasks:
                    if rt.retry_task_id == retry_task_id:
                        this_task = rt  # this object is the FOR UPDATE locked one
                    elif rt.status != RetryTaskStatuses.PENDING:
                        other_non_pending.append(rt)

                if not this_task:
                    raise ValueError("retry_task is unexpectedly None")
                elif this_task.status not in (
                    RetryTaskStatuses.PENDING,
                    RetryTaskStatuses.RETRYING,
                    RetryTaskStatuses.WAITING,
                ):
                    raise IncorrectRetryTaskStatusError(
                        f"task with retry_task_id {retry_task_id} is in incorrect state ({this_task.status})"
                    )
                elif other_non_pending:
                    logger.info(
                        f"Non-PENDING tasks returned for query "
                        f"{','.join([str(rt.retry_task_id) for rt in other_non_pending])}: "
                        f"Requeueing task with retry_task_id {retry_task_id}"
                    )
                    next_attempt_time = enqueue_retry_task_delay(
                        connection=redis_connection, retry_task=this_task, delay_seconds=delay_seconds
                    )
                    this_task.update_task(db_session, increase_attempts=False, next_attempt_time=next_attempt_time)
                else:
                    this_task.update_task(db_session, status=RetryTaskStatuses.IN_PROGRESS, increase_attempts=True)
                    return task_func(this_task, db_session)

        return wrapper

    return decorater


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


def enqueue_many_retry_tasks(
    db_session: Session, *, retry_tasks_ids: list[int], connection: Any, at_front: bool = False
) -> None:
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
                    at_front=at_front,
                )
            )

        jobs = q.enqueue_many(prepared, pipeline=pipeline)
        queued_jobs.extend(jobs)

    pipeline.execute()
    logger.info(f"Queued {len(queued_jobs)} jobs.")
