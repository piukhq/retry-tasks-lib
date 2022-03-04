import json

from collections import defaultdict
from typing import Optional

from flask import Markup, flash, url_for
from flask_admin.actions import action
from flask_admin.contrib.sqla import ModelView
from redis import Redis
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import enqueue_many_retry_tasks, sync_create_many_tasks


class RetryTaskAdminBase(ModelView):

    endpoint_prefix = ""  # Set this in subclass for url/routing
    redis: Optional[Redis] = None  # Set this in subclass

    form_create_rules = ("task_type",)
    column_exclude_list = ("audit_data",)
    column_filters = (
        "status",
        "task_type.name",
        "task_type_key_values.task_type_key.name",
        "task_type_key_values.value",
    )
    column_searchable_list = ("retry_task_id", "task_type_key_values.value")
    column_display_pk = True
    column_list = (
        "task_type",
        "created_at",
        "updated_at",
        "attempts",
        "audit_data",
        "next_attempt_time",
        "status",
        "params",
    )
    column_details_list = (
        "task_type",
        "created_at",
        "updated_at",
        "attempts",
        "audit_data",
        "next_attempt_time",
        "status",
        "params",
    )
    column_formatters = {
        "params": lambda view, c, model, n: Markup("<p>%s</p>")
        % Markup(
            "".join(
                [
                    '<strong><a href="{0}">{1}</a></strong>: {2}</br>'.format(  # pylint: disable=consider-using-f-string
                        url_for(
                            f"{view.endpoint_prefix}/task-type-key-values.details_view",
                            id=f"{value.retry_task_id},{value.task_type_key_id}",
                        ),
                        value.task_type_key.name,
                        value.value,
                    )
                    for value in sorted(model.task_type_key_values, key=lambda value: value.task_type_key.name)
                ]
            )
        ),
        "audit_data": lambda v, c, model, n: Markup("<pre>%s</pre>")
        % json.dumps(model.audit_data, indent=4, sort_keys=True),
    }

    def get_failed_and_cancelled_tasks(self, ids: list[str]) -> list[RetryTask]:
        return (
            self.session.execute(
                select(RetryTask)
                .options(selectinload(RetryTask.task_type_key_values))
                .with_for_update()
                .where(
                    RetryTask.retry_task_id.in_(ids),
                    RetryTask.status.in_(RetryTaskStatuses.requeueable_statuses_names()),
                )
            )
            .scalars()
            .all()
        )

    def clone_tasks(self, tasks: list[RetryTask]) -> list[RetryTask]:
        tasks_by_type: defaultdict[str, list[RetryTask]] = defaultdict(list)
        new_tasks: list[RetryTask] = []
        for task in tasks:
            tasks_by_type[task.task_type.name].append(task)

        for task_type_name, tasks_to_copy in tasks_by_type.items():
            new_tasks.extend(
                sync_create_many_tasks(
                    db_session=self.session,
                    task_type_name=task_type_name,
                    params_list=[task.get_params() for task in tasks_to_copy],
                )
            )
        return new_tasks

    @action("requeue", "Requeue", "Are you sure you want to requeue selected FAILED and/or CANCELLED tasks?")
    def action_requeue_tasks(self, ids: list[str]) -> None:
        tasks = self.get_failed_and_cancelled_tasks(ids)
        if not tasks:
            flash(f"No relevant {RetryTaskStatuses.requeueable_statuses_names()} tasks to requeue.", category="error")
            return

        for task in tasks:
            task.status = "REQUEUED"

        try:
            new_tasks = self.clone_tasks(tasks)
            for task in new_tasks:
                task.status = RetryTaskStatuses.PENDING
            self.session.add_all(new_tasks)
            self.session.flush()
            enqueue_many_retry_tasks(
                self.session,
                retry_tasks_ids=[task.retry_task_id for task in new_tasks],
                connection=self.redis,
                at_front=True,  # Ensure that requeued tasks get run first
            )
        except Exception as ex:  # pylint: disable=broad-except
            self.session.rollback()
            if not self.handle_view_exception(ex):
                raise
            flash("Failed to requeue selected tasks.", category="error")
        else:
            self.session.commit()
            flash(f"Requeued selected {RetryTaskStatuses.requeueable_statuses_names()} tasks")

    @action(
        "cancel",
        "Cancel",
        "Are you sure you want to cancel the selected tasks? This can break data consistency within our system.",
    )
    def action_cancel_tasks(self, ids: list[str]) -> None:
        required_statuses = RetryTaskStatuses.cancellable_statuses_names()
        tasks = (
            self.session.execute(
                select(RetryTask)
                .options(selectinload(RetryTask.task_type_key_values))
                .with_for_update()
                .where(
                    RetryTask.retry_task_id.in_(ids),
                    RetryTask.status.in_(required_statuses),
                )
            )
            .scalars()
            .all()
        )
        if not tasks:
            flash(
                f"No elegible task to cancel. Tasks must be in one of the following states: {required_statuses}",
                category="error",
            )
            return
        try:
            for task in tasks:
                task.status = "CANCELLED"

            self.session.commit()

        except Exception as ex:  # pylint: disable=broad-except
            self.session.rollback()
            if not self.handle_view_exception(ex):
                raise
            flash("Failed to cancel selected tasks.", category="error")
        else:
            self.session.commit()
            flash("Cancelled selected elegible tasks")


class TaskTypeAdminBase(ModelView):
    column_searchable_list = ("name",)
    column_filters = ("queue_name",)
    form_columns = ("name", "queue_name", "path", "error_handler_path")


class TaskTypeKeyAdminBase(ModelView):
    column_searchable_list = ("name",)
    column_filters = ("task_type.name", "type")
    form_columns = ("task_type", "name", "type")


class TaskTypeKeyValueAdminBase(ModelView):
    column_searchable_list = ("value",)
    column_filters = ("task_type_key.task_type.name", "task_type_key.task_type_key_id", "retry_task.retry_task_id")
