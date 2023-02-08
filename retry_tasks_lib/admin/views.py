import json

from collections import defaultdict

from flask import Markup, flash, url_for
from flask_admin import Admin
from flask_admin.actions import action
from flask_admin.contrib.sqla import ModelView
from redis import Redis
from sqlalchemy.future import select
from sqlalchemy.orm import Session, selectinload

from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKey, TaskTypeKeyValue
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import enqueue_many_retry_tasks, sync_create_many_tasks


class RetryTaskAdminBase(ModelView):

    redis: Redis | None = None  # Set this in subclass

    form_create_rules = ("task_type",)
    form_edit_rules = ("attempts",)
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
    form_ajax_refs = {
        "task_type": {
            "fields": ("task_type_id",),
            "placeholder": "Please select",
            "page_size": 10,
            "minimum_input_length": 0,
        }
    }
    column_formatters = {
        "params": lambda view, c, model, n: Markup("<p>%s</p>")
        % Markup(
            "".join(
                [
                    '<strong><a href="{0}">{1}</a></strong>: {2}</br>'.format(  # pylint: disable=consider-using-f-string
                        url_for(
                            f"{view.endpoint_prefix or ''}task-type-key-values.details_view",
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
        "Clean up and cancel",
        "Are you sure you want to cancel the selected tasks? This can break data consistency within our system.",
    )
    def action_cancel_tasks(self, ids: list[str]) -> None:
        required_statuses = RetryTaskStatuses.cancellable_statuses_names()
        tasks: list[RetryTask] = (
            self.session.execute(
                select(RetryTask)
                .options(selectinload(RetryTask.task_type_key_values))
                .options(selectinload(RetryTask.task_type))
                .with_for_update()
                .where(
                    RetryTask.retry_task_id.in_(ids),
                    RetryTask.status.in_(required_statuses),
                )
                .order_by(RetryTask.retry_task_id)
            )
            .scalars()
            .all()
        )
        if not tasks:
            flash(
                f"No elegible task to cancel. Tasks must be in one of the following states: {required_statuses}",
                category="error",
            )

        else:
            cleanup_tasks = []
            for task in tasks:
                if task.task_type.cleanup_handler_path:
                    task.status = RetryTaskStatuses.CLEANUP
                    cleanup_tasks.append(task)
                else:
                    task.status = RetryTaskStatuses.CANCELLED
                    msg = "Cancelled selected elegible tasks"

            if cleanup_tasks:
                enqueue_many_retry_tasks(
                    db_session=self.session,
                    connection=self.redis,
                    retry_tasks_ids=[task.retry_task_id for task in cleanup_tasks],
                    use_cleanup_hanlder_path=True,
                    use_task_type_exc_handler=False,
                )
                msg = "Started clean up jobs for selected tasks"
            try:
                self.session.commit()
                flash(msg)
            except Exception as ex:  # pylint: disable=broad-except
                self.session.rollback()
                if not self.handle_view_exception(ex):
                    raise
                flash("Failed to cancel selected tasks.", category="error")


class TaskTypeAdminBase(ModelView):
    column_searchable_list = ("name",)
    column_filters = ("queue_name",)
    form_columns = ("name", "queue_name", "path", "error_handler_path")


class TaskTypeKeyAdminBase(ModelView):
    column_searchable_list = ("name",)
    column_filters = ("task_type.name", "type")
    form_columns = ("task_type", "name", "type")
    form_ajax_refs = {
        "task_type": {
            "fields": ("task_type_id",),
            "placeholder": "Please select",
            "page_size": 10,
            "minimum_input_length": 0,
        }
    }


class TaskTypeKeyValueAdminBase(ModelView):
    column_searchable_list = ("value",)
    column_filters = ("task_type_key.task_type.name", "task_type_key.task_type_key_id", "retry_task.retry_task_id")
    form_ajax_refs = {
        "task_type_key": {
            "fields": ("task_type_key_id",),
            "placeholder": "Please select",
            "page_size": 10,
            "minimum_input_length": 0,
        },
        "retry_task": {
            "fields": ("retry_task_id",),
            "placeholder": "Please select",
            "page_size": 10,
            "minimum_input_length": 0,
        },
    }


def register_tasks_admin(
    *,
    admin: "Admin",
    scoped_db_session: "Session",
    redis: "Redis",
    admin_base_classes: tuple = (),
    url_prefix: str | None = None,
    endpoint_prefix: str | None = None,
    menu_title: str = "Tasks",
) -> None:
    redis_ = redis
    base_class = type("TaskBase", admin_base_classes, {"endpoint_prefix": endpoint_prefix})

    class TaskAdmin(RetryTaskAdminBase, base_class):  # type: ignore
        redis = redis_

    admin.add_view(
        TaskAdmin(
            RetryTask,
            scoped_db_session,
            "Tasks",
            endpoint=f"{endpoint_prefix or ''}retry-tasks",
            url=f'{f"{url_prefix}/" if url_prefix else ""}tasks',
            category=menu_title,
        )
    )

    class TaskTypeAdmin(TaskTypeAdminBase, base_class):  # type: ignore
        pass

    admin.add_view(
        TaskTypeAdmin(
            TaskType,
            scoped_db_session,
            "Task Types",
            endpoint=f"{endpoint_prefix or ''}task-types",
            url=f'{f"{url_prefix}/" if url_prefix else ""}task-types',
            category=menu_title,
        )
    )

    class TaskTypeKeyAdmin(TaskTypeKeyAdminBase, base_class):  # type: ignore
        pass

    admin.add_view(
        TaskTypeKeyAdmin(
            TaskTypeKey,
            scoped_db_session,
            "Task Type Keys",
            endpoint=f"{endpoint_prefix or ''}task-type-keys",
            url=f'{f"{url_prefix}/" if url_prefix else ""}task-type-keys',
            category=menu_title,
        )
    )

    class TaskTypeKeyValueAdmin(TaskTypeKeyValueAdminBase, base_class):  # type: ignore
        pass

    admin.add_view(
        TaskTypeKeyValueAdmin(
            TaskTypeKeyValue,
            scoped_db_session,
            "Task Type Key Values",
            endpoint=f"{endpoint_prefix or ''}task-type-key-values",
            url=f'{f"{url_prefix}/" if url_prefix else ""}task-type-key-values',
            category=menu_title,
        )
    )
