import json

from typing import Optional

import rq

from flask import Markup, flash, url_for
from flask_admin.actions import action
from flask_admin.contrib.sqla import ModelView
from redis import Redis
from sqlalchemy.future import select
from sqlalchemy.orm import lazyload

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.utils.synchronous import sync_create_task


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
                    '<strong><a href="{0}">{1}</a></strong>: {2}</br>'.format(
                        url_for(
                            f"{view.endpoint_prefix}/task-type-key-values.details_view",
                            id=f"{value.retry_task_id},{value.task_type_key_id}",
                        ),
                        value.task_type_key.name,
                        value.value,
                    )
                    for value in model.task_type_key_values
                ]
            )
        ),
        "audit_data": lambda v, c, model, n: Markup("<pre>%s</pre>")
        % json.dumps(model.audit_data, indent=4, sort_keys=True),
    }

    @action("requeue", "Requeue", "Are you sure you want to requeue selected FAILED tasks?")
    def action_requeue_tasks(self, ids: list[str]) -> None:
        tasks = (
            self.session.execute(
                select(RetryTask)
                .options(lazyload(RetryTask.task_type_key_values))
                .with_for_update()
                .where(RetryTask.retry_task_id.in_(ids))
                .where(RetryTask.status == "FAILED")
            )
            .scalars()
            .all()
        )
        if tasks:
            new_tasks: list[RetryTask] = []
            try:
                for task in tasks:
                    new_task = sync_create_task(
                        db_session=self.session, task_type_name=task.task_type.name, params=task.get_params()
                    )
                    task.status = "REQUEUED"
                    new_task.status = "IN_PROGRESS"
                    new_tasks.append(new_task)

                self.session.add_all(new_tasks)
                self.session.flush()
                q = rq.Queue(task.task_type.queue_name, connection=self.redis)
                jobs = q.enqueue_many(
                    [
                        rq.Queue.prepare_data(
                            task.task_type.path,
                            kwargs={"retry_task_id": task.retry_task_id},
                        )
                        for task in new_tasks
                    ]
                )
            except Exception as ex:
                self.session.rollback()
                if not self.handle_view_exception(ex):
                    raise
                flash("Failed to requeue selected tasks.", category="error")
            else:
                self.session.commit()
                flash(f"Requeued {len(jobs)} FAILED tasks")
        else:
            flash("No relevant (FAILED) tasks to requeue.", category="error")


class TaskTypeAdminBase(ModelView):
    column_searchable_list = ("name",)
    form_columns = ("name", "path")


class TaskTypeKeyAdminBase(ModelView):
    column_searchable_list = ("name",)
    column_filters = ("task_type.name", "type")
    form_columns = ("task_type", "name", "type")


class TaskTypeKeyValueAdminBase(ModelView):
    column_searchable_list = ("value",)
    column_filters = ("task_type_key.task_type.name", "task_type_key.task_type_key_id", "retry_task.retry_task_id")
