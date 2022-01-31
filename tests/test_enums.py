from datetime import date, datetime, timezone

import pytest

from retry_tasks_lib.enums import TaskParamsKeyTypes, to_bool


def test_to_bool() -> None:
    for value in ("y", "yes", "t", "true", "TRUE", "True", "1"):
        assert to_bool(value) is True

    for value in ("n", "no", "False", "false", "anything", "0", ""):
        assert to_bool(value) is False


def test_task_params_key_type() -> None:
    now = datetime.now(tz=timezone.utc)

    for t, v, r in (
        ("STRING", "sample", "sample"),
        ("INTEGER", "12", 12),
        ("FLOAT", "12", 12.0),
        ("DATE", "2021-08-12", date(day=12, month=8, year=2021)),
        ("DATETIME", now.isoformat(), now),
    ):
        assert getattr(TaskParamsKeyTypes, t).value(v) == r

    for t, v in (
        ("INTEGER", "not a number"),
        ("FLOAT", "not a number"),
        ("DATE", "12/08/21"),
        ("DATETIME", "21:30 12/08/21"),
    ):
        with pytest.raises(ValueError):
            getattr(TaskParamsKeyTypes, t).value(v)
