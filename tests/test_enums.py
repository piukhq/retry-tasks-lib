from datetime import UTC, datetime

import pytest

from retry_tasks_lib.enums import TaskParamsKeyTypes


def test_task_params_key_type() -> None:
    now = datetime.now(tz=UTC)
    today = now.date()

    for stored_value, expected_value, value_type in (
        ("True", True, TaskParamsKeyTypes.BOOLEAN),
        ("False", False, TaskParamsKeyTypes.BOOLEAN),
        ("Anything Else", False, TaskParamsKeyTypes.BOOLEAN),
        ("2", False, TaskParamsKeyTypes.BOOLEAN),
        ("123", 123, TaskParamsKeyTypes.INTEGER),
        ("12.3", 12.3, TaskParamsKeyTypes.FLOAT),
        ("12", 12.0, TaskParamsKeyTypes.FLOAT),
        ("noice", "noice", TaskParamsKeyTypes.STRING),
        (now.isoformat(), now, TaskParamsKeyTypes.DATETIME),
        (today.isoformat(), today, TaskParamsKeyTypes.DATE),
    ):
        assert value_type.convert_value(stored_value) == expected_value

    for stored_value, value_type in (
        ("not a number", TaskParamsKeyTypes.INTEGER),
        ("not a number", TaskParamsKeyTypes.FLOAT),
        ("21:30 12/08/21", TaskParamsKeyTypes.DATETIME),
        ("12/08/21", TaskParamsKeyTypes.DATE),
    ):
        with pytest.raises(ValueError):
            value_type.convert_value(stored_value)
