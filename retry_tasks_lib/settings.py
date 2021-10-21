from os import getenv
from typing import Any, Callable


def get_env(k: str, default: str = None, *, conv: Callable = str) -> Any:  # pragma: no cover
    v = getenv(k, default)
    if v is not None:
        return conv(v)
    else:
        return None


def to_bool(v: str) -> bool:  # pragma: no cover
    value = v.lower()
    if value not in ["true", "false"]:
        raise ValueError("invalid value for a boolean.")
    else:
        return value == "true"


DB_CONNECTION_RETRY_TIMES = get_env("DB_CONNECTION_RETRY_TIMES", "3", conv=int)
DEFAULT_FAILURE_TTL = 60 * 60 * 24 * 7  # 1 week
