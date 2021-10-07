from os import getenv
from typing import Any, Callable


def get_env(k: str, default: str = None, *, conv: Callable = str) -> Any:
    v = getenv(k, default)
    if v is not None:
        return conv(v)
    else:
        return None


def to_bool(v: str) -> bool:
    if v.lower() in ["true", "1", "t", "yes", "y"]:
        return True
    else:
        return False


DB_CONNECTION_RETRY_TIMES = get_env("DB_CONNECTION_RETRY_TIMES", "3", conv=int)
