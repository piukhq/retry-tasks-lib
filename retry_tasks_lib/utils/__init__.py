from importlib import import_module
from typing import Callable


class UnresolvableHandlerPath(Exception):
    pass


def resolve_callable_from_path(path: str) -> Callable:
    try:
        mod, func = path.rsplit(".", 1)
        module = import_module(name=mod)
        handler: Callable = getattr(module, func)
        return handler
    except (ValueError, ModuleNotFoundError, AttributeError) as ex:
        raise UnresolvableHandlerPath(f"Could resolve callable for path {ex}")  # pylint: disable=raise-missing-from
