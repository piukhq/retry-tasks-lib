from collections.abc import Callable
from importlib import import_module


class UnresolvableHandlerPathError(Exception):
    pass


def resolve_callable_from_path(path: str) -> Callable:
    try:
        mod, func = path.rsplit(".", 1)
        module = import_module(name=mod)
        handler: Callable = getattr(module, func)
        return handler
    except (ValueError, ModuleNotFoundError, AttributeError) as ex:
        raise UnresolvableHandlerPathError(f"Could resolve callable for path {ex}") from ex
