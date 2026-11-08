"""Explicit plugin factories; config strings never become imports or executable code."""

from collections.abc import Callable
from typing import Generic, TypeVar

from pairity.contracts import API_VERSION

T = TypeVar("T")


def require_api(plugin):
    if getattr(plugin, "api_version", None) != API_VERSION:
        raise ValueError(f"Plugin requires Pairity contract version {API_VERSION}")
    return plugin


class PluginRegistry(Generic[T]):
    """Use one registry per contract, and register trusted factories in application code."""

    def __init__(self):
        self._factories: dict[str, Callable[[], T]] = {}

    def register(self, name: str, factory: Callable[[], T]) -> None:
        if not isinstance(name, str) or not name.strip() or not callable(factory):
            raise ValueError("A plugin needs a name and callable factory")
        if name in self._factories:
            raise ValueError(f"Plugin already registered: {name}")
        self._factories[name] = factory

    def __contains__(self, name: object) -> bool:
        return name in self._factories

    def create(self, name: str) -> T:
        if name not in self._factories:
            raise ValueError(f"Unknown plugin: {name}")
        return require_api(self._factories[name]())
