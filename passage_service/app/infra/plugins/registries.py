from typing import Generic, TypeVar

from app.domain.models.plugin_contracts import BaseBuilder, BaseController, BaseTagger


T = TypeVar("T")


class BaseRegistry(Generic[T]):
    def __init__(self) -> None:
        self._items: dict[str, T] = {}

    def register(self, plugin: T) -> None:
        name = getattr(plugin, "name")
        if name in self._items:
            raise ValueError(f"duplicate plugin registration: {name}")
        self._items[name] = plugin

    def get(self, name: str) -> T:
        return self._items[name]

    def list(self) -> list[str]:
        return list(self._items.keys())


class TaggerRegistry(BaseRegistry[BaseTagger]):
    pass


class ControllerRegistry(BaseRegistry[BaseController]):
    pass


class BuilderRegistry(BaseRegistry[BaseBuilder]):
    pass


tagger_registry = TaggerRegistry()
controller_registry = ControllerRegistry()
builder_registry = BuilderRegistry()
