from importlib import import_module

from app.core.config import get_config_bundle
from app.infra.plugins.registries import builder_registry, controller_registry, tagger_registry


def _load_from_entries(entries: list[dict], registry) -> None:
    for entry in entries:
        if not entry.get("enabled", True):
            continue
        if entry["name"] in registry.list():
            continue
        module = import_module(entry["module"])
        cls = getattr(module, entry["class"])
        plugin = cls(**entry.get("config", {}))
        registry.register(plugin)


def load_plugins() -> None:
    config = get_config_bundle().plugins
    _load_from_entries(config.get("taggers", []), tagger_registry)
    _load_from_entries(config.get("controllers", []), controller_registry)
    _load_from_entries(config.get("builders", []), builder_registry)
