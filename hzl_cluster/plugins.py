"""
Plugin system for the Hazel cluster.

Plugins are plain .py files placed in a plugins/ directory. Each file may
declare optional metadata at module level and expose functions whose names
follow one of three conventions:

    fetch_*   -- fetcher plugins (pull data from external sources)
    send_*    -- sender plugins (push data or notifications outward)
    on_*      -- event handler plugins (respond to cluster events)

Minimal plugin example
----------------------
    # my_plugin.py
    __plugin_name__    = "My Custom Fetcher"
    __plugin_version__ = "1.0"
    __plugin_type__    = "fetcher"
    __plugin_description__ = "Fetches data from a custom source."

    def fetch_custom_data(staging_dir, simulate=False):
        ...
"""

import importlib.util
import logging
import os
import types

logger = logging.getLogger("hzl.plugins")


class PluginManager:
    """Discover, load, and introspect cluster plugins from a directory."""

    def __init__(self, plugin_dir: str) -> None:
        self.plugin_dir = plugin_dir
        # Cache: name -> loaded module
        self._loaded: dict[str, types.ModuleType] = {}

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def discover(self) -> list[dict]:
        """Scan plugin_dir for .py files and return metadata for each.

        Returns a list of dicts with keys:
            name, version, type, description, file
        Files that cannot be parsed are silently skipped.
        """
        results: list[dict] = []

        if not os.path.isdir(self.plugin_dir):
            return results

        for filename in sorted(os.listdir(self.plugin_dir)):
            if not filename.endswith(".py") or filename.startswith("_"):
                continue

            filepath = os.path.join(self.plugin_dir, filename)
            stem = filename[:-3]  # strip .py

            try:
                spec = importlib.util.spec_from_file_location(stem, filepath)
                if spec is None or spec.loader is None:
                    continue
                module = types.ModuleType(stem)
                spec.loader.exec_module(module)  # type: ignore[union-attr]
            except Exception as exc:
                logger.warning("Skipping plugin %s: %s", filename, exc)
                continue

            results.append({
                "name":        getattr(module, "__plugin_name__",        stem),
                "version":     getattr(module, "__plugin_version__",     "0.0"),
                "type":        getattr(module, "__plugin_type__",        "unknown"),
                "description": getattr(module, "__plugin_description__", ""),
                "file":        filepath,
            })

        return results

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def load(self, name: str) -> types.ModuleType:
        """Import and return the plugin module whose file stem matches name.

        Raises FileNotFoundError if the plugin does not exist.
        Raises ImportError (or SyntaxError) if the module fails to load.
        """
        if name in self._loaded:
            return self._loaded[name]

        filepath = os.path.join(self.plugin_dir, f"{name}.py")
        if not os.path.isfile(filepath):
            raise FileNotFoundError(f"Plugin not found: {filepath}")

        spec = importlib.util.spec_from_file_location(name, filepath)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot create module spec for plugin: {name}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[union-attr]

        self._loaded[name] = module
        return module

    def load_all(self) -> dict[str, types.ModuleType]:
        """Load every discoverable plugin and return a name -> module mapping.

        Plugins that fail to load are logged and skipped.
        """
        loaded: dict[str, types.ModuleType] = {}

        if not os.path.isdir(self.plugin_dir):
            return loaded

        for filename in sorted(os.listdir(self.plugin_dir)):
            if not filename.endswith(".py") or filename.startswith("_"):
                continue
            stem = filename[:-3]
            try:
                loaded[stem] = self.load(stem)
            except Exception as exc:
                logger.warning("Failed to load plugin %s: %s", stem, exc)

        return loaded

    # ------------------------------------------------------------------
    # Function introspection
    # ------------------------------------------------------------------

    def get_fetchers(self) -> dict[str, callable]:
        """Return all fetch_* functions found across loaded plugins."""
        return self._collect_by_prefix("fetch_")

    def get_senders(self) -> dict[str, callable]:
        """Return all send_* functions found across loaded plugins."""
        return self._collect_by_prefix("send_")

    def get_event_handlers(self) -> dict[str, callable]:
        """Return all on_* functions found across loaded plugins."""
        return self._collect_by_prefix("on_")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _collect_by_prefix(self, prefix: str) -> dict[str, callable]:
        """Scan all loaded modules for callables whose name starts with prefix."""
        found: dict[str, callable] = {}
        for module in self._loaded.values():
            for attr_name in dir(module):
                if attr_name.startswith(prefix):
                    obj = getattr(module, attr_name)
                    if callable(obj):
                        found[attr_name] = obj
        return found
