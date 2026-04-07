"""Tests for the plugin system (hzl_cluster/plugins.py)."""

import os
import textwrap

import pytest

from hzl_cluster.plugins import PluginManager


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def empty_plugin_dir(tmp_path):
    """A temporary directory with no plugin files."""
    d = tmp_path / "plugins"
    d.mkdir()
    return str(d)


@pytest.fixture()
def plugin_dir(tmp_path):
    """A temporary directory pre-populated with a few plugin files."""
    d = tmp_path / "plugins"
    d.mkdir()

    # Fetcher plugin
    (d / "data_fetcher.py").write_text(textwrap.dedent("""\
        __plugin_name__        = "Data Fetcher"
        __plugin_version__     = "1.2"
        __plugin_type__        = "fetcher"
        __plugin_description__ = "Fetches custom data."

        def fetch_custom_data(staging_dir, simulate=False):
            return {"success": True, "source": "custom"}
    """))

    # Sender plugin
    (d / "slack_sender.py").write_text(textwrap.dedent("""\
        __plugin_name__        = "Slack Sender"
        __plugin_version__     = "0.9"
        __plugin_type__        = "sender"
        __plugin_description__ = "Sends messages to Slack."

        def send_slack_message(channel, text):
            return {"sent": True, "channel": channel}
    """))

    # Event handler plugin
    (d / "node_watcher.py").write_text(textwrap.dedent("""\
        __plugin_name__        = "Node Watcher"
        __plugin_version__     = "1.0"
        __plugin_type__        = "event_handler"
        __plugin_description__ = "Watches node events."

        def on_node_joined(hostname, role, ip):
            return f"joined:{hostname}"
    """))

    return str(d)


# ---------------------------------------------------------------------------
# 1. test_discover_empty_dir
# ---------------------------------------------------------------------------

def test_discover_empty_dir(empty_plugin_dir):
    """discover() on an empty directory returns an empty list."""
    pm = PluginManager(empty_plugin_dir)
    result = pm.discover()
    assert result == []


# ---------------------------------------------------------------------------
# 2. test_discover_plugins
# ---------------------------------------------------------------------------

def test_discover_plugins(plugin_dir):
    """discover() finds all plugin files and returns correct metadata."""
    pm = PluginManager(plugin_dir)
    result = pm.discover()

    assert len(result) == 3

    names = {entry["name"] for entry in result}
    assert "Data Fetcher" in names
    assert "Slack Sender" in names
    assert "Node Watcher" in names

    by_name = {e["name"]: e for e in result}

    fetcher = by_name["Data Fetcher"]
    assert fetcher["version"] == "1.2"
    assert fetcher["type"] == "fetcher"
    assert fetcher["description"] == "Fetches custom data."

    sender = by_name["Slack Sender"]
    assert sender["type"] == "sender"

    watcher = by_name["Node Watcher"]
    assert watcher["type"] == "event_handler"


# ---------------------------------------------------------------------------
# 3. test_load_plugin
# ---------------------------------------------------------------------------

def test_load_plugin(plugin_dir):
    """load() imports a plugin module and its functions are callable."""
    pm = PluginManager(plugin_dir)
    module = pm.load("data_fetcher")

    assert hasattr(module, "fetch_custom_data")
    result = module.fetch_custom_data("/tmp/staging", simulate=True)
    assert result["success"] is True


# ---------------------------------------------------------------------------
# 4. test_get_fetchers
# ---------------------------------------------------------------------------

def test_get_fetchers(plugin_dir):
    """get_fetchers() returns all fetch_* callables across loaded plugins."""
    pm = PluginManager(plugin_dir)
    pm.load_all()

    fetchers = pm.get_fetchers()

    assert "fetch_custom_data" in fetchers
    assert callable(fetchers["fetch_custom_data"])

    # Sender and handler functions must not appear here
    assert not any(k.startswith("send_") for k in fetchers)
    assert not any(k.startswith("on_") for k in fetchers)


# ---------------------------------------------------------------------------
# 5. test_get_senders
# ---------------------------------------------------------------------------

def test_get_senders(plugin_dir):
    """get_senders() returns all send_* callables across loaded plugins."""
    pm = PluginManager(plugin_dir)
    pm.load_all()

    senders = pm.get_senders()

    assert "send_slack_message" in senders
    assert callable(senders["send_slack_message"])

    result = senders["send_slack_message"]("#general", "hello")
    assert result["sent"] is True
    assert result["channel"] == "#general"

    # Fetcher and handler functions must not appear here
    assert not any(k.startswith("fetch_") for k in senders)
    assert not any(k.startswith("on_") for k in senders)


# ---------------------------------------------------------------------------
# 6. test_invalid_plugin_skipped
# ---------------------------------------------------------------------------

def test_invalid_plugin_skipped(tmp_path):
    """A plugin with a syntax error is skipped; valid plugins still load."""
    d = tmp_path / "plugins"
    d.mkdir()

    # Valid plugin
    (d / "good_plugin.py").write_text(textwrap.dedent("""\
        __plugin_name__ = "Good Plugin"
        __plugin_type__ = "fetcher"

        def fetch_good_data(staging_dir, simulate=False):
            return {"ok": True}
    """))

    # Broken plugin -- deliberate SyntaxError
    (d / "broken_plugin.py").write_text(textwrap.dedent("""\
        __plugin_name__ = "Broken Plugin"
        def fetch_broken(: <- this is invalid python
    """))

    pm = PluginManager(str(d))

    # discover() must not raise; broken file is silently skipped
    discovered = pm.discover()
    discovered_names = [e["name"] for e in discovered]
    assert "Good Plugin" in discovered_names
    assert "Broken Plugin" not in discovered_names

    # load_all() must not raise; only the valid plugin is returned
    loaded = pm.load_all()
    assert "good_plugin" in loaded
    assert "broken_plugin" not in loaded
