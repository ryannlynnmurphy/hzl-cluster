"""
Tests for hzl_cluster.secrets -- encrypted secrets manager.
"""

import os
import pytest

from hzl_cluster.secrets import SecretStore, derive_key


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store(tmp_path):
    """Fresh SecretStore backed by a temp file with a known key."""
    store_path = str(tmp_path / "test_secrets.enc")
    key, _ = derive_key("test-passphrase")
    return SecretStore(store_path, key)


@pytest.fixture
def store_path(tmp_path):
    """Return a reusable path string (no store created yet)."""
    return str(tmp_path / "persist_secrets.enc")


@pytest.fixture
def key():
    """Deterministic key for persistence / wrong-key tests."""
    k, _ = derive_key("shared-passphrase")
    return k


@pytest.fixture
def wrong_key():
    k, _ = derive_key("completely-different-passphrase")
    return k


# ---------------------------------------------------------------------------
# 1. test_set_and_get
# ---------------------------------------------------------------------------

def test_set_and_get(store):
    """Storing a secret and retrieving it returns the original value."""
    store.set("openai_api_key", "sk-abc123")
    assert store.get("openai_api_key") == "sk-abc123"


# ---------------------------------------------------------------------------
# 2. test_get_nonexistent
# ---------------------------------------------------------------------------

def test_get_nonexistent(store):
    """get() on a name that was never set returns None."""
    result = store.get("does_not_exist")
    assert result is None


# ---------------------------------------------------------------------------
# 3. test_delete
# ---------------------------------------------------------------------------

def test_delete(store):
    """delete() removes the secret and returns True; second delete returns False."""
    store.set("db_password", "hunter2")

    # First delete succeeds
    assert store.delete("db_password") is True
    assert store.get("db_password") is None

    # Second delete on the now-absent key returns False
    assert store.delete("db_password") is False


# ---------------------------------------------------------------------------
# 4. test_list_names
# ---------------------------------------------------------------------------

def test_list_names(store):
    """list_names() returns all stored names and none of the values."""
    store.set("alpha", "value-a")
    store.set("beta", "value-b")
    store.set("gamma", "value-c")

    names = store.list_names()

    # All names present
    assert set(names) == {"alpha", "beta", "gamma"}
    # Values must NOT appear in the name list
    for v in ("value-a", "value-b", "value-c"):
        assert v not in names


# ---------------------------------------------------------------------------
# 5. test_persistence
# ---------------------------------------------------------------------------

def test_persistence(store_path, key):
    """Secrets survive closing and reopening the store."""
    # Write
    s1 = SecretStore(store_path, key)
    s1.set("stripe_key", "sk_live_xyz")
    s1.set("sendgrid_key", "SG.abc")

    # Read back with a brand-new instance pointed at the same file
    s2 = SecretStore(store_path, key)
    assert s2.get("stripe_key") == "sk_live_xyz"
    assert s2.get("sendgrid_key") == "SG.abc"


# ---------------------------------------------------------------------------
# 6. test_wrong_key_fails
# ---------------------------------------------------------------------------

def test_wrong_key_fails(store_path, key, wrong_key):
    """A store opened with a different key cannot read the secrets."""
    # Write with the correct key
    s_write = SecretStore(store_path, key)
    s_write.set("secret_message", "the cake is a lie")

    # Open with a wrong key -- _load silently discards unreadable data
    s_wrong = SecretStore(store_path, wrong_key)
    assert s_wrong.get("secret_message") is None
    assert s_wrong.list_names() == []
