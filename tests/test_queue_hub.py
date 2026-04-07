"""
tests/test_queue_hub.py
-----------------------
Tests for HazelMessage, QueueDB, and QueueHub.
"""

import os
import tempfile
import time
import pytest

from hzl_cluster.queue_hub import HazelMessage, QueueDB, QueueHub


# ─────────────────────────────────────────────────────────────
# TestHazelMessage
# ─────────────────────────────────────────────────────────────

class TestHazelMessage:

    def test_create_message(self):
        """Verify all default fields are set correctly by create()."""
        msg = HazelMessage.create(
            source="core",
            destination="worker-1",
            msg_type="task",
            action="run_inference",
        )
        assert msg.source == "core"
        assert msg.destination == "worker-1"
        assert msg.msg_type == "task"
        assert msg.action == "run_inference"
        assert msg.priority == "normal"
        assert msg.status == "queued"
        assert msg.ttl == 86400
        assert msg.payload == {}
        assert msg.delivered_at is None
        assert isinstance(msg.id, str) and len(msg.id) == 36  # UUID4
        assert isinstance(msg.created_at, float)

    def test_to_dict_roundtrip(self):
        """Serialise to dict and back — all fields survive the round trip."""
        original = HazelMessage.create(
            source="core",
            destination="worker-2",
            msg_type="control",
            action="ping",
            payload={"seq": 1},
            priority="critical",
            ttl=300,
        )
        restored = HazelMessage.from_dict(original.to_dict())

        assert restored.id == original.id
        assert restored.source == original.source
        assert restored.destination == original.destination
        assert restored.msg_type == original.msg_type
        assert restored.action == original.action
        assert restored.payload == original.payload
        assert restored.priority == original.priority
        assert restored.status == original.status
        assert restored.created_at == original.created_at
        assert restored.delivered_at == original.delivered_at
        assert restored.ttl == original.ttl

    def test_is_expired_no_ttl(self):
        """A message with ttl=0 never expires regardless of age."""
        msg = HazelMessage.create(
            source="core",
            destination="worker-1",
            msg_type="task",
            action="no_expire",
            ttl=0,
        )
        # Artificially age the message
        msg.created_at = time.time() - 9999999
        assert msg.is_expired() is False

    def test_is_expired_with_ttl(self):
        """A message whose elapsed time exceeds its ttl is expired."""
        msg = HazelMessage.create(
            source="core",
            destination="worker-1",
            msg_type="task",
            action="expire_me",
            ttl=10,
        )
        # Push created_at back so it's well past expiry
        msg.created_at = time.time() - 60
        assert msg.is_expired() is True


# ─────────────────────────────────────────────────────────────
# TestQueueDB
# ─────────────────────────────────────────────────────────────

class TestQueueDB:

    def setup_method(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._tmp.close()
        self.db = QueueDB(self._tmp.name)

    def teardown_method(self):
        self.db.close()
        os.unlink(self._tmp.name)

    def _make_msg(self, destination="worker-1", priority="normal", ttl=86400):
        return HazelMessage.create(
            source="core",
            destination=destination,
            msg_type="task",
            action="do_work",
            priority=priority,
            ttl=ttl,
        )

    def test_store_and_retrieve(self):
        """Store a message then retrieve it by id."""
        msg = self._make_msg()
        self.db.store(msg)
        fetched = self.db.get(msg.id)
        assert fetched is not None
        assert fetched.id == msg.id
        assert fetched.destination == "worker-1"
        assert fetched.status == "queued"

    def test_get_pending_by_destination(self):
        """get_pending returns only messages for the requested destination."""
        msg_a = self._make_msg(destination="worker-1")
        msg_b = self._make_msg(destination="worker-2")
        self.db.store(msg_a)
        self.db.store(msg_b)

        pending_1 = self.db.get_pending("worker-1")
        pending_2 = self.db.get_pending("worker-2")

        assert len(pending_1) == 1
        assert pending_1[0].id == msg_a.id
        assert len(pending_2) == 1
        assert pending_2[0].id == msg_b.id

    def test_update_status(self):
        """update_status changes status and sets delivered_at when delivered."""
        msg = self._make_msg()
        self.db.store(msg)

        self.db.update_status(msg.id, "delivered")
        updated = self.db.get(msg.id)

        assert updated.status == "delivered"
        assert updated.delivered_at is not None
        assert updated.delivered_at <= time.time()

    def test_expire_old_messages(self):
        """expire() deletes old queued/failed messages but preserves fresh ones."""
        old_msg = self._make_msg(ttl=10)
        old_msg.created_at = time.time() - 60  # expired
        self.db.store(old_msg)

        fresh_msg = self._make_msg(ttl=86400)
        self.db.store(fresh_msg)

        deleted = self.db.expire()
        assert deleted == 1
        assert self.db.get(old_msg.id) is None
        assert self.db.get(fresh_msg.id) is not None

    def test_get_pending_respects_priority_order(self):
        """Critical messages come before low-priority ones regardless of insertion order."""
        low_msg = self._make_msg(priority="low")
        low_msg.created_at = time.time() - 10  # older
        self.db.store(low_msg)

        critical_msg = self._make_msg(priority="critical")
        critical_msg.created_at = time.time()   # newer
        self.db.store(critical_msg)

        pending = self.db.get_pending("worker-1")
        assert len(pending) == 2
        assert pending[0].id == critical_msg.id
        assert pending[1].id == low_msg.id


# ─────────────────────────────────────────────────────────────
# TestQueueHub
# ─────────────────────────────────────────────────────────────

class TestQueueHub:

    def setup_method(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._tmp.close()
        self.hub = QueueHub({"queue": {"db_path": self._tmp.name}})

    def teardown_method(self):
        self.hub.close()
        os.unlink(self._tmp.name)

    def _make_msg(self, destination="worker-1", priority="normal", ttl=86400):
        return HazelMessage.create(
            source="core",
            destination=destination,
            msg_type="task",
            action="do_work",
            priority=priority,
            ttl=ttl,
        )

    def test_ingest_stores_message(self):
        """ingest() returns accepted=1 for a valid message."""
        msg = self._make_msg()
        result = self.hub.ingest([msg])
        assert result["accepted"] == 1
        assert result["rejected"] == 0
        assert result["errors"] == []

    def test_ingest_rejects_expired(self):
        """ingest() rejects a message that has already expired."""
        msg = self._make_msg(ttl=10)
        msg.created_at = time.time() - 60
        result = self.hub.ingest([msg])
        assert result["accepted"] == 0
        assert result["rejected"] == 1
        assert len(result["errors"]) == 1

    def test_get_outbound_returns_pending_for_destination(self):
        """get_outbound returns queued messages for the given destination."""
        msg_a = self._make_msg(destination="worker-1")
        msg_b = self._make_msg(destination="worker-2")
        self.hub.ingest([msg_a, msg_b])

        outbound = self.hub.get_outbound("worker-1")
        assert len(outbound) == 1
        assert outbound[0].id == msg_a.id

    def test_ack_marks_delivered(self):
        """ack() sets the message status to delivered."""
        msg = self._make_msg()
        self.hub.ingest([msg])
        self.hub.ack(msg.id)

        # Outbound should now be empty (status is no longer queued)
        outbound = self.hub.get_outbound("worker-1")
        assert len(outbound) == 0

        # Verify via db directly
        stored = self.hub._db.get(msg.id)
        assert stored.status == "delivered"
        assert stored.delivered_at is not None

    def test_queue_status(self):
        """status() returns correct total_pending and per-destination counts."""
        self.hub.ingest([
            self._make_msg(destination="worker-1"),
            self._make_msg(destination="worker-1"),
            self._make_msg(destination="worker-2"),
        ])

        s = self.hub.status()
        assert s["total_pending"] == 3
        assert s["by_destination"]["worker-1"] == 2
        assert s["by_destination"]["worker-2"] == 1
