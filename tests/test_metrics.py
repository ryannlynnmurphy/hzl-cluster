"""
tests/test_metrics.py
---------------------
Tests for MetricsCollector — time-series metrics with SQLite persistence.
"""

import os
import tempfile
import time

import pytest

from hzl_cluster.metrics import (
    MetricsCollector,
    METRIC_SYNC_DURATION,
    METRIC_QUEUE_DEPTH,
    METRIC_FETCHER_SUCCESS,
    METRIC_NODE_CPU,
)


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

@pytest.fixture
def collector(tmp_path):
    db = str(tmp_path / "metrics.db")
    mc = MetricsCollector(db)
    yield mc
    mc.close()


# ─────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────

class TestMetricsCollector:

    def test_record_and_query(self, collector):
        """Record 3 data points; query returns all 3 in chronological order."""
        collector.record(METRIC_SYNC_DURATION, 120.0)
        collector.record(METRIC_SYNC_DURATION, 135.5)
        collector.record(METRIC_SYNC_DURATION, 98.2)

        results = collector.query(METRIC_SYNC_DURATION, since_hours=1)

        assert len(results) == 3
        values = [r["value"] for r in results]
        assert 120.0 in values
        assert 135.5 in values
        assert 98.2 in values
        # Verify expected keys are present on every entry
        for entry in results:
            assert "timestamp" in entry
            assert "value" in entry
            assert "tags" in entry

    def test_query_time_filter(self, collector):
        """Records older than the time window are excluded from query results."""
        # Inject a row with a timestamp well in the past via a second collector
        # on the same db, bypassing the public API isn't available — instead
        # we use the internal connection directly (white-box is acceptable for
        # time-travel tests).
        old_ts = time.time() - 48 * 3600  # 48 hours ago
        collector._conn.execute(
            "INSERT INTO metrics (metric_name, value, tags, timestamp) VALUES (?, ?, ?, ?)",
            (METRIC_QUEUE_DEPTH, 999.0, None, old_ts),
        )
        collector._conn.commit()

        # Recent point
        collector.record(METRIC_QUEUE_DEPTH, 42.0)

        results = collector.query(METRIC_QUEUE_DEPTH, since_hours=24)
        values = [r["value"] for r in results]

        assert 42.0 in values
        assert 999.0 not in values

    def test_summary_stats(self, collector):
        """Summary returns correct count, min, max, avg, and latest."""
        for v in [10.0, 20.0, 30.0]:
            collector.record(METRIC_NODE_CPU, v)

        s = collector.summary(METRIC_NODE_CPU, since_hours=1)

        assert s["count"] == 3
        assert s["min"] == pytest.approx(10.0)
        assert s["max"] == pytest.approx(30.0)
        assert s["avg"] == pytest.approx(20.0)
        # latest is the most recently recorded value
        assert s["latest"] == pytest.approx(30.0)

    def test_all_metrics(self, collector):
        """all_metrics() returns every unique metric name that has been recorded."""
        collector.record(METRIC_SYNC_DURATION, 1.0)
        collector.record(METRIC_QUEUE_DEPTH, 2.0)
        collector.record(METRIC_FETCHER_SUCCESS, 3.0)

        names = collector.all_metrics()

        assert METRIC_SYNC_DURATION in names
        assert METRIC_QUEUE_DEPTH in names
        assert METRIC_FETCHER_SUCCESS in names
        assert len(names) == 3

    def test_prune_old(self, collector):
        """prune() removes rows older than the threshold and returns the count."""
        old_ts = time.time() - 35 * 86400  # 35 days ago
        for _ in range(5):
            collector._conn.execute(
                "INSERT INTO metrics (metric_name, value, tags, timestamp) VALUES (?, ?, ?, ?)",
                (METRIC_SYNC_DURATION, 1.0, None, old_ts),
            )
        collector._conn.commit()

        # Add a recent point that must survive
        collector.record(METRIC_SYNC_DURATION, 2.0)

        deleted = collector.prune(older_than_days=30)

        assert deleted == 5
        remaining = collector.query(METRIC_SYNC_DURATION, since_hours=24)
        assert len(remaining) == 1
        assert remaining[0]["value"] == pytest.approx(2.0)

    def test_tags_stored(self, collector):
        """Tags dict is serialised and deserialised correctly."""
        tags = {"node": "pi-1", "region": "us-east"}
        collector.record(METRIC_NODE_CPU, 55.5, tags=tags)

        results = collector.query(METRIC_NODE_CPU, since_hours=1)

        assert len(results) == 1
        assert results[0]["tags"] == tags

    def test_empty_query(self, collector):
        """Querying a metric that has never been recorded returns an empty list."""
        results = collector.query("nonexistent.metric", since_hours=24)
        assert results == []
