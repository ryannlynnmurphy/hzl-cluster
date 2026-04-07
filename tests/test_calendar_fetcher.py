import json
import os
import tempfile
import pytest
from hzl_cluster.fetchers.calendar_fetcher import fetch_calendar


class TestCalendarFetcher:
    def setup_method(self):
        self.staging = tempfile.mkdtemp()

    def test_simulate_returns_data(self):
        result = fetch_calendar(self.staging, simulate=True)
        assert result["success"] is True
        assert result["file"] is not None
        assert os.path.exists(result["file"])
        assert result["count"] > 0

    def test_simulate_writes_events(self):
        result = fetch_calendar(self.staging, simulate=True)
        with open(result["file"]) as f:
            data = json.load(f)
        assert "events" in data
        assert isinstance(data["events"], list)
        assert len(data["events"]) > 0

    def test_simulate_event_structure(self):
        result = fetch_calendar(self.staging, simulate=True)
        with open(result["file"]) as f:
            data = json.load(f)
        event = data["events"][0]
        assert "title" in event
        assert "start" in event
        assert "end" in event
        assert "location" in event
        assert "description" in event
        assert "all_day" in event
        assert isinstance(event["all_day"], bool)
        assert event["title"]  # non-empty

    def test_staging_dir_created(self):
        new_dir = os.path.join(self.staging, "subdir", "gateway")
        result = fetch_calendar(new_dir, simulate=True)
        assert result["success"] is True
        # events.json lives inside a calendar/ sub-directory
        assert os.path.isdir(os.path.join(new_dir, "calendar"))
        assert os.path.exists(result["file"])
