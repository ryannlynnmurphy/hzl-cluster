import json
import os
import tempfile
import pytest
from hzl_cluster.fetchers.weather_fetcher import fetch_weather


class TestWeatherFetcher:
    def setup_method(self):
        self.staging = tempfile.mkdtemp()

    def test_simulate_returns_data(self):
        result = fetch_weather(self.staging, simulate=True)
        assert result["success"] is True
        assert result["file"] is not None
        assert os.path.exists(result["file"])

    def test_simulate_writes_valid_json(self):
        result = fetch_weather(self.staging, simulate=True)
        with open(result["file"]) as f:
            data = json.load(f)
        assert "current" in data
        assert "daily" in data
        assert data["current"]["temperature"] == 72.0

    def test_simulate_summary(self):
        result = fetch_weather(self.staging, simulate=True)
        assert "72" in result["summary"]
        assert "Clear" in result["summary"]

    def test_custom_location(self):
        result = fetch_weather(self.staging, latitude=34.05, longitude=-118.24, simulate=True)
        assert result["success"] is True
        with open(result["file"]) as f:
            data = json.load(f)
        assert data["latitude"] == 34.05

    def test_staging_dir_created(self):
        new_dir = os.path.join(self.staging, "subdir", "weather")
        result = fetch_weather(new_dir, simulate=True)
        assert result["success"] is True
        assert os.path.isdir(new_dir)
