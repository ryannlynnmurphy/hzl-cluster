import json
import os
import tempfile
import pytest
from hzl_cluster.fetchers.podcast_fetcher import fetch_podcasts


class TestPodcastFetcher:
    def setup_method(self):
        self.staging = tempfile.mkdtemp()

    def test_simulate_returns_data(self):
        result = fetch_podcasts(self.staging, simulate=True)
        assert result["success"] is True
        assert result["episodes_downloaded"] == 2
        assert result["feeds_fetched"] == 2
        assert result["index_file"] is not None

    def test_simulate_writes_index(self):
        result = fetch_podcasts(self.staging, simulate=True)
        with open(result["index_file"]) as f:
            data = json.load(f)
        assert "episodes" in data
        assert "fetched_at" in data
        assert len(data["episodes"]) == 2

    def test_simulate_episode_structure(self):
        result = fetch_podcasts(self.staging, simulate=True)
        with open(result["index_file"]) as f:
            data = json.load(f)
        ep = data["episodes"][0]
        assert "title" in ep
        assert "show" in ep
        assert "duration" in ep
        assert "file" in ep
        assert "description" in ep

    def test_staging_dir_created(self):
        new_dir = os.path.join(self.staging, "deep", "staging")
        result = fetch_podcasts(new_dir, simulate=True)
        podcasts_dir = os.path.join(new_dir, "podcasts")
        assert os.path.isdir(podcasts_dir)

    def test_custom_feeds(self):
        custom_feeds = {
            "test_show": "https://example.com/fake.rss",
        }
        # simulate=True ignores feed URLs, but the feeds dict is still accepted
        result = fetch_podcasts(self.staging, feeds=custom_feeds, simulate=True)
        assert result["success"] is True
        # simulate always returns its hardcoded episodes regardless of custom feeds
        assert result["index_file"] is not None
        with open(result["index_file"]) as f:
            data = json.load(f)
        assert isinstance(data["episodes"], list)
