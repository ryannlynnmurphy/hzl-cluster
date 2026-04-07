import json
import os
import tempfile
import pytest
from hzl_cluster.fetchers.news_fetcher import fetch_news, _clean_html


class TestNewsFetcher:
    def setup_method(self):
        self.staging = tempfile.mkdtemp()

    def test_simulate_returns_data(self):
        result = fetch_news(self.staging, simulate=True)
        assert result["success"] is True
        assert result["articles_count"] == 3
        assert result["feeds_fetched"] == 3

    def test_simulate_writes_valid_json(self):
        result = fetch_news(self.staging, simulate=True)
        with open(result["file"]) as f:
            data = json.load(f)
        assert "articles" in data
        assert len(data["articles"]) == 3
        assert data["articles"][0]["feed"] == "hackernews"

    def test_simulate_article_structure(self):
        result = fetch_news(self.staging, simulate=True)
        with open(result["file"]) as f:
            data = json.load(f)
        article = data["articles"][0]
        assert "title" in article
        assert "link" in article
        assert "published" in article
        assert "summary" in article
        assert "feed" in article

    def test_staging_dir_created(self):
        new_dir = os.path.join(self.staging, "sub", "news")
        result = fetch_news(new_dir, simulate=True)
        assert os.path.isdir(new_dir)

    def test_clean_html(self):
        assert _clean_html("<p>Hello <b>world</b></p>") == "Hello world"
        assert _clean_html("plain text") == "plain text"
        assert _clean_html("") == ""
