import json
import os
import tempfile
import pytest
from hzl_cluster.fetchers.contacts_fetcher import fetch_contacts


class TestContactsFetcher:
    def setup_method(self):
        self.staging = tempfile.mkdtemp()

    def test_simulate_returns_data(self):
        result = fetch_contacts(self.staging, simulate=True)
        assert result["success"] is True
        assert result["file"] is not None
        assert os.path.exists(result["file"])

    def test_simulate_writes_contacts(self):
        result = fetch_contacts(self.staging, simulate=True)
        with open(result["file"]) as f:
            data = json.load(f)
        assert "contacts" in data
        assert len(data["contacts"]) >= 3
        assert data["source"] == "simulate"

    def test_simulate_contact_structure(self):
        result = fetch_contacts(self.staging, simulate=True)
        with open(result["file"]) as f:
            data = json.load(f)
        contact = data["contacts"][0]
        assert "name" in contact
        assert "email" in contact
        assert "phone" in contact
        assert contact["name"]   # must be non-empty

    def test_staging_dir_created(self):
        new_dir = os.path.join(self.staging, "subdir", "contacts")
        result = fetch_contacts(new_dir, simulate=True)
        assert result["success"] is True
        assert os.path.isdir(new_dir)
