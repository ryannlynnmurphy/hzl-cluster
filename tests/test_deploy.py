import pytest
from hzl_cluster.deploy import get_ip, C


class TestDeploy:
    def test_get_ip_returns_string(self):
        ip = get_ip()
        assert isinstance(ip, str)
        assert "." in ip  # basic IP format check

    def test_colors_defined(self):
        assert C.GOLD
        assert C.GREEN
        assert C.RED
        assert C.RESET
