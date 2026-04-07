from hzl_cluster.dashboard import bar, render_header, render_node, render_queue, C


class TestDashboard:
    def test_bar_low(self):
        result = bar(25.0)
        assert "█" in result
        assert "25.0%" in result

    def test_bar_high(self):
        result = bar(90.0)
        assert "90.0%" in result

    def test_render_header(self):
        header = render_header()
        assert "HAZEL" in header or "H A Z E L" in header

    def test_render_node_online(self):
        node = {
            "hostname": "hazel-core", "role": "core", "alive": True,
            "healthy": True, "cpu": 45.0, "memory": 60.0,
            "capabilities": ["orchestrator", "voice"],
            "circuit_breaker": {"state": "closed"},
        }
        result = render_node(node)
        assert "hazel-core" in result
        assert "core" in result

    def test_render_node_offline(self):
        node = {
            "hostname": "hazel-worker-01", "role": "worker", "alive": False,
            "healthy": False, "cpu": 0, "memory": 0,
            "capabilities": ["inference"],
            "circuit_breaker": {"state": "closed"},
        }
        result = render_node(node)
        assert "OFFLINE" in result

    def test_render_queue_empty(self):
        result = render_queue({"total_pending": 0, "by_destination": {}})
        assert "0 pending" in result

    def test_render_queue_with_messages(self):
        result = render_queue({"total_pending": 5, "by_destination": {"gateway": 3, "hazel-phone": 2}})
        assert "5 pending" in result
        assert "gateway" in result
