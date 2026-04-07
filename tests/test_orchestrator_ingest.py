import tempfile, os, pytest
from hzl_cluster.queue_hub import HazelMessage, QueueHub

class TestIngestEndpoint:
    def setup_method(self):
        self.tmp = tempfile.mktemp(suffix=".db")
        self.config = {"queue": {"db_path": self.tmp}}
        self.hub = QueueHub(self.config)

    def teardown_method(self):
        self.hub.close()
        if os.path.exists(self.tmp): os.unlink(self.tmp)

    def test_ingest_accepts_valid_messages(self):
        msg = HazelMessage.create(source="hazel-phone", destination="gateway",
            msg_type="fetch", action="fetch.email", payload={"account": "protonmail"})
        result = self.hub.ingest([msg])
        assert result["accepted"] == 1

    def test_ingest_multiple_messages(self):
        msgs = [HazelMessage.create(source="phone", destination="gateway",
            msg_type="fetch", action=f"fetch.{t}", payload={}) for t in ["email", "weather", "news"]]
        result = self.hub.ingest(msgs)
        assert result["accepted"] == 3

    def test_queue_status_after_ingest(self):
        msg = HazelMessage.create(source="phone", destination="gateway",
            msg_type="fetch", action="fetch.email", payload={})
        self.hub.ingest([msg])
        assert self.hub.status()["total_pending"] == 1

    def test_ack_removes_from_pending(self):
        msg = HazelMessage.create(source="gw", destination="core",
            msg_type="delivery", action="delivery.email", payload={})
        self.hub.ingest([msg])
        self.hub.ack(msg.id)
        assert self.hub.status()["total_pending"] == 0
