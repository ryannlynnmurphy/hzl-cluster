import json
import logging
import os
import tempfile
from hzl_cluster.logging_config import HazelFormatter, setup_logging, get_logger


class TestLogging:
    def test_json_format(self):
        formatter = HazelFormatter()
        record = logging.LogRecord(
            name="hzl.test", level=logging.INFO, pathname="test.py",
            lineno=1, msg="test message", args=(), exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["msg"] == "test message"
        assert parsed["level"] == "INFO"
        assert parsed["logger"] == "hzl.test"
        assert "ts" in parsed
        assert "host" in parsed

    def test_extra_fields(self):
        formatter = HazelFormatter()
        record = logging.LogRecord(
            name="hzl.gateway", level=logging.INFO, pathname="gw.py",
            lineno=1, msg="sync", args=(), exc_info=None,
        )
        record.request_id = "abc123"
        record.relay_state = "core_connected"
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["request_id"] == "abc123"
        assert parsed["relay_state"] == "core_connected"

    def test_setup_logging_creates_file(self):
        tmp = tempfile.mkdtemp()
        config = {"paths": {"log_dir": tmp}, "logging": {"level": "DEBUG", "format": "json"}}
        setup_logging(config)
        logger = get_logger("test")
        logger.info("hello from test")
        log_file = os.path.join(tmp, "hazel.log")
        assert os.path.exists(log_file)
        with open(log_file) as f:
            line = f.readline()
        parsed = json.loads(line)
        assert parsed["msg"] == "hello from test"

    def test_get_logger_prefix(self):
        logger = get_logger("scanner")
        assert logger.name == "hzl.scanner"

    def test_text_format(self):
        tmp = tempfile.mkdtemp()
        config = {"paths": {"log_dir": tmp}, "logging": {"level": "INFO", "format": "text"}}
        setup_logging(config)
        logger = get_logger("test")
        logger.info("text mode")
        # Should not crash, text format works
