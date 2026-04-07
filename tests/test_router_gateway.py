from hzl_cluster.router import classify_task


class TestGatewayClassifier:
    def test_check_email(self):
        assert classify_task("check my email") == "gateway_fetch"

    def test_fetch_email(self):
        assert classify_task("fetch my email") == "gateway_fetch"

    def test_download_podcast(self):
        assert classify_task("download the latest podcast") == "gateway_fetch"

    def test_get_the_news(self):
        assert classify_task("get me the news") == "gateway_fetch"

    def test_update_weather(self):
        assert classify_task("update my weather") == "gateway_fetch"

    def test_sync_with_internet(self):
        assert classify_task("sync with the internet") == "gateway_fetch"

    def test_go_online(self):
        assert classify_task("go online") == "gateway_sync"

    def test_sync_now(self):
        assert classify_task("sync now") == "gateway_sync"

    def test_connect_to_internet(self):
        assert classify_task("connect to the internet") == "gateway_sync"

    def test_run_a_sync(self):
        assert classify_task("run a sync") == "gateway_sync"

    def test_existing_voice_response_unchanged(self):
        assert classify_task("hey what time is it") == "voice_response"

    def test_existing_home_control_unchanged(self):
        assert classify_task("turn off the kitchen lights") == "home_control"

    def test_existing_search_unchanged(self):
        assert classify_task("search for the latest AI news") == "search"
