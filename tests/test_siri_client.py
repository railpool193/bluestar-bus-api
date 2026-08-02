import io
import unittest
import urllib.error

from app.services.siri_client import SIRIClient, SIRIClientConfig


class Response(io.BytesIO):
    def __init__(self, payload=b"<Siri/>", status=200, headers=None):
        super().__init__(payload); self.status = status; self.headers = headers or {}


class SIRIClientTests(unittest.TestCase):
    def test_success_adds_key_header_and_query_without_exposing_source(self):
        requests = []
        def opener(request, **_kwargs): requests.append(request); return Response()
        client = SIRIClient(SIRIClientConfig(feed_url="https://example.test/live", api_key="secret", attempts=1), opener=opener)
        self.assertEqual(client.download(), b"<Siri/>")
        self.assertEqual(requests[0].get_header("X-api-key"), "secret")
        self.assertEqual(requests[0].full_url.count("api_key="), 1)
        self.assertNotIn("secret", client.source)

    def test_existing_key_is_not_duplicated(self):
        requests = []
        client = SIRIClient(SIRIClientConfig(feed_url="https://example.test/live?api_key=existing", api_key="secret", attempts=1), opener=lambda request, **_: requests.append(request) or Response())
        client.download()
        self.assertEqual(requests[0].full_url.count("api_key="), 1)

    def test_4xx_not_retried_and_5xx_is_bounded(self):
        for code, expected in ((401, 1), (503, 3)):
            calls = []
            def opener(*_args, **_kwargs): calls.append(1); raise urllib.error.HTTPError("masked", code, "error", {}, None)
            with self.assertRaises(urllib.error.HTTPError):
                SIRIClient(SIRIClientConfig(attempts=3), opener=opener, sleep=lambda _: None).download()
            self.assertEqual(len(calls), expected)

    def test_timeout_and_size_limit(self):
        with self.assertRaises(TimeoutError):
            SIRIClient(SIRIClientConfig(attempts=1), opener=lambda *_a, **_k: (_ for _ in ()).throw(TimeoutError())).download()
        with self.assertRaises(ValueError):
            SIRIClient(SIRIClientConfig(max_response_bytes=3, attempts=1), opener=lambda *_a, **_k: Response(b"1234")).download()


if __name__ == "__main__": unittest.main()
