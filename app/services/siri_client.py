from __future__ import annotations

import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable


logger = logging.getLogger(__name__)
DEFAULT_LIVE_FEED_URL = "https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/"


def masked_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    sensitive = {"api_key", "apikey", "key", "token", "secret", "password", "signature", "sig"}
    query = urllib.parse.urlencode([(key, "***" if key.lower() in sensitive else value) for key, value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)])
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))


@dataclass(frozen=True)
class SIRIClientConfig:
    feed_url: str = DEFAULT_LIVE_FEED_URL
    api_key: str = ""
    timeout_seconds: int = 20
    max_response_bytes: int = 8 * 1024 * 1024
    user_agent: str = "Bluestar-Unilink-App/1.0"
    attempts: int = 3

    @classmethod
    def from_env(cls) -> "SIRIClientConfig":
        return cls(
            feed_url=os.getenv("LIVE_FEED_URL", DEFAULT_LIVE_FEED_URL).strip(),
            api_key=os.getenv("LIVE_API_KEY", os.getenv("BODS_API_KEY", "")).strip(),
            timeout_seconds=max(1, int(os.getenv("LIVE_REQUEST_TIMEOUT_SECONDS", "20"))),
            max_response_bytes=max(1024, int(os.getenv("LIVE_MAX_RESPONSE_BYTES", str(8 * 1024 * 1024)))),
            user_agent=os.getenv("LIVE_USER_AGENT", "Bluestar-Unilink-App/1.0").strip(),
            attempts=min(3, max(1, int(os.getenv("LIVE_REFRESH_ATTEMPTS", "3")))),
        )


class SIRIClient:
    def __init__(self, config: SIRIClientConfig, *, opener: Callable[..., Any] = urllib.request.urlopen, sleep: Callable[[float], None] = time.sleep):
        self.config, self.opener, self.sleep = config, opener, sleep

    @property
    def source(self) -> str:
        return masked_url(self.config.feed_url)

    def _request(self) -> urllib.request.Request:
        url = self.config.feed_url
        headers = {"User-Agent": self.config.user_agent}
        if self.config.api_key:
            keys = {key.lower() for key, _ in urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query, keep_blank_values=True)}
            if not ({"api_key", "key"} & keys):
                separator = "&" if "?" in url else "?"
                url = f"{url}{separator}{urllib.parse.urlencode({'api_key': self.config.api_key})}"
            headers["x-api-key"] = self.config.api_key
        return urllib.request.Request(url, headers=headers)

    def download(self) -> bytes:
        attempts = min(3, max(1, self.config.attempts))
        for attempt in range(1, attempts + 1):
            try:
                response = self.opener(self._request(), timeout=self.config.timeout_seconds)
                status = int(getattr(response, "status", getattr(response, "code", 200)))
                if not 200 <= status < 300:
                    raise urllib.error.HTTPError(self.source, status, "SIRI HTTP failure", {}, None)
                with response:
                    length = response.headers.get("Content-Length")
                    if length and int(length) > self.config.max_response_bytes:
                        raise ValueError("SIRI response exceeds configured size limit")
                    chunks, total = [], 0
                    while chunk := response.read(64 * 1024):
                        total += len(chunk)
                        if total > self.config.max_response_bytes:
                            raise ValueError("SIRI response exceeds configured size limit")
                        chunks.append(chunk)
                    return b"".join(chunks)
            except urllib.error.HTTPError as exc:
                if exc.code < 500 or attempt == attempts:
                    raise
            except (urllib.error.URLError, TimeoutError, OSError):
                if attempt == attempts:
                    raise
            self.sleep(min(attempt, 2))
        raise RuntimeError("SIRI download failed")
