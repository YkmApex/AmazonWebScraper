from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


DEFAULT_USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.4 Safari/605.1.15"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
]


DEFAULT_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
]


@dataclass
class ProxySettings:
    server: str
    username: Optional[str] = None
    password: Optional[str] = None

    def as_playwright_proxy(self) -> dict[str, str]:
        payload = {"server": self.server}
        if self.username:
            payload["username"] = self.username
        if self.password:
            payload["password"] = self.password
        return payload


@dataclass
class CrawlerSettings:
    start_url: str
    workspace_dir: Path = field(default_factory=lambda: Path.cwd())
    output_dir: Path = field(init=False)
    temp_dir: Path = field(init=False)
    image_dir: Path = field(init=False)
    log_dir: Path = field(init=False)
    snapshot_path: Path = field(init=False)
    retry_list_path: Path = field(init=False)

    headless: bool = False
    browser_channel: Optional[str] = None
    slow_mo_ms: int = 50
    navigation_timeout_ms: int = 60_000
    page_timeout_ms: int = 60_000

    locale: str = "en-US"
    timezone_id: str = "America/Los_Angeles"
    geolocation: Optional[dict[str, float]] = None

    min_list_scroll_pause: float = 0.8
    max_list_scroll_pause: float = 2.2
    min_detail_pause: float = 1.0
    max_detail_pause: float = 5.0

    max_detail_concurrency: int = 3
    max_navigation_retries: int = 3
    max_parse_retries: int = 2
    negative_review_limit: int = 5
    max_negative_review_pages: int = 2

    user_agents: list[str] = field(default_factory=lambda: list(DEFAULT_USER_AGENTS))
    viewports: list[dict[str, int]] = field(default_factory=lambda: list(DEFAULT_VIEWPORTS))
    proxy: Optional[ProxySettings] = None

    accept_language: str = "en-US,en;q=0.9"
    sec_ch_ua_platform: str = '"Windows"'

    screenshot_on_error: bool = True
    verbose_console_log: bool = False

    def __post_init__(self) -> None:
        self.output_dir = self.workspace_dir / "output"
        self.temp_dir = self.workspace_dir / "temp"
        self.image_dir = self.output_dir / "images"
        self.log_dir = self.workspace_dir / "logs"
        self.snapshot_path = self.output_dir / "records_snapshot.json"
        self.retry_list_path = self.output_dir / "retry_list.json"

    @property
    def base_headers(self) -> dict[str, str]:
        return {
            "accept-language": self.accept_language,
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "upgrade-insecure-requests": "1",
        }

    @property
    def required_retry_fields(self) -> tuple[str, ...]:
        return (
            "title",
            "price",
            "brand",
            "dimensions_weight",
            "sub_category_rank",
        )
