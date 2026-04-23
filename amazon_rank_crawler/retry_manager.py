from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

from .config import CrawlerSettings
from .models import AmazonProductRecord
from .utils import build_logger, ensure_directories


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RetryRunResult:
    records: list[AmazonProductRecord]
    retry_targets: list[AmazonProductRecord]
    snapshot_path: Path
    retry_list_path: Path
    report_path: Optional[Path] = None


class AmazonRetryManager:
    def __init__(self, settings: CrawlerSettings) -> None:
        self.settings = settings
        self.logger = build_logger()
        self.amazon_base = self._build_amazon_base_url(settings.start_url)

    def save_records_snapshot(
        self,
        records: list[AmazonProductRecord],
        output_path: Optional[Path] = None,
    ) -> Path:
        output_path = output_path or self.settings.snapshot_path
        ensure_directories([output_path.parent])

        payload = {
            "generated_at": utc_now_iso(),
            "count": len(records),
            "records": [record.to_state_dict() for record in records],
        }
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        self.logger.info("Saved crawl snapshot to %s", output_path)
        return output_path

    def load_records_snapshot(self, snapshot_path: Optional[Path] = None) -> list[AmazonProductRecord]:
        snapshot_path = snapshot_path or self.settings.snapshot_path
        if not snapshot_path.exists():
            return []

        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        records_payload = payload.get("records", []) or []
        return [AmazonProductRecord.from_state_dict(item) for item in records_payload]

    def generate_retry_list(
        self,
        records: list[AmazonProductRecord],
        output_path: Optional[Path] = None,
    ) -> Path:
        output_path = output_path or self.settings.retry_list_path
        ensure_directories([output_path.parent])

        retry_records = [record for record in records if record.needs_retry(self.settings.required_retry_fields)]
        payload = {
            "generated_at": utc_now_iso(),
            "required_fields": list(self.settings.required_retry_fields),
            "count": len(retry_records),
            "items": [
                {
                    "asin": record.asin,
                    "url": record.url,
                    "rank": record.rank,
                    "title": record.title,
                    "missing_fields": record.missing_required_fields(self.settings.required_retry_fields),
                    "errors": [f"[{item.stage}] {item.message}" for item in record.errors],
                    "snapshot": record.to_state_dict(),
                }
                for record in retry_records
            ],
        }
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        self.logger.info("Saved retry list to %s", output_path)
        return output_path

    def load_retry_records(self, retry_list_path: Optional[Path] = None) -> list[AmazonProductRecord]:
        retry_list_path = retry_list_path or self.settings.retry_list_path
        if not retry_list_path.exists():
            return []

        payload = json.loads(retry_list_path.read_text(encoding="utf-8"))
        items = payload.get("items", []) or []
        records: list[AmazonProductRecord] = []
        for item in items:
            snapshot = item.get("snapshot")
            if isinstance(snapshot, dict):
                records.append(AmazonProductRecord.from_state_dict(snapshot))
                continue

            asin = str(item.get("asin", "") or "").strip()
            url = str(item.get("url", "") or "").strip()
            records.append(
                AmazonProductRecord(
                    asin=asin,
                    url=url or self._build_detail_url_from_asin(asin),
                    rank=str(item.get("rank", "") or "").strip(),
                    title=str(item.get("title", "") or "").strip(),
                )
            )
        return records

    def select_retry_targets(
        self,
        records: list[AmazonProductRecord],
        retry_asins: Optional[list[str]] = None,
        retry_urls: Optional[list[str]] = None,
        retry_list_path: Optional[Path] = None,
    ) -> list[AmazonProductRecord]:
        retry_asins = [item.strip().upper() for item in (retry_asins or []) if item and item.strip()]
        retry_urls = [item.strip() for item in (retry_urls or []) if item and item.strip()]

        by_asin = {record.asin.upper(): record for record in records if record.asin}
        by_url = {record.url: record for record in records if record.url}

        selected: list[AmazonProductRecord] = []
        seen_keys: set[str] = set()

        def append_target(record: AmazonProductRecord) -> None:
            key = record.asin.upper() if record.asin else record.url
            if not key or key in seen_keys:
                return
            seen_keys.add(key)
            selected.append(record)

        if retry_list_path is not None:
            for record in self.load_retry_records(retry_list_path):
                existing = by_asin.get(record.asin.upper(), None) if record.asin else None
                if existing is None and record.url:
                    existing = by_url.get(record.url)
                append_target(existing or record)

        for asin in retry_asins:
            existing = by_asin.get(asin)
            append_target(existing or AmazonProductRecord(asin=asin, url=self._build_detail_url_from_asin(asin)))

        for url in retry_urls:
            existing = by_url.get(url)
            append_target(existing or AmazonProductRecord(url=url, asin=self._extract_asin_from_url(url)))

        if not selected and not retry_asins and not retry_urls and retry_list_path is None:
            for record in records:
                if record.needs_retry(self.settings.required_retry_fields):
                    append_target(record)

        return selected

    def merge_retry_results(
        self,
        records: list[AmazonProductRecord],
        retried_records: list[AmazonProductRecord],
    ) -> list[AmazonProductRecord]:
        merged_records = list(records)
        by_asin = {record.asin.upper(): index for index, record in enumerate(merged_records) if record.asin}
        by_url = {record.url: index for index, record in enumerate(merged_records) if record.url}

        for retried in retried_records:
            target_index: Optional[int] = None
            if retried.asin:
                target_index = by_asin.get(retried.asin.upper())
            if target_index is None and retried.url:
                target_index = by_url.get(retried.url)

            if target_index is None:
                merged_records.append(retried)
                if retried.asin:
                    by_asin[retried.asin.upper()] = len(merged_records) - 1
                if retried.url:
                    by_url[retried.url] = len(merged_records) - 1
                continue

            merged_records[target_index] = retried

        return merged_records

    def build_retry_output(
        self,
        records: list[AmazonProductRecord],
        retry_targets: list[AmazonProductRecord],
        report_path: Optional[Path] = None,
    ) -> RetryRunResult:
        snapshot_path = self.save_records_snapshot(records)
        retry_list_path = self.generate_retry_list(records)
        return RetryRunResult(
            records=records,
            retry_targets=retry_targets,
            snapshot_path=snapshot_path,
            retry_list_path=retry_list_path,
            report_path=report_path,
        )

    def _build_detail_url_from_asin(self, asin: str) -> str:
        asin = asin.strip().upper()
        return urljoin(self.amazon_base, f"/dp/{asin}") if asin else ""

    def _extract_asin_from_url(self, url: str) -> str:
        parsed = urlparse(url)
        segments = [segment for segment in parsed.path.split("/") if segment]
        for index, segment in enumerate(segments):
            if segment in {"dp", "product"} and index + 1 < len(segments):
                candidate = segments[index + 1].strip().upper()
                if len(candidate) == 10 and candidate.isalnum():
                    return candidate
        return ""

    def _build_amazon_base_url(self, url: str) -> str:
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return "https://www.amazon.com"
