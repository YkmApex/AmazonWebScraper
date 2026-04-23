from __future__ import annotations

import hashlib
import io
import mimetypes
import random
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import pandas as pd
import requests

from .config import CrawlerSettings
from .models import AmazonProductRecord
from .utils import build_logger

EXPORT_SHEET_NAME = "Amazon Rank Report"
IMAGE_COLUMN_NAME = "Image"
IMAGE_COLUMN_WIDTH = 18
DEFAULT_ROW_HEIGHT = 90

EXPORT_COLUMN_ORDER = [
    IMAGE_COLUMN_NAME,
    "Rank",
    "Title",
    "URL",
    "ASIN",
    "Main Image URL",
    "Price",
    "Rating",
    "Review Count",
    "Brand",
    "Monthly Sales",
    "Coupon/Discount",
    "Dimensions/Weight",
    "Feature 1",
    "Feature 2",
    "Feature 3",
    "Feature 4",
    "Feature 5",
    "Sub-category Rank",
    "A+ Content Flag",
    "Bad_Review_1",
    "Bad_Review_2",
    "Bad_Review_3",
    "Bad_Review_4",
    "Bad_Review_5",
    "Crawled At",
    "Updated At",
    "Source Category URL",
    "Errors",
]


@dataclass
class DownloadedImage:
    file_path: Path
    image_bytes: bytes


class AmazonExcelExporter:
    def __init__(self, settings: CrawlerSettings) -> None:
        self.settings = settings
        self.logger = build_logger()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": random.choice(self.settings.user_agents),
                "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                "Accept-Language": self.settings.accept_language,
                "Referer": self.settings.start_url,
                **self.settings.base_headers,
            }
        )

    def export(
        self,
        records: list[AmazonProductRecord],
        output_path: Optional[Path] = None,
    ) -> Path:
        output_path = output_path or self._default_output_path()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        dataframe = self.build_dataframe(records)
        export_frame = dataframe.copy()
        if IMAGE_COLUMN_NAME not in export_frame.columns:
            export_frame.insert(0, IMAGE_COLUMN_NAME, "")

        with pd.ExcelWriter(
            output_path,
            engine="xlsxwriter",
            engine_kwargs={"options": {"strings_to_urls": False}},
        ) as writer:
            export_frame.to_excel(writer, index=False, sheet_name=EXPORT_SHEET_NAME)
            workbook = writer.book
            worksheet = writer.sheets[EXPORT_SHEET_NAME]

            self._apply_sheet_layout(workbook=workbook, worksheet=worksheet, dataframe=export_frame)
            self._write_hyperlinks(workbook=workbook, worksheet=worksheet, dataframe=export_frame)
            self._insert_images(worksheet=worksheet, dataframe=export_frame)

        self.logger.info("Excel report exported to %s", output_path)
        return output_path

    def build_dataframe(self, records: list[AmazonProductRecord]) -> pd.DataFrame:
        rows = [record.as_flat_dict() for record in records]
        dataframe = pd.DataFrame(rows)

        if dataframe.empty:
            dataframe = pd.DataFrame(columns=[column for column in EXPORT_COLUMN_ORDER if column != IMAGE_COLUMN_NAME])

        for column in EXPORT_COLUMN_ORDER:
            if column == IMAGE_COLUMN_NAME:
                continue
            if column not in dataframe.columns:
                dataframe[column] = ""

        dataframe = dataframe[[column for column in EXPORT_COLUMN_ORDER if column != IMAGE_COLUMN_NAME]]
        return dataframe

    def _apply_sheet_layout(self, workbook, worksheet, dataframe: pd.DataFrame) -> None:
        header_format = workbook.add_format(
            {
                "bold": True,
                "font_color": "#FFFFFF",
                "bg_color": "#1F4E78",
                "align": "center",
                "valign": "vcenter",
                "border": 1,
                "text_wrap": True,
            }
        )
        text_format = workbook.add_format(
            {
                "valign": "top",
                "text_wrap": True,
                "border": 1,
            }
        )
        center_format = workbook.add_format(
            {
                "valign": "top",
                "align": "center",
                "border": 1,
                "text_wrap": True,
            }
        )
        url_format = workbook.add_format(
            {
                "font_color": "#0563C1",
                "underline": 1,
                "valign": "top",
                "border": 1,
                "text_wrap": True,
            }
        )

        worksheet.set_row(0, 30, header_format)
        worksheet.freeze_panes(1, 2)
        worksheet.autofilter(0, 0, max(len(dataframe), 1), len(dataframe.columns) - 1)

        worksheet.set_column(0, 0, IMAGE_COLUMN_WIDTH)
        worksheet.set_column(1, 1, 8, center_format)
        worksheet.set_column(2, 2, 42, text_format)
        worksheet.set_column(3, 3, 22, url_format)
        worksheet.set_column(4, 4, 16, center_format)
        worksheet.set_column(5, 5, 32, text_format)
        worksheet.set_column(6, 8, 14, center_format)
        worksheet.set_column(9, 11, 22, text_format)
        worksheet.set_column(12, 12, 26, text_format)
        worksheet.set_column(13, 17, 30, text_format)
        worksheet.set_column(18, 18, 26, text_format)
        worksheet.set_column(19, 19, 12, center_format)
        worksheet.set_column(20, 24, 36, text_format)
        worksheet.set_column(25, 28, 24, text_format)

        for row_index in range(1, len(dataframe) + 1):
            worksheet.set_row(row_index, DEFAULT_ROW_HEIGHT)

        for column_index, column_name in enumerate(dataframe.columns):
            worksheet.write(0, column_index, column_name, header_format)

        for column_name in ("A+ Content Flag",):
            if column_name in dataframe.columns:
                col_index = dataframe.columns.get_loc(column_name)
                for row_offset, value in enumerate(dataframe[column_name].tolist(), start=1):
                    worksheet.write_boolean(row_offset, col_index, bool(value), center_format)

    def _write_hyperlinks(self, workbook, worksheet, dataframe: pd.DataFrame) -> None:
        if "URL" not in dataframe.columns:
            return

        url_format = workbook.add_format(
            {
                "font_color": "#0563C1",
                "underline": 1,
                "valign": "top",
                "border": 1,
                "text_wrap": True,
            }
        )
        url_col = dataframe.columns.get_loc("URL")

        for row_offset, (_, row) in enumerate(dataframe.iterrows(), start=1):
            url = str(row.get("URL", "") or "").strip()
            asin = str(row.get("ASIN", "") or "").strip()
            if url.startswith("http"):
                display = asin or "Open Product"
                worksheet.write_url(row_offset, url_col, url, url_format, string=display)

    def _insert_images(self, worksheet, dataframe: pd.DataFrame) -> None:
        if "Main Image URL" not in dataframe.columns:
            return

        image_url_col = dataframe.columns.get_loc("Main Image URL")
        image_col = dataframe.columns.get_loc(IMAGE_COLUMN_NAME)

        for row_offset, (_, row) in enumerate(dataframe.iterrows(), start=1):
            image_url = str(row.iloc[image_url_col] or "").strip()
            asin = str(row.get("ASIN", "") or row.get("Rank", "") or f"row_{row_offset}").strip()
            if not image_url:
                continue

            try:
                downloaded = self._download_image(image_url=image_url, asin=asin)
                if downloaded is None:
                    continue

                worksheet.insert_image(
                    row_offset,
                    image_col,
                    str(downloaded.file_path),
                    {
                        "image_data": io.BytesIO(downloaded.image_bytes),
                        "x_offset": 6,
                        "y_offset": 5,
                        "x_scale": 0.58,
                        "y_scale": 0.58,
                        "object_position": 1,
                    },
                )
            except Exception as exc:
                self.logger.warning("Failed to embed image for ASIN=%s URL=%s: %s", asin, image_url, exc)

    def _download_image(self, image_url: str, asin: str) -> Optional[DownloadedImage]:
        cache_path = self._build_image_cache_path(image_url=image_url, asin=asin)
        if cache_path.exists():
            return DownloadedImage(file_path=cache_path, image_bytes=cache_path.read_bytes())

        try:
            response = self.session.get(image_url, timeout=(10, 30), stream=True)
            response.raise_for_status()
            image_bytes = response.content
            if not image_bytes:
                return None

            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_bytes(image_bytes)
            return DownloadedImage(file_path=cache_path, image_bytes=image_bytes)
        except Exception as exc:
            self.logger.warning("Image download failed for ASIN=%s URL=%s: %s", asin, image_url, exc)
            return None

    def _build_image_cache_path(self, image_url: str, asin: str) -> Path:
        url_hash = hashlib.md5(image_url.encode("utf-8")).hexdigest()[:12]
        ext = self._guess_extension(image_url)
        safe_asin = re.sub(r"[^A-Za-z0-9_-]+", "_", asin) or "image"
        return self.settings.image_dir / f"{safe_asin}_{url_hash}{ext}"

    def _guess_extension(self, image_url: str) -> str:
        parsed = urlparse(image_url)
        suffix = Path(parsed.path).suffix.lower()
        if suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}:
            return suffix

        mime_type, _ = mimetypes.guess_type(image_url)
        guessed = mimetypes.guess_extension(mime_type or "")
        return guessed or ".jpg"

    def _default_output_path(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.settings.output_dir / f"amazon_rank_report_{timestamp}.xlsx"
