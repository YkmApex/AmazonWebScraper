from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class CrawlError:
    stage: str
    message: str
    created_at: str = field(default_factory=utc_now_iso)


@dataclass
class AmazonProductRecord:
    rank: str = ""
    title: str = ""
    url: str = ""
    asin: str = ""
    main_image_url: str = ""
    price: str = ""
    rating: str = ""
    review_count: str = ""

    brand: str = ""
    monthly_sales: str = ""
    coupon_discount: str = ""
    dimensions_weight: str = ""
    feature_1: str = ""
    feature_2: str = ""
    feature_3: str = ""
    feature_4: str = ""
    feature_5: str = ""
    sub_category_rank: str = ""
    a_plus_content_flag: bool = False
    bad_review_1: str = ""
    bad_review_2: str = ""
    bad_review_3: str = ""
    bad_review_4: str = ""
    bad_review_5: str = ""

    crawled_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)
    source_category_url: str = ""
    errors: list[CrawlError] = field(default_factory=list)

    def add_error(self, stage: str, message: str) -> None:
        self.errors.append(CrawlError(stage=stage, message=message))
        self.updated_at = utc_now_iso()

    def merge(self, payload: dict[str, Any]) -> None:
        for key, value in payload.items():
            if hasattr(self, key):
                current_value = getattr(self, key)
                if isinstance(value, str):
                    if value.strip() or not str(current_value or "").strip():
                        setattr(self, key, value)
                    continue
                if value is not None:
                    setattr(self, key, value)
        self.updated_at = utc_now_iso()

    def needs_retry(self, required_fields: tuple[str, ...]) -> bool:
        for field_name in required_fields:
            value = getattr(self, field_name, None)
            if value is None:
                return True
            if isinstance(value, str) and not value.strip():
                return True
        return False

    def missing_required_fields(self, required_fields: tuple[str, ...]) -> list[str]:
        missing_fields: list[str] = []
        for field_name in required_fields:
            value = getattr(self, field_name, None)
            if value is None:
                missing_fields.append(field_name)
                continue
            if isinstance(value, str) and not value.strip():
                missing_fields.append(field_name)
        return missing_fields

    def to_state_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_state_dict(cls, payload: dict[str, Any]) -> "AmazonProductRecord":
        errors_payload = payload.get("errors", []) or []
        normalized_payload = dict(payload)
        normalized_payload["errors"] = [
            item if isinstance(item, CrawlError) else CrawlError(**item) for item in errors_payload
        ]
        return cls(**normalized_payload)

    def as_flat_dict(self) -> dict[str, Any]:
        return {
            "Rank": self.rank,
            "Title": self.title,
            "URL": self.url,
            "ASIN": self.asin,
            "Main Image URL": self.main_image_url,
            "Price": self.price,
            "Rating": self.rating,
            "Review Count": self.review_count,
            "Brand": self.brand,
            "Monthly Sales": self.monthly_sales,
            "Coupon/Discount": self.coupon_discount,
            "Dimensions/Weight": self.dimensions_weight,
            "Feature 1": self.feature_1,
            "Feature 2": self.feature_2,
            "Feature 3": self.feature_3,
            "Feature 4": self.feature_4,
            "Feature 5": self.feature_5,
            "Sub-category Rank": self.sub_category_rank,
            "A+ Content Flag": self.a_plus_content_flag,
            "Bad_Review_1": self.bad_review_1,
            "Bad_Review_2": self.bad_review_2,
            "Bad_Review_3": self.bad_review_3,
            "Bad_Review_4": self.bad_review_4,
            "Bad_Review_5": self.bad_review_5,
            "Crawled At": self.crawled_at,
            "Updated At": self.updated_at,
            "Source Category URL": self.source_category_url,
            "Errors": " | ".join(f"[{item.stage}] {item.message}" for item in self.errors),
        }
