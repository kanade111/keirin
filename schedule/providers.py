"""Schedule providers for automatic race discovery."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional

from urllib.request import Request, urlopen

from utils import get_logger

logger = get_logger(__name__)

RACE_ID_PATTERN = re.compile(r"(\d{8}\w{2}\d{2})")


@dataclass
class Provider:
    name: str
    url_builder: Callable[[str], str]

    def fetch(self, date_str: str) -> str:
        url = self.url_builder(date_str)
        headers = {"User-Agent": "keirin-yosou-bot/0.1"}
        request = Request(url, headers=headers)
        with urlopen(request, timeout=10) as resp:
            data = resp.read()
            encoding = resp.headers.get_content_charset() or "utf-8"
        return data.decode(encoding, errors="ignore")

    def extract(self, text: str) -> List[str]:
        return sorted(set(RACE_ID_PATTERN.findall(text)))

    def list_race_ids(self, date_str: str) -> List[str]:
        try:
            text = self.fetch(date_str)
        except Exception as exc:  # pragma: no cover - network errors
            logger.warning("Provider %s failed: %s", self.name, exc)
            return []
        race_ids = self.extract(text)
        logger.info("Provider %s yielded %d race ids", self.name, len(race_ids))
        return race_ids


KdreamsProvider = Provider(
    name="kdreams",
    url_builder=lambda date_str: f"https://keirin.kdreams.jp/gamboo/keirin-kaisai/schedule/{date_str.replace('-', '')}",
)

KeirinJpProvider = Provider(
    name="keirin_jp",
    url_builder=lambda date_str: f"https://keirin.jp/pc/racecalendar?date={date_str.replace('-', '')}",
)


def list_race_ids_for_date(
    date_str: str,
    venues: Optional[List[str]] = None,
    providers: str = "kdreams,keirin_jp",
) -> List[str]:
    provider_map = {
        "kdreams": KdreamsProvider,
        "keirin_jp": KeirinJpProvider,
    }
    selected = [p.strip() for p in providers.split(",") if p.strip()]
    results: List[str] = []
    for name in selected:
        provider = provider_map.get(name)
        if not provider:
            continue
        race_ids = provider.list_race_ids(date_str)
        if venues:
            race_ids = [rid for rid in race_ids if any(venue in rid for venue in venues)]
        if race_ids:
            results.extend(race_ids)
    return sorted(set(results))

