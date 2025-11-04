"""Schedule providers for automatic race discovery."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

from urllib.request import Request, urlopen

from utils import get_logger

logger = get_logger(__name__)

RACE_ID_PATTERN = re.compile(r"(\d{8}\w{2}\d{2})")

_CACHE: Dict[Tuple[str, str], List[str]] = {}


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

    def list_race_ids(self, date_str: str) -> Tuple[List[str], Optional[str]]:
        cache_key = (self.name, date_str)
        if cache_key in _CACHE:
            cached = list(_CACHE[cache_key])
            logger.debug("Provider %s cache hit (%d ids)", self.name, len(cached))
            return cached, None if cached else "cache-empty"
        try:
            text = self.fetch(date_str)
        except Exception as exc:  # pragma: no cover - network errors
            logger.warning("Provider %s failed: %s", self.name, exc)
            _CACHE[cache_key] = []
            return [], f"error: {exc}"
        race_ids = self.extract(text)
        _CACHE[cache_key] = list(race_ids)
        logger.info("Provider %s yielded %d race ids", self.name, len(race_ids))
        if not race_ids:
            return race_ids, "empty"
        return race_ids, None


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
    status: Dict[str, str] = {}
    for name in selected:
        provider = provider_map.get(name)
        if not provider:
            continue
        race_ids, reason = provider.list_race_ids(date_str)
        if venues:
            race_ids = [rid for rid in race_ids if any(venue in rid for venue in venues)]
        if race_ids:
            results.extend(race_ids)
            status[name] = f"ok:{len(race_ids)}"
        else:
            status[name] = reason or "empty"
        if results:
            break
    if not results:
        detail = ", ".join(f"{key}={value}" for key, value in status.items()) or "no providers"
        logger.warning("No race ids found for %s (%s)", date_str, detail)
    return sorted(set(results))

