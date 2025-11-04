"""Schedule providers for automatic race discovery."""
from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from io import StringIO
from typing import Callable, Dict, List, Optional, Tuple

from urllib.request import Request, urlopen

try:  # pragma: no cover - optional dependency
    import pandas as pd  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback
    from pandas_compat import pd  # type: ignore

import re
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple
from typing import Callable, Iterable, List, Optional

from urllib.request import Request, urlopen

from utils import get_logger

logger = get_logger(__name__)

RACE_ID_PATTERN = re.compile(r"(\d{8}\w{2}\d{2,4})")

_CACHE: Dict[Tuple[str, str], List[str]] = {}

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"


def _rate_limited_fetch(url: str, timeout: int = 10, min_interval: float = 1.0) -> str:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    if not hasattr(_rate_limited_fetch, "_last_request"):
        _rate_limited_fetch._last_request = 0.0  # type: ignore[attr-defined]
    last_request: float = getattr(_rate_limited_fetch, "_last_request")  # type: ignore[attr-defined]
    elapsed = time.time() - last_request
    if elapsed < min_interval:
        wait = min_interval - elapsed + random.uniform(0.0, 0.3)
        if wait > 0:
            time.sleep(wait)
    for attempt in range(3):
        try:
            with urlopen(request, timeout=timeout) as resp:
                data = resp.read()
                encoding = resp.headers.get_content_charset() or "utf-8"
            getattr(_rate_limited_fetch, "__dict__")["_last_request"] = time.time()  # type: ignore[attr-defined]
            return data.decode(encoding, errors="ignore")
        except Exception as exc:  # pragma: no cover - network errors
            logger.warning("Fetch failed for %s (%s) attempt %d", url, exc, attempt + 1)
            time.sleep(1.0 + random.uniform(0.0, 0.5))
    raise RuntimeError(f"Failed to fetch {url}")

RACE_ID_PATTERN = re.compile(r"(\d{8}\w{2}\d{2})")

_CACHE: Dict[Tuple[str, str], List[str]] = {}


@dataclass
class Provider:
    name: str
    url_builder: Callable[[str], str]

    def fetch(self, date_str: str) -> str:
        url = self.url_builder(date_str)
        logger.info("Provider %s requesting %s", self.name, url)
        return _rate_limited_fetch(url)
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
    def list_race_ids(self, date_str: str) -> List[str]:
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


class CharilotoProvider:
    """Provider that enumerates race ids from chariloto.com results pages."""

    name = "chariloto"
    BANK_CODES: Tuple[str, ...] = (
        "01",
        "02",
        "03",
        "04",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
        "11",
        "12",
        "13",
        "14",
        "15",
        "16",
        "17",
        "18",
        "19",
        "20",
        "21",
        "22",
        "23",
        "24",
        "25",
        "26",
        "27",
        "28",
        "29",
        "30",
        "31",
        "32",
        "33",
        "34",
        "35",
        "36",
        "37",
        "38",
        "39",
        "40",
        "41",
        "42",
        "43",
        "44",
        "45",
        "46",
        "47",
        "48",
        "49",
        "50",
        "51",
        "52",
    )

    LIST_URL = "https://www.chariloto.com/keirin/results/{bank}?year={year}"
    DAY_URL = "https://www.chariloto.com/keirin/results/{bank}/{date}"

    def __init__(self, min_interval: float = 1.0) -> None:
        self.min_interval = min_interval

    def _list_has_date(self, html: str, bank: str, date_str: str) -> bool:
        path = f"/keirin/results/{bank}/{date_str}"
        return path in html

    def _extract_race_count(self, html: str) -> int:
        try:
            tables = pd.read_html(StringIO(html))
        except ValueError:
            return 0
        count = 0
        for table in tables:
            columns = ["".join(str(part) for part in (col if isinstance(col, tuple) else (col,))) for col in table.columns]
            joined = "".join(columns)
            if "着" in joined and "車" in joined:
                count += 1
        return count

    def list_race_ids(self, date_str: str) -> Tuple[List[str], Optional[str]]:
        cache_key = (self.name, date_str)
        if cache_key in _CACHE:
            cached = list(_CACHE[cache_key])
            logger.debug("Provider %s cache hit (%d ids)", self.name, len(cached))
            return cached, None if cached else "cache-empty"

        race_ids: List[str] = []
        year = date_str.split("-")[0]
        for bank in self.BANK_CODES:
            list_url = self.LIST_URL.format(bank=bank, year=year)
            try:
                logger.info("Provider %s requesting %s", self.name, list_url)
                html = _rate_limited_fetch(list_url, min_interval=self.min_interval)
            except Exception as exc:  # pragma: no cover - network errors
                logger.debug("Provider %s bank %s list fetch failed: %s", self.name, bank, exc)
                continue
            if not self._list_has_date(html, bank, date_str):
                continue
            day_url = self.DAY_URL.format(bank=bank, date=date_str)
            try:
                logger.info("Provider %s requesting %s", self.name, day_url)
                day_html = _rate_limited_fetch(day_url, min_interval=self.min_interval)
            except Exception as exc:  # pragma: no cover - network errors
                logger.warning("Provider %s failed to fetch day page %s: %s", self.name, day_url, exc)
                continue
            race_count = self._extract_race_count(day_html)
            logger.info(
                "Provider %s bank %s yielded %d races for %s",
                self.name,
                bank,
                race_count,
                date_str,
            )
            for idx in range(race_count):
                race_no = idx + 1
                rid = f"{date_str.replace('-', '')}CL{bank}{race_no:02d}"
                race_ids.append(rid)
        race_ids = sorted(set(race_ids))
        _CACHE[cache_key] = list(race_ids)
        if not race_ids:
            return race_ids, "empty"
        return race_ids, None
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

Chariloto = CharilotoProvider()


def list_race_ids_for_date(
    date_str: str,
    venues: Optional[List[str]] = None,
    providers: str = "chariloto,kdreams,keirin_jp",
) -> List[str]:
    provider_map = {
        "chariloto": Chariloto,
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
        race_ids = provider.list_race_ids(date_str)
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

