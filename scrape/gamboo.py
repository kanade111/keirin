"""Simple scraper utilities implemented with the standard library."""
from __future__ import annotations

import re
import time
from html import unescape
from typing import Dict, Iterable, List, Optional, Tuple

from urllib.request import Request, urlopen

from utils import get_logger

logger = get_logger(__name__)

BASE_URL = "https://keirin.kdreams.jp/gamboo/keirin-kaisai/race-card/result/{}/{}/{}"


def _race_urls(race_id: str) -> str:
    first = race_id[:10]
    second = race_id[:-2]
    third = race_id[-2:]
    return BASE_URL.format(first, second, third)


def _fetch(url: str, timeout: int) -> str:
    headers = {"User-Agent": "keirin-yosou-bot/0.1"}
    request = Request(url, headers=headers)
    with urlopen(request, timeout=timeout) as resp:
        data = resp.read()
        encoding = resp.headers.get_content_charset() or "utf-8"
    return data.decode(encoding, errors="ignore")


def _extract_table(html: str, data_attr: str) -> List[List[str]]:
    pattern = re.compile(rf"<table[^>]*data-table=\"{data_attr}\"[^>]*>(.*?)</table>", re.S)
    match = pattern.search(html)
    if not match:
        return []
    table_html = match.group(1)
    rows_html = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.S)
    rows: List[List[str]] = []
    for row_html in rows_html:
        cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", row_html, re.S)
        cleaned = [unescape(re.sub(r"<.*?>", "", cell).strip()) for cell in cells]
        if cleaned:
            rows.append(cleaned)
    return rows


def _table_to_dicts(table: List[List[str]]) -> List[Dict[str, str]]:
    if not table:
        return []
    headers = table[0]
    dicts: List[Dict[str, str]] = []
    for row in table[1:]:
        record = {header: row[idx] if idx < len(row) else "" for idx, header in enumerate(headers)}
        dicts.append(record)
    return dicts


def _parse_info(html: str, race_id: str) -> List[Dict[str, str]]:
    table = _extract_table(html, "info")
    records = _table_to_dicts(table)
    for record in records:
        record["race_id"] = race_id
    if not records:
        records = [{"race_id": race_id}]
    return records


ENTRY_HEADER_MAP = {
    "車番": "lane_no",
    "lane": "lane_no",
    "選手名": "rider_name",
    "選手": "rider_name",
    "競走得点": "score",
    "得点": "score",
    "脚質": "style",
    "年齢": "age",
    "Ｂ": "backs",
    "H": "homes",
    "Ｈ": "homes",
    "Ｓ": "starts",
    "S": "starts",
    "逃": "kimarite_nige",
    "捲": "kimarite_makuri",
    "差": "kimarite_sashi",
    "マ": "kimarite_mark",
    "着順": "finish_pos",
}


def _parse_entries(html: str, race_id: str) -> List[Dict[str, str]]:
    table = _extract_table(html, "entries")
    if not table:
        table = _extract_table(html, "entry")
    dicts = _table_to_dicts(table)
    for record in dicts:
        record["race_id"] = race_id
        normalized: Dict[str, str] = {"race_id": race_id}
        for key, value in record.items():
            target = ENTRY_HEADER_MAP.get(key, key)
            normalized[target] = value
        record.clear()
        record.update(normalized)
    return dicts


def _parse_payout(html: str, race_id: str) -> List[Dict[str, str]]:
    table = _extract_table(html, "payout")
    dicts = _table_to_dicts(table)
    for record in dicts:
        record["race_id"] = race_id
    return dicts


def race_data_scrape(
    race_ids: Iterable[str],
    rate_limit: float = 1.0,
    retries: int = 3,
    timeout: int = 15,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]]]:
    info_rows: List[Dict[str, str]] = []
    entry_rows: List[Dict[str, str]] = []
    payout_rows: List[Dict[str, str]] = []
    last_request = 0.0

    for race_id in race_ids:
        now = time.time()
        elapsed = now - last_request
        if elapsed < rate_limit:
            time.sleep(rate_limit - elapsed)
        url = _race_urls(str(race_id))
        html: Optional[str] = None
        for attempt in range(retries):
            try:
                html = _fetch(url, timeout)
                break
            except Exception as exc:  # pragma: no cover
                logger.warning("Fetch failed for %s (%s), retry %d", race_id, exc, attempt + 1)
                time.sleep(1.0)
        if html is None:
            continue
        last_request = time.time()
        info_rows.extend(_parse_info(html, str(race_id)))
        entry_rows.extend(_parse_entries(html, str(race_id)))
        payout_rows.extend(_parse_payout(html, str(race_id)))

    return info_rows, entry_rows, payout_rows

