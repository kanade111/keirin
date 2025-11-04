"""Normalization helpers for scraped data."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

from utils import ensure_directory, get_logger

logger = get_logger(__name__)

ENTRY_DEFAULTS: Dict[str, str] = {
    "race_id": "",
    "date": "",
    "race_no": "",
    "stadium": "",
    "track": "",
    "class": "",
    "grade": "",
    "lane_no": "",
    "rider_id": "",
    "rider_name": "",
    "score": "",
    "style": "",
    "backs": "0",
    "homes": "0",
    "starts": "0",
    "win_rate": "0",
    "quinella_rate": "0",
    "top3_rate": "0",
    "kimarite_nige": "0",
    "kimarite_makuri": "0",
    "kimarite_sashi": "0",
    "kimarite_mark": "0",
    "finish_pos": "",
    "age": "",
    "prefecture": "",
    "gear": "",
    "term": "",
    "line_id": "",
    "line_pos": "",
    "bank_code": "",
    "source": "",
}

INFO_DEFAULTS: Dict[str, str] = {
    "race_id": "",
    "date": "",
    "race_no": "",
    "stadium": "",
    "track": "",
    "title": "",
    "race_name": "",
    "grade": "",
    "start_time": "",
    "weather": "",
    "wind": "",
    "kaizai_no": "",
    "field_size": "",
    "line_count": "",
    "line_pattern": "",
    "bank_code": "",
    "source": "",
}

TRAINING_COLUMNS_ORDER: List[str] = [
    "race_id",
    "date",
    "race_no",
    "stadium",
    "track",
    "title",
    "race_name",
    "grade",
    "class",
    "lane_no",
    "rider_id",
    "rider_name",
    "age",
    "prefecture",
    "score",
    "style",
    "backs",
    "homes",
    "starts",
    "win_rate",
    "quinella_rate",
    "top3_rate",
    "kimarite_nige",
    "kimarite_makuri",
    "kimarite_sashi",
    "kimarite_mark",
    "finish_pos",
    "line_id",
    "line_pos",
    "gear",
    "bank_code",
    "source",
    "field_size",
    "line_count",
    "line_pattern",
]

CARDS_COLUMNS_ORDER: List[str] = [col for col in TRAINING_COLUMNS_ORDER if col != "finish_pos"]


def _collect_headers(rows: Iterable[Dict[str, str]], base_order: List[str]) -> List[str]:
    headers = list(base_order)
    seen = set(headers)
    for row in rows:
        for key in row.keys():
            if key not in seen:
                headers.append(key)
                seen.add(key)
    return headers


def _write_csv(path: str | Path, rows: List[Dict[str, str]], preferred_order: List[str]) -> None:
    if not rows:
        return
    ensure_directory(Path(path).parent)
    headers = _collect_headers(rows, preferred_order)
    with open(path, "w", encoding="utf-8") as f:
        f.write(",".join(headers) + "\n")
        for row in rows:
            values = [str(row.get(h, "")) for h in headers]
            f.write(",".join(values) + "\n")


def _normalize_row(row: Dict[str, str], defaults: Dict[str, str]) -> Dict[str, str]:
    normalized = dict(defaults)
    for key, value in row.items():
        if value is None:
            continue
        normalized[key] = str(value)
    return normalized


def to_training_csv(entry_rows: List[Dict[str, str]], info_rows: List[Dict[str, str]], payout_rows: List[Dict[str, str]], out_path: str) -> None:
    info_map: Dict[str, Dict[str, str]] = {}
    for info in info_rows:
        rid = str(info.get("race_id"))
        info_map[rid] = _normalize_row(info, INFO_DEFAULTS)

    enriched: List[Dict[str, str]] = []
    for row in entry_rows:
        rid = str(row.get("race_id"))
        entry_norm = _normalize_row(row, ENTRY_DEFAULTS)
        info_norm = info_map.get(rid, dict(INFO_DEFAULTS))
        combined = dict(info_norm)
        combined.update(entry_norm)
        if not combined.get("track") and combined.get("stadium"):
            combined["track"] = combined.get("stadium", "")
        if not combined.get("date"):
            combined["date"] = entry_norm.get("date", "")
        enriched.append(combined)

    if enriched:
        _write_csv(out_path, enriched, TRAINING_COLUMNS_ORDER)
        logger.info("Training CSV written to %s", out_path)


def to_cards_csv(entry_rows: List[Dict[str, str]], out_path: str) -> None:
    cards: List[Dict[str, str]] = []
    for row in entry_rows:
        entry_norm = _normalize_row(row, ENTRY_DEFAULTS)
        entry_norm.pop("finish_pos", None)
        cards.append(entry_norm)
    if cards:
        _write_csv(out_path, cards, CARDS_COLUMNS_ORDER)
        logger.info("Cards CSV written to %s", out_path)

