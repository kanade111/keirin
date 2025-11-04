"""Normalization helpers for scraped data."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from utils import ensure_directory, get_logger

logger = get_logger(__name__)


def _write_csv(path: str | Path, rows: List[Dict[str, str]]) -> None:
    if not rows:
        return
    ensure_directory(Path(path).parent)
    headers = list(rows[0].keys())
    with open(path, "w", encoding="utf-8") as f:
        f.write(",".join(headers) + "\n")
        for row in rows:
            values = [str(row.get(h, "")) for h in headers]
            f.write(",".join(values) + "\n")


def to_training_csv(entry_rows: List[Dict[str, str]], info_rows: List[Dict[str, str]], payout_rows: List[Dict[str, str]], out_path: str) -> None:
    info_map: Dict[str, Dict[str, str]] = {}
    for info in info_rows:
        rid = str(info.get("race_id"))
        info_map[rid] = info

    enriched: List[Dict[str, str]] = []
    for row in entry_rows:
        rid = str(row.get("race_id"))
        combined = dict(info_map.get(rid, {}))
        combined.update(row)
        enriched.append(combined)

    if enriched:
        _write_csv(out_path, enriched)
        logger.info("Training CSV written to %s", out_path)


def to_cards_csv(entry_rows: List[Dict[str, str]], out_path: str) -> None:
    cards: List[Dict[str, str]] = []
    for row in entry_rows:
        card = dict(row)
        card.pop("finish_pos", None)
        cards.append(card)
    if cards:
        _write_csv(out_path, cards)
        logger.info("Cards CSV written to %s", out_path)

