"""Simple scraper utilities implemented with the standard library."""
from __future__ import annotations

import random
import re
import time
from html import unescape
from io import StringIO
from typing import Dict, Iterable, List, Optional, Tuple

from urllib.request import Request, urlopen

try:  # pragma: no cover - optional dependency
    import pandas as pd  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback
    from pandas_compat import pd  # type: ignore

from utils import get_logger

logger = get_logger(__name__)

BASE_URL = "https://keirin.kdreams.jp/gamboo/keirin-kaisai/race-card/result/{}/{}/{}"
CHARILOTO_URL = "https://www.chariloto.com/keirin/results/{bank}/{date}"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"


def _clean_text(value: str) -> str:
    cleaned = unescape(re.sub(r"<.*?>", "", value)).strip()
    return re.sub(r"\s+", " ", cleaned)


def _race_urls(race_id: str) -> str:
    first = race_id[:10]
    second = race_id[:-2]
    third = race_id[-2:]
    return BASE_URL.format(first, second, third)


def _rate_limit_sleep(last_request: float, rate_limit: float) -> float:
    if rate_limit <= 0:
        return last_request
    now = time.time()
    elapsed = now - last_request
    if elapsed < rate_limit:
        wait = rate_limit - elapsed + random.uniform(0.0, min(0.3, rate_limit))
        if wait > 0:
            time.sleep(wait)
    return time.time()


def _fetch(url: str, timeout: int) -> str:
    headers = {"User-Agent": USER_AGENT}
    request = Request(url, headers=headers)
    with urlopen(request, timeout=timeout) as resp:
        data = resp.read()
        encoding = resp.headers.get_content_charset() or "utf-8"
    return data.decode(encoding, errors="ignore")


def _fetch_with_retries(
    url: str,
    timeout: int,
    retries: int,
    rate_limit: float,
    last_request: float,
) -> Tuple[Optional[str], float]:
    html: Optional[str] = None
    for attempt in range(retries):
        last_request = _rate_limit_sleep(last_request, rate_limit)
        try:
            html = _fetch(url, timeout)
            last_request = time.time()
            break
        except Exception as exc:  # pragma: no cover
            logger.warning("Fetch failed for %s (%s), retry %d", url, exc, attempt + 1)
            time.sleep(1.0 + min(attempt, 2) * 0.5)
    return html, last_request


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
    "B": "backs",
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


def _is_chariloto_race(race_id: str) -> bool:
    return len(race_id) >= 12 and race_id[8:10].upper() == "CL"


def _flatten_columns(columns: Iterable) -> List[str]:
    flattened: List[str] = []
    for col in columns:
        if isinstance(col, tuple):
            parts = [str(part) for part in col if part and not str(part).startswith("Unnamed")]
            name = "".join(parts)
        else:
            name = str(col)
        flattened.append(name.strip())
    return flattened


def _normalize_value(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):  # type: ignore[attr-defined]
            return ""
    except TypeError:
        pass
    return str(value).strip()


def _normalize_lane(value: str) -> str:
    digits = re.findall(r"\d+", value)
    if digits:
        return str(int(digits[0]))
    return value.strip()


def _find_column(columns: List[str], keywords: List[str]) -> Optional[str]:
    for keyword in keywords:
        for column in columns:
            if keyword in column:
                return column
    return None


def _build_line_map(df: pd.DataFrame) -> Dict[str, Tuple[str, int]]:
    mapping: Dict[str, Tuple[str, int]] = {}
    line_index = 1
    for _, row in df.iterrows():
        values = [
            _normalize_value(val)
            for val in row.values
            if _normalize_value(val)
        ]
        if not values:
            continue
        text = " ".join(values)
        lanes = [_normalize_lane(match) for match in re.findall(r"\d+", text)]
        if not lanes:
            continue
        if len(lanes) == 1:
            mapping[lanes[0]] = (str(line_index), 0)
        else:
            for pos, lane in enumerate(lanes):
                mapping[lane] = (str(line_index), pos)
        line_index += 1
    return mapping


def _chariloto_entries(
    df: pd.DataFrame,
    race_id: str,
    date_str: str,
    race_no: int,
    bank_code: str,
    line_map: Dict[str, Tuple[str, int]],
    stadium: str,
) -> List[Dict[str, str]]:
    columns = _flatten_columns(df.columns)
    df.columns = columns
    finish_col = _find_column(columns, ["着順", "着"])
    lane_col = _find_column(columns, ["車番", "車号", "車"])
    name_col = _find_column(columns, ["選手名", "選手"])
    style_col = _find_column(columns, ["脚質"])
    score_col = _find_column(columns, ["得点", "競走得点"])
    age_col = _find_column(columns, ["年齢"])
    pref_col = _find_column(columns, ["府県", "都道府県"])
    class_col = _find_column(columns, ["級班", "級"])
    gear_col = _find_column(columns, ["ギヤ", "ギア"])
    b_col = _find_column(columns, ["Ｂ", "B"])
    h_col = _find_column(columns, ["Ｈ", "H"])
    s_col = _find_column(columns, ["Ｓ", "S"])
    kimari_col = _find_column(columns, ["決まり手", "決め手"])

    entries: List[Dict[str, str]] = []
    defaults = {
        "race_id": race_id,
        "date": date_str,
        "race_no": f"{race_no:02d}",
        "bank_code": bank_code,
        "source": "chariloto",
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
        "line_id": "",
        "line_pos": "",
        "prefecture": "",
        "class": "",
        "age": "",
        "gear": "",
        "track": stadium,
    }
    if finish_col is None or lane_col is None:
        return entries

    for _, row in df.iterrows():
        finish_val = _normalize_value(row.get(finish_col, ""))
        lane_val_raw = _normalize_value(row.get(lane_col, ""))
        if not finish_val or not lane_val_raw:
            continue
        lane_val = _normalize_lane(lane_val_raw)
        entry = dict(defaults)
        entry["lane_no"] = lane_val
        entry["finish_pos"] = finish_val
        if name_col:
            entry["rider_name"] = _normalize_value(row.get(name_col, ""))
        if style_col:
            entry["style"] = _normalize_value(row.get(style_col, ""))
        if score_col:
            entry["score"] = _normalize_value(row.get(score_col, ""))
        if age_col:
            entry["age"] = _normalize_value(row.get(age_col, ""))
        if pref_col:
            entry["prefecture"] = _normalize_value(row.get(pref_col, ""))
        if class_col:
            entry["class"] = _normalize_value(row.get(class_col, ""))
        if gear_col:
            entry["gear"] = _normalize_value(row.get(gear_col, ""))
        if b_col:
            entry["backs"] = _normalize_value(row.get(b_col, "0")) or "0"
        if h_col:
            entry["homes"] = _normalize_value(row.get(h_col, "0")) or "0"
        if s_col:
            entry["starts"] = _normalize_value(row.get(s_col, "0")) or "0"
        technique = _normalize_value(row.get(kimari_col, "")) if kimari_col else ""
        if finish_val == "1":
            if "逃" in technique:
                entry["kimarite_nige"] = "1"
            if "捲" in technique or "捲り" in technique:
                entry["kimarite_makuri"] = "1"
            if "差" in technique:
                entry["kimarite_sashi"] = "1"
            if "マ" in technique:
                entry["kimarite_mark"] = "1"
        if lane_val in line_map:
            line_id, line_pos = line_map[lane_val]
            entry["line_id"] = line_id
            entry["line_pos"] = str(line_pos)
        entries.append(entry)
    return entries


def _chariloto_payouts(
    tables: List[pd.DataFrame],
    race_ids: List[str],
) -> List[Dict[str, str]]:
    payout_rows: List[Dict[str, str]] = []
    if not tables:
        return payout_rows
    for table in tables:
        df = table.copy()
        columns = _flatten_columns(df.columns)
        df.columns = columns
        race_col = _find_column(columns, ["レース", "R", "レースNo", "レースNO"])
        for _, row in df.iterrows():
            record = {col: _normalize_value(row.get(col, "")) for col in columns}
            race_id = None
            if race_col:
                label = record.get(race_col, "")
                match = re.search(r"\d+", label)
                if match:
                    idx = int(match.group()) - 1
                    if 0 <= idx < len(race_ids):
                        race_id = race_ids[idx]
            if race_id is None:
                continue
            record["race_id"] = race_id
            record["source"] = "chariloto"
            payout_rows.append(record)
    return payout_rows


def _extract_title(html: str) -> str:
    match = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.S)
    if not match:
        return ""
    return _clean_text(match.group(1))


def _extract_stadium(title: str) -> str:
    if not title:
        return ""
    match = re.search(r"(.+?)競輪", title)
    if match:
        return match.group(1).strip()
    return title


def _parse_chariloto_day(
    date_str: str,
    bank_code: str,
    html: str,
    race_ids: List[str],
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]]]:
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return [], [], []

    result_tables: List[pd.DataFrame] = []
    line_tables: List[pd.DataFrame] = []
    payout_tables: List[pd.DataFrame] = []
    for table in tables:
        df = table.copy()
        columns = _flatten_columns(df.columns)
        joined = "".join(columns)
        if "着" in joined and "車" in joined:
            result_tables.append(df)
        elif "ライン" in joined or "並び" in joined:
            line_tables.append(df)
        elif "払戻" in joined or "払戻金" in joined or "賭式" in joined:
            payout_tables.append(df)

    line_maps = [_build_line_map(table.copy()) for table in line_tables]
    title = _extract_title(html)
    stadium = _extract_stadium(title)

    info_rows: List[Dict[str, str]] = []
    entry_rows: List[Dict[str, str]] = []
    payout_rows: List[Dict[str, str]] = []

    sorted_ids = sorted(race_ids, key=lambda rid: int(rid[-2:]))
    for idx, race_id in enumerate(sorted_ids):
        race_no = int(race_id[-2:]) if race_id[-2:].isdigit() else idx + 1
        line_map = line_maps[idx] if idx < len(line_maps) else {}
        result_df = result_tables[idx] if idx < len(result_tables) else None
        if result_df is None:
            continue
        entries = _chariloto_entries(result_df, race_id, date_str, race_no, bank_code, line_map, stadium)
        if not entries:
            continue
        entry_rows.extend(entries)
        line_ids = {line_id for line_id, _ in line_map.values() if line_id}
        pattern_segments: List[str] = []
        for line_id in sorted(line_ids, key=lambda x: int(x)):
            count = sum(1 for value in line_map.values() if value[0] == line_id)
            pattern_segments.append(str(count))
        info_rows.append(
            {
                "race_id": race_id,
                "date": date_str,
                "race_no": f"{race_no:02d}",
                "stadium": stadium,
                "track": stadium or f"bank_{bank_code}",
                "title": title,
                "grade": "",
                "weather": "",
                "wind": "",
                "field_size": str(len(entries)),
                "line_count": str(len(line_ids)) if line_ids else "0",
                "line_pattern": "/".join(pattern_segments),
                "bank_code": bank_code,
                "source": "chariloto",
            }
        )

    payout_rows.extend(_chariloto_payouts(payout_tables, sorted_ids))
    return info_rows, entry_rows, payout_rows


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
    chariloto_groups: Dict[Tuple[str, str], List[str]] = {}
    standard_ids: List[str] = []

    for race_id in race_ids:
        rid = str(race_id)
        if _is_chariloto_race(rid):
            date_digits = rid[:8]
            date_str = f"{date_digits[:4]}-{date_digits[4:6]}-{date_digits[6:8]}"
            bank_code = rid[10:12]
            key = (date_str, bank_code)
            chariloto_groups.setdefault(key, []).append(rid)
        else:
            standard_ids.append(rid)

    for race_id in standard_ids:
        url = _race_urls(str(race_id))
        html, last_request = _fetch_with_retries(url, timeout, retries, rate_limit, last_request)
        if html is None:
            continue
        info_rows.extend(_parse_info(html, str(race_id)))
        entry_rows.extend(_parse_entries(html, str(race_id)))
        payout_rows.extend(_parse_payout(html, str(race_id)))

    for (date_str, bank_code), ids in chariloto_groups.items():
        url = CHARILOTO_URL.format(bank=bank_code, date=date_str)
        html, last_request = _fetch_with_retries(url, timeout, retries, rate_limit, last_request)
        if html is None:
            continue
        info, entries, payouts = _parse_chariloto_day(date_str, bank_code, html, ids)
        info_rows.extend(info)
        entry_rows.extend(entries)
        payout_rows.extend(payouts)

    return info_rows, entry_rows, payout_rows

