from typing import Any

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest

from scrape.gamboo import race_data_scrape
from scrape.normalize import to_cards_csv, to_training_csv


HTML_TEMPLATE = """
<html>
  <body>
    <table data-table="info">
      <tr><th>race_id</th><th>date</th><th>track</th><th>grade</th></tr>
      <tr><td>{race_id}</td><td>2023-01-01</td><td>Test Stadium</td><td>G3</td></tr>
    </table>
    <table data-table="entries">
      <tr>
        <th>車番</th><th>選手名</th><th>競走得点</th><th>脚質</th><th>年齢</th>
        <th>Ｂ</th><th>Ｈ</th><th>Ｓ</th><th>逃</th><th>捲</th><th>差</th><th>マ</th><th>着順</th>
      </tr>
      <tr>
        <td>1</td><td>Rider A</td><td>90</td><td>逃</td><td>30</td>
        <td>1</td><td>2</td><td>3</td><td>1</td><td>0</td><td>0</td><td>0</td><td>1</td>
      </tr>
      <tr>
        <td>2</td><td>Rider B</td><td>88</td><td>追</td><td>29</td>
        <td>0</td><td>1</td><td>2</td><td>0</td><td>1</td><td>0</td><td>0</td><td>2</td>
      </tr>
    </table>
    <table data-table="payout">
      <tr><th>種別</th><th>組番</th><th>金額</th></tr>
      <tr><td>三連複</td><td>1-2-3</td><td>1000</td></tr>
    </table>
  </body>
</html>
"""


def fake_fetch(url: str, timeout: int) -> str:
    return HTML_TEMPLATE.format(race_id="2023010111")


@pytest.fixture(autouse=True)
def patch_fetch(monkeypatch):
    monkeypatch.setattr("scrape.gamboo._fetch", fake_fetch)
    yield


def test_race_data_scrape(tmp_path):
    info_rows, entry_rows, payout_rows = race_data_scrape(["2023010111"])
    assert info_rows and entry_rows and payout_rows
    assert entry_rows[0]["rider_name"] == "Rider A"

    train_path = tmp_path / "races.csv"
    cards_path = tmp_path / "cards.csv"
    to_training_csv(entry_rows, info_rows, payout_rows, str(train_path))
    to_cards_csv(entry_rows, str(cards_path))

    assert train_path.exists()
    assert cards_path.exists()
    with open(train_path, "r", encoding="utf-8") as f:
        content = f.read()
    assert "finish_pos" in content
    with open(cards_path, "r", encoding="utf-8") as f:
        content_cards = f.read()
    assert "finish_pos" not in content_cards

