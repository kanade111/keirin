import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scrape import gamboo


class DummyHeaders(dict):
    def get_content_charset(self):
        return "utf-8"


class DummyResponse:
    def __init__(self, html: str):
        self.html = html
        self.headers = DummyHeaders()

    def read(self):
        return self.html.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


DAY_URL = "https://www.chariloto.com/keirin/results/01/2025-02-03"

DAY_HTML = """
<html>
  <body>
    <h1>松戸競輪 2025-02-03 の結果</h1>
    <table>
      <tr><th>着順</th><th>車番</th><th>選手名</th><th>脚質</th><th>得点</th><th>府県</th><th>級班</th><th>年齢</th><th>決まり手</th><th>Ｂ</th><th>Ｈ</th><th>Ｓ</th></tr>
      <tr><td>1</td><td>1</td><td>A選手</td><td>逃</td><td>95.1</td><td>千葉</td><td>S1</td><td>29</td><td>逃げ</td><td>2</td><td>1</td><td>1</td></tr>
      <tr><td>2</td><td>2</td><td>B選手</td><td>追</td><td>92.0</td><td>東京</td><td>S2</td><td>31</td><td></td><td>1</td><td>2</td><td>0</td></tr>
    </table>
    <table>
      <tr><th>ライン</th></tr>
      <tr><td>1-2</td></tr>
    </table>
    <table>
      <tr><th>着順</th><th>車番</th><th>選手名</th><th>脚質</th><th>得点</th><th>府県</th><th>級班</th><th>年齢</th><th>決まり手</th><th>Ｂ</th><th>Ｈ</th><th>Ｓ</th></tr>
      <tr><td>1</td><td>3</td><td>C選手</td><td>逃</td><td>91.2</td><td>神奈川</td><td>S2</td><td>28</td><td>差し</td><td>3</td><td>1</td><td>1</td></tr>
      <tr><td>2</td><td>4</td><td>D選手</td><td>追</td><td>90.0</td><td>埼玉</td><td>S2</td><td>30</td><td></td><td>0</td><td>1</td><td>0</td></tr>
    </table>
    <table>
      <tr><th>ライン</th></tr>
      <tr><td>3-4</td></tr>
    </table>
    <table>
      <tr><th>賭式</th><th>レース</th><th>払戻金額</th></tr>
      <tr><td>3連単</td><td>1R</td><td>1230</td></tr>
      <tr><td>3連単</td><td>2R</td><td>4560</td></tr>
    </table>
  </body>
</html>
"""


def fake_urlopen(request, timeout=10):
    url = getattr(request, "full_url", request)
    if url == DAY_URL:
        return DummyResponse(DAY_HTML)
    raise AssertionError(f"Unexpected URL {url}")


def test_chariloto_scrape_parses_results(monkeypatch):
    monkeypatch.setattr(gamboo, "urlopen", fake_urlopen)
    info_rows, entry_rows, payout_rows = gamboo.race_data_scrape(
        ["20250203CL0101", "20250203CL0102"],
        rate_limit=0.0,
        retries=1,
        timeout=5,
    )
    assert len(info_rows) == 2
    assert len(entry_rows) == 4
    first = next(row for row in entry_rows if row["race_id"] == "20250203CL0101" and row["lane_no"] == "1")
    assert first["kimarite_nige"] == "1"
    assert first["line_id"] == "1"
    assert first["line_pos"] == "0"
    second_race = [row for row in entry_rows if row["race_id"] == "20250203CL0102"]
    assert {row["lane_no"] for row in second_race} == {"3", "4"}
    assert payout_rows and all(row["race_id"].startswith("20250203CL01") for row in payout_rows)
    assert info_rows[0]["stadium"] == "松戸"
    assert info_rows[0]["track"] == "松戸"
