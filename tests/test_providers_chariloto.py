import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from schedule import providers


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


LIST_URL = "https://www.chariloto.com/keirin/results/01?year=2025"
DAY_URL = "https://www.chariloto.com/keirin/results/01/2025-02-03"

LIST_HTML = """
<html>
  <body>
    <a href="/keirin/results/01/2025-02-03">2025-02-03</a>
  </body>
</html>
"""

DAY_HTML = """
<html>
  <body>
    <h1>松戸競輪 2025-02-03 の結果</h1>
    <table>
      <tr><th>着順</th><th>車番</th><th>選手名</th><th>決まり手</th></tr>
      <tr><td>1</td><td>1</td><td>A選手</td><td>逃げ</td></tr>
      <tr><td>2</td><td>2</td><td>B選手</td><td></td></tr>
    </table>
    <table>
      <tr><th>ライン</th></tr>
      <tr><td>1-2</td></tr>
    </table>
    <table>
      <tr><th>着順</th><th>車番</th><th>選手名</th><th>決まり手</th></tr>
      <tr><td>1</td><td>3</td><td>C選手</td><td>差し</td></tr>
      <tr><td>2</td><td>4</td><td>D選手</td><td></td></tr>
    </table>
    <table>
      <tr><th>ライン</th></tr>
      <tr><td>3-4</td></tr>
    </table>
  </body>
</html>
"""


def fake_urlopen(request, timeout=10):
    url = getattr(request, "full_url", request)
    if url == LIST_URL:
        return DummyResponse(LIST_HTML)
    if url == DAY_URL:
        return DummyResponse(DAY_HTML)
    return DummyResponse("<html></html>")


def test_chariloto_provider_lists_race_ids(monkeypatch):
    providers._CACHE.clear()
    monkeypatch.setattr(providers.Chariloto, "min_interval", 0.0)
    monkeypatch.setattr(providers, "urlopen", fake_urlopen)
    race_ids = providers.list_race_ids_for_date("2025-02-03", providers="chariloto")
    assert race_ids == ["20250203CL0101", "20250203CL0102"]
