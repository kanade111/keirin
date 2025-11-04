import csv
from pathlib import Path

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import bets
import model


def _write_csv(path: Path, rows):
    if not rows:
        return
    headers = list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _make_dataset():
    rows = []
    for race_no in range(1, 3):
        race_id = f"2023010{race_no:02d}11"
        for lane in range(1, 7):
            rows.append(
                {
                    "race_id": race_id,
                    "date": f"2023-01-0{race_no}",
                    "track": "test_track",
                    "class": "S1",
                    "grade": "G3",
                    "lane_no": lane,
                    "rider_name": f"rider_{race_no}_{lane}",
                    "score": str(90 + lane),
                    "style": "逃" if lane % 3 == 0 else "追",
                    "backs": str(2 + lane),
                    "homes": str(1 + lane),
                    "starts": str(1 + lane),
                    "win_rate": str(0.2 + lane * 0.01),
                    "quinella_rate": str(0.3 + lane * 0.01),
                    "top3_rate": str(0.4 + lane * 0.01),
                    "kimarite_nige": str(1 + lane),
                    "kimarite_makuri": str(0.5 + lane),
                    "kimarite_sashi": str(0.3 + lane),
                    "kimarite_mark": str(0.2 + lane),
                    "finish_pos": str((lane % 3) + 1),
                    "age": str(30 + lane),
                    "prefecture": "tokyo",
                }
            )
    return rows


def test_train_predict(tmp_path):
    races_rows = _make_dataset()
    races_path = tmp_path / "races.csv"
    _write_csv(races_path, races_rows)

    model_dir = tmp_path / "model"
    model_dir.mkdir()
    model.train(str(races_path), str(model_dir))

    cards_rows = [{k: v for k, v in row.items() if k != "finish_pos"} for row in races_rows]
    cards_path = tmp_path / "cards.csv"
    _write_csv(cards_path, cards_rows)

    preds = model.predict(str(cards_path), str(model_dir), ct_method="independent")
    assert preds
    assert all("top3_prob" in row for row in preds)

    bet_table = bets.suggest_bets(preds, model.config_thresholds(), budget=1000)
    assert bet_table
    assert all("allocation_json" in row for row in bet_table)

