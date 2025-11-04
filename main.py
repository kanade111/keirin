"""Command line interface for the keirin prediction project."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Optional

import bets
import model
from schedule.providers import list_race_ids_for_date
from scrape.gamboo import race_data_scrape
from scrape.normalize import to_cards_csv, to_training_csv
from utils import TimeResolver, ensure_directory, get_logger, read_yaml

logger = get_logger(__name__)


def _parse_thresholds(config_path: Optional[str]) -> Optional[dict]:
    if not config_path:
        return None
    config = read_yaml(config_path)
    return config.get("thresholds")


def cmd_train(args: argparse.Namespace) -> None:
    config = read_yaml(args.config) if args.config else {}
    model.train(args.races, args.out, config=config)


def cmd_predict(args: argparse.Namespace) -> None:
    thresholds = _parse_thresholds(args.config)
    predictions = model.predict(
        args.cards,
        args.model,
        out_path=args.out,
        ct_method=args.ct,
        mc_iters=args.mc_iters,
        thresholds=thresholds,
    )
    logger.info("予測件数: %d", len(predictions))


def cmd_backtest(args: argparse.Namespace) -> None:
    summary = model.backtest(
        args.races,
        args.model,
        budget=args.budget,
        bet_policy=args.bet_policy,
        zone_filter=args.zone_filter,
    )
    logger.info("バックテスト結果: %s", json.dumps(summary, ensure_ascii=False))


def cmd_fetch(args: argparse.Namespace) -> None:
    race_ids: List[str]
    if args.race_ids:
        race_ids = [rid.strip() for rid in args.race_ids.split(",") if rid.strip()]
    elif args.date:
        race_ids = list_race_ids_for_date(args.date, providers=args.providers)
    else:
        raise SystemExit("--race-ids または --date を指定してください")

    info_df, entry_df, payout_df = race_data_scrape(
        race_ids,
        rate_limit=args.rate_limit,
        retries=args.retries,
        timeout=args.timeout,
    )
    out_dir = ensure_directory(args.out)
    to_training_csv(entry_df, info_df, payout_df, str(out_dir / "races.csv"))
    to_cards_csv(entry_df, str(out_dir / "cards.csv"))


def cmd_today(args: argparse.Namespace) -> None:
    resolver = TimeResolver()
    date_str = args.date or resolver.today_str()
    race_ids = list_race_ids_for_date(date_str, providers=args.providers)
    if not race_ids:
        logger.warning("対象レースが見つかりませんでした")
        return

    info_df, entry_df, payout_df = race_data_scrape(
        race_ids,
        rate_limit=args.rate_limit,
        retries=args.retries,
        timeout=args.timeout,
    )
    cards_path = Path(args.tmp_dir) / "cards.csv"
    ensure_directory(cards_path.parent)
    to_cards_csv(entry_df, str(cards_path))

    thresholds = _parse_thresholds(args.config)
    preds = model.predict(
        cards_path,
        args.model,
        ct_method=args.ct,
        mc_iters=args.mc_iters,
        thresholds=thresholds,
    )
    bet_table = bets.suggest_bets(
        preds,
        model.config_thresholds(thresholds),
        budget=args.budget,
        bet_policy=args.bet_policy,
        max_points=args.max_points,
    )
    out_path = Path(args.out) / f"today_bets_{date_str}.csv"
    ensure_directory(out_path.parent)
    if bet_table:
        headers = list(bet_table[0].keys())
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(",".join(headers) + "\n")
            for row in bet_table:
                f.write(",".join(str(row.get(h, "")) for h in headers) + "\n")
        summary_rows = [
            {k: row[k] for k in ["race_id", "zone", "A", "B", "C", "n_tickets", "budget"] if k in row}
            for row in bet_table
        ]
        for row in summary_rows:
            print(row)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Keirin prediction pipeline")
    subparsers = parser.add_subparsers(dest="command")

    train_parser = subparsers.add_parser("train", help="学習を実行")
    train_parser.add_argument("--races", required=True, help="学習用CSV")
    train_parser.add_argument("--out", required=True, help="モデル出力ディレクトリ")
    train_parser.add_argument("--config", help="設定ファイル(YAML)")
    train_parser.set_defaults(func=cmd_train)

    predict_parser = subparsers.add_parser("predict", help="推論を実行")
    predict_parser.add_argument("--cards", required=True, help="出走表CSV")
    predict_parser.add_argument("--model", required=True, help="モデルディレクトリ")
    predict_parser.add_argument("--out", help="予測出力パス", default="predictions.csv")
    predict_parser.add_argument("--ct", choices=["independent", "mc"], default="independent")
    predict_parser.add_argument("--mc-iters", type=int, default=2000, dest="mc_iters")
    predict_parser.add_argument("--config", help="閾値設定ファイル")
    predict_parser.set_defaults(func=cmd_predict)

    backtest_parser = subparsers.add_parser("backtest", help="バックテスト")
    backtest_parser.add_argument("--races", required=True)
    backtest_parser.add_argument("--model", required=True)
    backtest_parser.add_argument("--budget", type=int, default=10000)
    backtest_parser.add_argument("--bet-policy", default="flat")
    backtest_parser.add_argument("--zone-filter", default="any")
    backtest_parser.set_defaults(func=cmd_backtest)

    fetch_parser = subparsers.add_parser("fetch", help="スクレイピング")
    fetch_parser.add_argument("--race-ids", help="カンマ区切りのrace_id")
    fetch_parser.add_argument("--date", help="開催日")
    fetch_parser.add_argument("--out", required=True)
    fetch_parser.add_argument("--providers", default="chariloto,kdreams,keirin_jp")
    fetch_parser.add_argument("--rate-limit", type=float, default=1.0, dest="rate_limit")
    fetch_parser.add_argument("--retries", type=int, default=3)
    fetch_parser.add_argument("--timeout", type=int, default=15)
    fetch_parser.set_defaults(func=cmd_fetch)

    today_parser = subparsers.add_parser("today", help="今日の買い目を出力")
    today_parser.add_argument("--date", help="対象日")
    today_parser.add_argument("--providers", default="chariloto,kdreams,keirin_jp")
    today_parser.add_argument("--model", required=True)
    today_parser.add_argument("--budget", type=int, default=10000)
    today_parser.add_argument("--bet-policy", default="flat")
    today_parser.add_argument("--max-points", type=int, dest="max_points")
    today_parser.add_argument("--ct", choices=["independent", "mc"], default="independent")
    today_parser.add_argument("--mc-iters", type=int, default=2000, dest="mc_iters")
    today_parser.add_argument("--config", help="閾値設定ファイル")
    today_parser.add_argument("--out", default="out")
    today_parser.add_argument("--tmp-dir", default="tmp")
    today_parser.add_argument("--rate-limit", type=float, default=1.0, dest="rate_limit")
    today_parser.add_argument("--retries", type=int, default=3)
    today_parser.add_argument("--timeout", type=int, default=15)
    today_parser.set_defaults(func=cmd_today)

    return parser


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":  # pragma: no cover
    main()

