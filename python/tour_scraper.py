"""Utilities for collecting NBA "tour date" performances.

This module currently focuses on orchestration and data hygiene. The actual
network scraping step (pulling box scores from stats.nba.com) is intentionally
left as a placeholder so that the project can be wired up incrementally without
requiring working API credentials or network access.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence

BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent
DEFAULT_DB_PATH = REPO_ROOT / "web" / "tourdates.db"

MONTH_DAY_LIMITS = {
    1: 31,
    2: 28,
    3: 31,
    4: 30,
    5: 31,
    6: 30,
    7: 31,
    8: 31,
    9: 30,
    10: 31,
    11: 30,
    12: 31,
}


@dataclass(slots=True, frozen=True)
class TourDatePerformance:
    season: str
    player_name: str
    team_abbr: str
    opponent_abbr: str
    game_id: str
    game_date: dt.date
    fgm: int
    fga: int
    fg_pct: float

    @property
    def month(self) -> int:
        return self.fgm

    @property
    def day(self) -> int:
        return self.fga

    @property
    def is_valid_tour_date(self) -> bool:
        return validate_tour_date(self.fgm, self.fga, self.fg_pct)


def validate_tour_date(fgm: int, fga: int, fg_pct: float) -> bool:
    """Validate a potential tour date using the business rules."""

    if not (1 <= fgm <= 12):
        return False

    month_max = MONTH_DAY_LIMITS.get(fgm)
    if month_max is None or fga > month_max:
        return False

    if fga <= fgm:
        return False

    if fga > 31:
        return False

    if fg_pct >= 0.50:
        return False

    return True


def get_connection(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def load_existing_game_ids(
    conn: sqlite3.Connection, season: str
) -> set[str]:
    query = "SELECT DISTINCT game_id FROM tour_dates WHERE season = ?"
    rows = conn.execute(query, (season,)).fetchall()
    return {row["game_id"] for row in rows}


def load_known_combinations(conn: sqlite3.Connection, season: str) -> set[tuple[int, int]]:
    query = "SELECT DISTINCT fgm, fga FROM tour_dates WHERE season = ?"
    rows = conn.execute(query, (season,)).fetchall()
    return {(row["fgm"], row["fga"]) for row in rows}


def compute_missing_calendar_slots(
    known: Iterable[tuple[int, int]]
) -> List[tuple[int, int]]:
    """Return sorted list of month/day pairs without entries."""

    missing: List[tuple[int, int]] = []
    for month, limit in MONTH_DAY_LIMITS.items():
        for day in range(1, limit + 1):
            if (month, day) not in known:
                missing.append((month, day))
    return missing


def serialise_performance(perf: TourDatePerformance) -> dict:
    return {
        "season": perf.season,
        "player_name": perf.player_name,
        "team_abbr": perf.team_abbr,
        "opponent_abbr": perf.opponent_abbr,
        "game_id": perf.game_id,
        "game_date": perf.game_date.isoformat(),
        "fgm": perf.fgm,
        "fga": perf.fga,
        "fg_pct": perf.fg_pct,
    }


def insert_performances(
    conn: sqlite3.Connection, performances: Sequence[TourDatePerformance]
) -> int:
    query = """
        INSERT INTO tour_dates (
            season, player_name, team_abbr, opponent_abbr,
            game_id, game_date, fgm, fga, fg_pct
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(season, game_id, player_name) DO UPDATE SET
            fgm = excluded.fgm,
            fga = excluded.fga,
            fg_pct = excluded.fg_pct
    """
    values = [
        (
            perf.season,
            perf.player_name,
            perf.team_abbr,
            perf.opponent_abbr,
            perf.game_id,
            perf.game_date.isoformat(),
            perf.fgm,
            perf.fga,
            perf.fg_pct,
        )
        for perf in performances
        if perf.is_valid_tour_date
    ]

    if not values:
        return 0

    with conn:
        conn.executemany(query, values)
    return len(values)


def fetch_box_scores_for_range(
    season: str, since: dt.date, until: dt.date
) -> Iterable[TourDatePerformance]:
    """Placeholder fetcher. Replace with stats.nba.com integration."""

    raise NotImplementedError(
        "Scraping is not yet implemented. Follow the README plan to add it."
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--season",
        default="2025-26",
        help="Season identifier to target (default: 2025-26)",
    )
    parser.add_argument(
        "--since",
        type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
        default=None,
        help="Inclusive start date (YYYY-MM-DD). Defaults to last stored date + 1.",
    )
    parser.add_argument(
        "--until",
        type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
        default=None,
        help="Inclusive end date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Collect and display candidates without writing to the database.",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to the SQLite database (defaults to web/tourdates.db).",
    )
    parser.add_argument(
        "--export-json",
        type=Path,
        help="Optional path to dump new performances as JSON.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args(argv)


def infer_default_since(conn: sqlite3.Connection, season: str) -> dt.date:
    row = conn.execute(
        "SELECT MAX(game_date) AS last_date FROM tour_dates WHERE season = ?",
        (season,),
    ).fetchone()
    last_date = dt.date.fromisoformat(row["last_date"]) if row["last_date"] else None
    return (last_date + dt.timedelta(days=1)) if last_date else dt.date(2025, 10, 1)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level))

    conn = get_connection(args.db_path)
    since = args.since or infer_default_since(conn, args.season)
    until = args.until or dt.date.today()

    logging.info(
        "Scanning for tour dates | season=%s | since=%s | until=%s | dry_run=%s",
        args.season,
        since,
        until,
        args.dry_run,
    )

    try:
        performances = list(fetch_box_scores_for_range(args.season, since, until))
    except NotImplementedError as exc:
        logging.error("%s", exc)
        logging.info(
            "Once scraping is implemented you can rerun this command to ingest new data."
        )
        return

    valid = [perf for perf in performances if perf.is_valid_tour_date]
    logging.info("Found %d candidate performances (%d valid)", len(performances), len(valid))

    if args.export_json:
        args.export_json.write_text(json.dumps([serialise_performance(p) for p in valid], indent=2))
        logging.info("Exported %d performances to %s", len(valid), args.export_json)

    if args.dry_run:
        for perf in valid:
            logging.info(
                "%s vs %s on %s: %s-%s (%.1f%%)",
                perf.player_name,
                perf.opponent_abbr,
                perf.game_date,
                perf.fgm,
                perf.fga,
                perf.fg_pct * 100,
            )
        return

    inserted = insert_performances(conn, valid)
    logging.info("Inserted %d new tour dates", inserted)


if __name__ == "__main__":
    main()

