"""Utilities for collecting NBA "tour date" performances.

The scraper walks NBA.com's public schedule pages to discover recent games and
parses each game's box score to surface extremely poor shooting performances
that qualify as "tour dates".
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent
DEFAULT_DB_PATH = REPO_ROOT / "web" / "tourdates.db"

NBA_BASE_URL = "https://www.nba.com"
NBA_SCHEDULE_URL = NBA_BASE_URL + "/games"
USER_AGENT = "tourdates-scraper/0.1 (+https://github.com/parth/tourdates)"

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

TEAM_NAME_TO_ABBR = {
    "Atlanta Hawks": "ATL",
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "LA Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
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


@dataclass(slots=True, frozen=True)
class GameMetadata:
    game_id: str
    url: str
    game_date: dt.date


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


def iter_games_for_date(session: requests.Session, target_date: dt.date) -> Iterator[GameMetadata]:
    url = f"{NBA_SCHEDULE_URL}?date={target_date.isoformat()}"
    logging.debug("Fetching schedule: %s", url)
    response = session.get(url, timeout=20)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    cards = soup.select("a[data-id='nba:games:main:game:card']")

    seen: set[str] = set()
    for card in cards:
        href = card.get("href")
        if not href:
            continue
        game_id = extract_game_id(href)
        if not game_id or game_id in seen:
            continue
        seen.add(game_id)
        full_url = urljoin(NBA_BASE_URL, href)
        yield GameMetadata(game_id=game_id, url=full_url, game_date=target_date)


def extract_game_id(href: str) -> str | None:
    trimmed = href.strip().strip("/")
    if not trimmed:
        return None
    parts = trimmed.split("-")
    candidate = parts[-1]
    return candidate if candidate.isdigit() else None


def scrape_game_box_score(
    session: requests.Session, game: GameMetadata, season: str
) -> Iterator[TourDatePerformance]:
    box_url = game.url.rstrip("/") + "/box-score"
    logging.debug("Fetching box score: %s", box_url)
    response = session.get(box_url, timeout=20)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    sections = soup.select("section.GameBoxscore_gbTableSection__zTOUg")

    teams: list[dict[str, object]] = []
    for section in sections:
        header = section.find("h2")
        if not header:
            continue
        team_name = header.get_text(strip=True)
        team_abbr = TEAM_NAME_TO_ABBR.get(team_name)
        if not team_abbr:
            logging.warning(
                "Unrecognised team name '%s' in game %s", team_name, game.game_id
            )
            continue

        players = list(parse_player_rows(section))
        if not players:
            continue

        teams.append({
            "team_name": team_name,
            "team_abbr": team_abbr,
            "players": players,
        })

    if len(teams) < 1:
        logging.debug("No team data extracted for game %s", game.game_id)
        return

    for idx, team in enumerate(teams):
        opponent_abbr = (
            teams[1 - idx]["team_abbr"] if len(teams) == 2 else "TBD"
        )
        for player in team["players"]:
            yield TourDatePerformance(
                season=season,
                player_name=player["player_name"],
                team_abbr=team["team_abbr"],
                opponent_abbr=opponent_abbr,
                game_id=game.game_id,
                game_date=game.game_date,
                fgm=player["fgm"],
                fga=player["fga"],
                fg_pct=player["fg_pct"],
            )


def parse_player_rows(section) -> Iterator[dict[str, object]]:
    tbody = section.find("tbody")
    if not tbody:
        return

    for row in tbody.find_all("tr"):
        if row.find("span", class_="GameBoxscoreTable_totals__tM8PG"):
            continue

        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        player_cell = cells[0]
        name_tag = player_cell.select_one(
            ".GameBoxscoreTablePlayer_gbpNameFull__cf_sn"
        )
        if not name_tag:
            continue

        player_name = name_tag.get_text(strip=True)
        fgm = parse_int(cells[2].get_text(strip=True))
        fga = parse_int(cells[3].get_text(strip=True))
        fg_pct = parse_percent(cells[4].get_text(strip=True))

        if fgm is None or fga is None or fg_pct is None:
            continue
        if fga == 0:
            continue

        yield {
            "player_name": player_name,
            "fgm": fgm,
            "fga": fga,
            "fg_pct": fg_pct,
        }


def parse_int(value: str) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_percent(value: str) -> float | None:
    cleaned = value.strip().replace("%", "")
    if not cleaned or cleaned in {"--", "-"}:
        return None
    try:
        numeric = float(cleaned)
    except ValueError:
        return None
    return numeric / 100 if numeric > 1 else numeric


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
    season: str,
    since: dt.date,
    until: dt.date,
    processed_game_ids: Iterable[str] | None = None,
) -> Iterable[TourDatePerformance]:
    """Scrape NBA.com box scores to discover qualifying tour dates."""

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    seen_ids = set(processed_game_ids or [])
    current = since

    while current <= until:
        try:
            games = list(iter_games_for_date(session, current))
        except requests.RequestException as exc:  # pragma: no cover - network guard
            logging.warning("Failed to load schedule for %s: %s", current, exc)
            current += dt.timedelta(days=1)
            continue

        logging.debug("%s: discovered %d games", current, len(games))

        for game in games:
            if game.game_id in seen_ids:
                logging.debug("Skipping already processed game %s", game.game_id)
                continue

            seen_ids.add(game.game_id)

            try:
                for perf in scrape_game_box_score(session, game, season):
                    yield perf
            except requests.RequestException as exc:  # pragma: no cover - network guard
                logging.warning("Failed to fetch box score for %s: %s", game.game_id, exc)
                continue

            # be polite
            time.sleep(0.5)

        current += dt.timedelta(days=1)


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
    processed_game_ids = load_existing_game_ids(conn, args.season)
    since = args.since or infer_default_since(conn, args.season)
    until = args.until or dt.date.today()

    logging.info(
        "Scanning for tour dates | season=%s | since=%s | until=%s | dry_run=%s",
        args.season,
        since,
        until,
        args.dry_run,
    )

    performances = list(
        fetch_box_scores_for_range(
            args.season,
            since,
            until,
            processed_game_ids=processed_game_ids,
        )
    )

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

