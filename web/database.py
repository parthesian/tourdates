from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable, Mapping

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "tourdates.db"
SCHEMA_PATH = BASE_DIR / "schema.sql"
SEED_PATH = BASE_DIR / "seed_data.json"


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_database(db_path: Path | None = None) -> None:
    path = db_path or DB_PATH
    if not path.exists():
        initialise_database(path)


def initialise_database(db_path: Path | None = None, *, seed: bool = True, force: bool = False) -> None:
    path = db_path or DB_PATH

    if path.exists():
        if not force:
            raise FileExistsError(f"Database already exists at {path}. Pass force=True to overwrite.")
        path.unlink()

    path.parent.mkdir(parents=True, exist_ok=True)

    with get_connection(path) as conn:
        apply_schema(conn)
        if seed:
            seed_records = load_seed_records()
            insert_seed_data(conn, seed_records)


def apply_schema(conn: sqlite3.Connection) -> None:
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript(schema_sql)


def load_seed_records() -> list[Mapping[str, object]]:
    if not SEED_PATH.exists():
        return []
    return json.loads(SEED_PATH.read_text(encoding="utf-8"))


def insert_seed_data(conn: sqlite3.Connection, records: Iterable[Mapping[str, object]]) -> None:
    if not records:
        return

    query = """
        INSERT INTO tour_dates (
            season, player_name, team_abbr, opponent_abbr,
            game_id, game_date, fgm, fga, fg_pct
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(season, game_id, player_name) DO NOTHING
    """

    payload = []
    for record in records:
        payload.append(
            (
                record["season"],
                record["player_name"],
                record["team_abbr"],
                record["opponent_abbr"],
                record["game_id"],
                record["game_date"],
                record["fgm"],
                record["fga"],
                record["fg_pct"],
            )
        )

    with conn:
        conn.executemany(query, payload)


__all__ = [
    "get_connection",
    "ensure_database",
    "initialise_database",
    "load_seed_records",
    "insert_seed_data",
]

