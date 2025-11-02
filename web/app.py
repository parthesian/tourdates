from __future__ import annotations
from collections import defaultdict
from typing import Any, Dict, Iterable, List

from flask import Flask, g, render_template

from database import ensure_database, get_connection

app = Flask(__name__)

MONTH_NAMES = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}

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

SEASON = "2025-26"


@app.before_request
def connect_db() -> None:
    ensure_database()
    g.db = get_connection()


@app.teardown_request
def close_db(exception: Exception | None) -> None:  # noqa: ARG001 (Flask signature)
    db = g.pop("db", None)
    if db is not None:
        db.close()


def fetch_new_tour_dates(limit: int = 10) -> List[Dict[str, Any]]:
    query = """
        SELECT player_name, team_abbr, opponent_abbr, game_date, fgm, fga, fg_pct
        FROM tour_dates
        WHERE season = ?
        ORDER BY game_date DESC, player_name ASC
        LIMIT ?
    """
    rows = g.db.execute(query, (SEASON, limit)).fetchall()
    return [dict(row) for row in rows]


def fetch_calendar_rows() -> Iterable[Dict[str, Any]]:
    query = """
        SELECT player_name, team_abbr, opponent_abbr, game_date, fgm, fga, fg_pct
        FROM tour_dates
        WHERE season = ?
    """
    rows = g.db.execute(query, (SEASON,)).fetchall()
    for row in rows:
        yield dict(row)


def build_calendar(entries: Iterable[Dict[str, Any]]):
    grouped: dict[tuple[int, int], list[Dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        grouped[(entry["fgm"], entry["fga"])].append(entry)

    months = []
    for month in range(1, 13):
        limit = MONTH_DAY_LIMITS[month]
        days = []
        for day in range(1, limit + 1):
            key = (month, day)
            day_entries = grouped.get(key, [])
            days.append(
                {
                    "day": day,
                    "announced": bool(day_entries),
                    "entries": day_entries,
                }
            )
        months.append(
            {
                "month": month,
                "name": MONTH_NAMES[month],
                "days": days,
            }
        )
    return months


def format_percentage(value: float) -> str:
    return f"{value * 100:.1f}%"


@app.route("/")
def index() -> str:
    new_dates = fetch_new_tour_dates()
    calendar = build_calendar(fetch_calendar_rows())
    return render_template(
        "index.html",
        season=SEASON,
        new_dates=new_dates,
        calendar=calendar,
        format_percentage=format_percentage,
    )


if __name__ == "__main__":
    app.run(debug=True)

