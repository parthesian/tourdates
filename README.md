# Tour Dates

Tour Dates is an NBA community project that tracks extremely poor shooting performances and frames them as tongue-in-cheek "tour date" announcements. A tour date is determined by a player's field goals made (FGM) and field goals attempted (FGA). For example, shooting 4-for-28 is celebrated as an "April 28" tour date. This repository contains the data collection plan, a Python scraping workspace, and a small Flask site backed by SQLite that surfaces the latest tour dates and visualises which calendar days are still unclaimed.

## Project Structure

- `python/` – Workspace for the data collection tooling.
  - `tour_scraper.py` – Skeleton script with helper utilities for discovering missing tour dates and normalising scraped box scores.
  - `requirements.txt` – Dependencies for the scraping environment.
- `web/` – Flask application that serves the public website.
  - `app.py` – Application entrypoint.
  - `database.py` – SQLite helpers (connection, schema management, seed data loader).
  - `schema.sql` – Declarative schema for the `tour_dates` table.
  - `seed_data.json` – Starter records for development.
  - `static/` – Stylesheets and other assets.
  - `templates/` – HTML templates.
  - `requirements.txt` – Web runtime dependencies.

## Scraping Plan (2025-26 Season Focus)

1. **Source discovery**
   - Use stats.nba.com box score endpoints (or an HTML fallback as in the linked tutorial) because they expose per-player FGM/FGA/FG% along with game metadata.
   - Respect rate limits by applying modest delays (1–2 seconds) between API calls.
2. **Database-aware backlog**
   - Read the SQLite database to collect existing `(season, game_id)` pairs and known tour date combinations `(fgm, fga)`.
   - Compute which calendar combinations are still missing within the 2025-26 season window (pre-season optional, regular season primary).
3. **Game schedule walk**
   - Start from the day after the most recent stored game date; iterate forward until the present day.
   - For each day, query the NBA scoreboard endpoint to grab game IDs, then request detailed box scores per game.
4. **Eligibility filtering**
   - Keep only rows where:
     - `fgm` ∈ [1, 12]
     - `fga` does not exceed the number of days in the corresponding month (31 for Jan/Mar/May/Jul/Aug/Oct/Dec, 30 for Apr/Jun/Sep/Nov, 28 for Feb unless a leap year)
     - `fgm < fga` and `fg% < 0.50`
     - Attempts stay within a realistic upper bound (<= 31) even for 31-day months.
   - Deduplicate by `(season, player_id, game_id)` to avoid double inserts when re-scraping.
5. **Persistence layer**
   - Normalise and insert qualifying records into `tour_dates` with fields: `player_name`, `team_abbr`, `opponent_abbr`, `game_date`, `fgm`, `fga`, `fg_pct`, `season`, `game_id`, `created_at` (UTC timestamp).
   - Use upserts keyed by `(season, game_id, player_name)` to stay idempotent.
6. **Operational touches**
   - Provide CLI options for `--since YYYY-MM-DD` and `--until YYYY-MM-DD` to backfill specific ranges.
   - Log summary statistics (games scanned, candidates found, inserts) for monitoring.

## Local Development

### Prerequisites

- Python 3.11+
- Node/npm not required (the site is server-rendered via Flask)

### Setup Steps

1. **Create and populate the virtual environments**
   ```bash
   cd web
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   python init_db.py  # creates tourdates.db with sample data
   flask --app app run --debug
   ```
2. **Scraper workspace** (no live scraping code yet)
   ```bash
   cd ../python
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   python tour_scraper.py --help
   ```

The Flask dev server will expose the site at `http://127.0.0.1:5000/`, including a "Newly Announced Tour Dates" table and a calendar view that highlights dates already achieved.

## Next Steps

- Implement the NBA data fetch layer using the plan above (stats.nba.com JSON endpoints recommended).
- Add automated tests for the calendar logic and the filtering rules.
- Replace the sample seed data with live entries from the 2025-26 season once scraping is wired up.