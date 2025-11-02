PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS tour_dates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    season TEXT NOT NULL,
    player_name TEXT NOT NULL,
    team_abbr TEXT NOT NULL,
    opponent_abbr TEXT NOT NULL,
    game_id TEXT NOT NULL,
    game_date TEXT NOT NULL,
    fgm INTEGER NOT NULL,
    fga INTEGER NOT NULL,
    fg_pct REAL NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (season, game_id, player_name)
);

CREATE INDEX IF NOT EXISTS idx_tour_dates_season_game_date
    ON tour_dates (season, game_date DESC);

