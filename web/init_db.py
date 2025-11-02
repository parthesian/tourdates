from __future__ import annotations

import argparse
from pathlib import Path

from database import initialise_database


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialise the tour dates SQLite database.")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="Optional path for the database file (defaults to web/tourdates.db).",
    )
    parser.add_argument(
        "--no-seed",
        dest="seed",
        action="store_false",
        help="Create an empty schema without inserting sample data.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite any existing database at the target path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    initialise_database(db_path=args.db_path, seed=args.seed, force=args.force)


if __name__ == "__main__":
    main()

