"""CLI for applying and reporting the additive producer-storage migration."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from .storage import (
    DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
    DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
    migrate_storage_schema,
    storage_schema_report,
)
from .storage_policy import same_file


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apply the additive producer-storage schema to an explicit DB copy",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument(
        "--wal-autocheckpoint-pages", type=int,
        default=DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
    )
    parser.add_argument(
        "--journal-size-limit-bytes", type=int,
        default=DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
    )
    args = parser.parse_args()
    if not args.database.is_file():
        parser.error("--database must name an existing SQLite file")
    if args.report is not None and same_file(args.database, args.report):
        parser.error("--report must not alias --database")

    db = sqlite3.connect(str(args.database))
    try:
        with db:
            migration = migrate_storage_schema(
                db,
                wal_autocheckpoint_pages=args.wal_autocheckpoint_pages,
                journal_size_limit_bytes=args.journal_size_limit_bytes,
            )
        report = {
            "migration": migration,
            "database": str(args.database),
            **storage_schema_report(db, str(args.database)),
        }
    finally:
        db.close()

    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(encoded, encoding="utf-8")
    print(encoded, end="")


if __name__ == "__main__":
    main()
