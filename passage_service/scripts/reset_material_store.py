from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import get_settings
from app.infra.db.session import init_db


RESET_TABLES = [
    "feedback_records",
    "feedback_aggregates",
    "tagging_reviews",
    "audit_events",
    "material_spans",
    "candidate_spans",
    "sentences",
    "paragraphs",
    "jobs",
    "articles",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Back up and clear the local passage/material store while preserving schema."
    )
    parser.add_argument(
        "--skip-backup",
        action="store_true",
        help="Do not create a database backup before clearing data.",
    )
    parser.add_argument(
        "--include-exports",
        action="store_true",
        help="Also remove generated export directories under passage_service/exports.",
    )
    return parser.parse_args()


def resolve_db_path() -> Path:
    db_url = get_settings().database_url
    prefix = "sqlite:///"
    if not db_url.startswith(prefix):
        raise RuntimeError(f"Unsupported database url for reset: {db_url}")
    raw_path = db_url[len(prefix) :]
    path = Path(raw_path)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def backup_database(db_path: Path) -> Path:
    backup_dir = PROJECT_ROOT / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"{db_path.stem}_backup_{timestamp}{db_path.suffix}"
    shutil.copy2(db_path, backup_path)
    return backup_path


def clear_tables(db_path: Path) -> dict[str, int]:
    deleted_counts: dict[str, int] = {}
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA foreign_keys=OFF")
        for table in RESET_TABLES:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            deleted_counts[table] = int(cur.fetchone()[0])
            cur.execute(f"DELETE FROM {table}")
        conn.commit()
        cur.execute("VACUUM")
        conn.commit()
        return deleted_counts
    finally:
        conn.close()


def clear_exports() -> list[str]:
    exports_dir = PROJECT_ROOT / "exports"
    removed: list[str] = []
    if not exports_dir.exists():
        return removed
    for child in exports_dir.iterdir():
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
            removed.append(str(child))
    return removed


def main() -> int:
    args = parse_args()
    db_path = resolve_db_path()
    if not db_path.exists():
        init_db()
        db_path = resolve_db_path()

    backup_path: Path | None = None
    if not args.skip_backup:
        backup_path = backup_database(db_path)

    deleted_counts = clear_tables(db_path)
    removed_exports = clear_exports() if args.include_exports else []

    # Re-run init to ensure schema exists after cleanup.
    init_db()

    print("Reset completed.")
    print(f"Database: {db_path}")
    if backup_path:
        print(f"Backup: {backup_path}")
    for table, count in deleted_counts.items():
        print(f"{table}: removed {count}")
    if removed_exports:
        print("Removed export directories:")
        for item in removed_exports:
            print(f"  - {item}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
