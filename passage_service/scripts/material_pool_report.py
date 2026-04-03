from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.domain.services.pool_service import PoolService
from app.infra.db.session import get_session, init_db


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a local material-pool statistics snapshot.")
    parser.add_argument("--status", type=str, default=None, help="Optional material status filter.")
    parser.add_argument("--release-channel", type=str, default=None, help="Optional release channel filter.")
    parser.add_argument("--output", type=str, default=None, help="Optional output JSON path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    init_db()
    session = get_session()
    try:
        report = PoolService(session).get_pool_stats(
            status=args.status,
            release_channel=args.release_channel,
        )
    finally:
        session.close()

    output_path = Path(args.output) if args.output else (
        PROJECT_ROOT / "exports" / f"material_pool_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\nreport_saved_to={output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
