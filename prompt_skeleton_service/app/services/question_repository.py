from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from pydantic import BaseModel


class QuestionRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS generation_batches (
                    batch_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS question_items (
                    item_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL,
                    question_type TEXT NOT NULL,
                    business_subtype TEXT,
                    pattern_id TEXT,
                    review_status TEXT NOT NULL,
                    generation_status TEXT NOT NULL,
                    current_version_no INTEGER DEFAULT 1,
                    current_status TEXT DEFAULT 'draft',
                    latest_action TEXT,
                    latest_action_at TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS question_item_versions (
                    version_id TEXT PRIMARY KEY,
                    item_id TEXT NOT NULL,
                    version_no INTEGER NOT NULL,
                    parent_version_no INTEGER,
                    source_action TEXT NOT NULL,
                    target_difficulty TEXT,
                    material_id TEXT,
                    prompt_template_name TEXT,
                    prompt_template_version TEXT,
                    stem TEXT,
                    options_json TEXT NOT NULL,
                    answer TEXT,
                    analysis TEXT,
                    prompt_package_json TEXT NOT NULL,
                    prompt_render_snapshot_json TEXT,
                    raw_model_output_json TEXT,
                    parsed_structured_output_json TEXT,
                    parse_error TEXT,
                    validation_result_json TEXT NOT NULL,
                    evaluation_result_json TEXT,
                    runtime_snapshot_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, version_no)
                );

                CREATE TABLE IF NOT EXISTS question_review_actions (
                    action_id TEXT PRIMARY KEY,
                    item_id TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    from_version_no INTEGER,
                    to_version_no INTEGER,
                    result_status TEXT,
                    operator TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS question_usage_events (
                    event_id TEXT PRIMARY KEY,
                    item_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    operator TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS source_question_assets (
                    asset_id TEXT PRIMARY KEY,
                    source_hash TEXT NOT NULL UNIQUE,
                    source_type TEXT NOT NULL,
                    question_card_id TEXT,
                    question_type TEXT,
                    business_subtype TEXT,
                    pattern_id TEXT,
                    difficulty_target TEXT,
                    topic TEXT,
                    payload_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            self._apply_lightweight_migrations(conn)

    def _apply_lightweight_migrations(self, conn: sqlite3.Connection) -> None:
        table_columns = {
            "question_items": {
                "current_version_no": "ALTER TABLE question_items ADD COLUMN current_version_no INTEGER DEFAULT 1",
                "current_status": "ALTER TABLE question_items ADD COLUMN current_status TEXT DEFAULT 'draft'",
                "latest_action": "ALTER TABLE question_items ADD COLUMN latest_action TEXT",
                "latest_action_at": "ALTER TABLE question_items ADD COLUMN latest_action_at TEXT",
            },
            "question_review_actions": {
                "from_version_no": "ALTER TABLE question_review_actions ADD COLUMN from_version_no INTEGER",
                "to_version_no": "ALTER TABLE question_review_actions ADD COLUMN to_version_no INTEGER",
                "result_status": "ALTER TABLE question_review_actions ADD COLUMN result_status TEXT",
                "operator": "ALTER TABLE question_review_actions ADD COLUMN operator TEXT",
            },
            "question_item_versions": {
                "prompt_template_name": "ALTER TABLE question_item_versions ADD COLUMN prompt_template_name TEXT",
                "prompt_template_version": "ALTER TABLE question_item_versions ADD COLUMN prompt_template_version TEXT",
                "prompt_render_snapshot_json": "ALTER TABLE question_item_versions ADD COLUMN prompt_render_snapshot_json TEXT",
                "evaluation_result_json": "ALTER TABLE question_item_versions ADD COLUMN evaluation_result_json TEXT",
            },
        }
        for table_name, migrations in table_columns.items():
            existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
            for column_name, ddl in migrations.items():
                if column_name not in existing:
                    conn.execute(ddl)

    def save_batch(self, batch_id: str, payload: dict) -> None:
        now = self._utc_now()
        serialized = json.dumps(payload, ensure_ascii=False, default=self._json_default)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO generation_batches (batch_id, payload_json, created_at, updated_at)
                VALUES (?, ?, COALESCE((SELECT created_at FROM generation_batches WHERE batch_id = ?), ?), ?)
                """,
                (batch_id, serialized, batch_id, now, now),
            )

    def save_item(self, item: dict) -> None:
        now = self._utc_now()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT created_at FROM question_items WHERE item_id = ?",
                (item["item_id"],),
            ).fetchone()
            payload = self._normalize_item_payload(
                item,
                now=now,
                created_at=existing["created_at"] if existing else None,
            )
            serialized = json.dumps(payload, ensure_ascii=False, default=self._json_default)
            statuses = payload.get("statuses", {})
            conn.execute(
                """
                INSERT OR REPLACE INTO question_items (
                    item_id, batch_id, question_type, business_subtype, pattern_id,
                    review_status, generation_status, current_version_no, current_status,
                    latest_action, latest_action_at, payload_json, created_at, updated_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    COALESCE((SELECT created_at FROM question_items WHERE item_id = ?), ?),
                    ?
                )
                """,
                (
                    payload["item_id"],
                    payload["batch_id"],
                    payload["question_type"],
                    payload.get("business_subtype"),
                    payload.get("pattern_id"),
                    statuses.get("review_status", "draft"),
                    statuses.get("generation_status", "not_started"),
                    int(payload.get("current_version_no", 1)),
                    payload.get("current_status", "draft"),
                    payload.get("latest_action"),
                    payload.get("latest_action_at"),
                    serialized,
                    payload["item_id"],
                    payload.get("created_at") or now,
                    now,
                ),
            )

    def save_version(self, version: dict) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO question_item_versions (
                    version_id, item_id, version_no, parent_version_no, source_action,
                    target_difficulty, material_id, prompt_template_name, prompt_template_version,
                    stem, options_json, answer, analysis, prompt_package_json, prompt_render_snapshot_json,
                    raw_model_output_json, parsed_structured_output_json, parse_error, validation_result_json,
                    evaluation_result_json, runtime_snapshot_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    version["version_id"],
                    version["item_id"],
                    version["version_no"],
                    version.get("parent_version_no"),
                    version["source_action"],
                    version.get("target_difficulty"),
                    version.get("material_id"),
                    version.get("prompt_template_name"),
                    version.get("prompt_template_version"),
                    version.get("stem"),
                    json.dumps(version.get("options", {}), ensure_ascii=False, default=self._json_default),
                    version.get("answer"),
                    version.get("analysis"),
                    json.dumps(version.get("prompt_package", {}), ensure_ascii=False, default=self._json_default),
                    json.dumps(version.get("prompt_render_snapshot", {}), ensure_ascii=False, default=self._json_default),
                    json.dumps(version.get("raw_model_output"), ensure_ascii=False, default=self._json_default),
                    json.dumps(version.get("parsed_structured_output"), ensure_ascii=False, default=self._json_default),
                    version.get("parse_error"),
                    json.dumps(version.get("validation_result", {}), ensure_ascii=False, default=self._json_default),
                    json.dumps(version.get("evaluation_result", {}), ensure_ascii=False, default=self._json_default),
                    json.dumps(version.get("runtime_snapshot", {}), ensure_ascii=False, default=self._json_default),
                    version.get("created_at") or self._utc_now(),
                ),
            )

    def get_item(self, item_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute("SELECT payload_json FROM question_items WHERE item_id = ?", (item_id,)).fetchone()
        if row is None:
            return None
        return self._normalize_item_payload(json.loads(row["payload_json"]))

    def get_batch(self, batch_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload_json, created_at, updated_at FROM generation_batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
        if row is None:
            return None

        payload = json.loads(row["payload_json"])
        items = self.list_items(batch_id=batch_id, limit=500)
        stats = self._build_batch_stats(items)
        batch_meta = payload.get("batch_meta", {})
        return {
            "batch_id": batch_id,
            "requested_count": batch_meta.get("requested_count", 0),
            "effective_count": batch_meta.get("effective_count", 0),
            "question_type": batch_meta.get("question_type"),
            "business_subtype": batch_meta.get("business_subtype"),
            "difficulty_target": batch_meta.get("difficulty_target"),
            "item_count": len(items),
            "review_status_counts": self._build_status_counts(items, "review_status"),
            "generation_status_counts": self._build_status_counts(items, "generation_status"),
            "items": items,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            **stats,
        }

    def get_item_history(self, item_id: str) -> dict | None:
        item = self.get_item(item_id)
        if item is None:
            return None
        versions = self.list_versions(item_id)
        if not versions:
            versions = [self._build_legacy_version_from_item(item)]
        versions = self._attach_diff_summaries(versions)
        actions = self.list_review_actions(item_id=item_id, limit=500)
        current_version_no = int(item.get("current_version_no", 1))
        current_version = next((version for version in versions if version["version_no"] == current_version_no), None)
        return {
            "item": self._item_to_review_summary(item),
            "current_version_no": current_version_no,
            "current_version": current_version,
            "versions": versions,
            "review_actions": actions,
        }

    def list_versions(self, item_id: str) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM question_item_versions
                WHERE item_id = ?
                ORDER BY version_no DESC
                """,
                (item_id,),
            ).fetchall()
        return [self._version_row_to_dict(row) for row in rows]

    def list_items(
        self,
        *,
        review_status: str | None = None,
        generation_status: str | None = None,
        question_type: str | None = None,
        batch_id: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        clauses: list[str] = []
        params: list[object] = []
        if review_status:
            clauses.append("review_status = ?")
            params.append(review_status)
        if generation_status:
            clauses.append("generation_status = ?")
            params.append(generation_status)
        if question_type:
            clauses.append("question_type = ?")
            params.append(question_type)
        if batch_id:
            clauses.append("batch_id = ?")
            params.append(batch_id)

        sql = "SELECT payload_json, created_at, updated_at FROM question_items"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        return [self._item_row_to_summary(row) for row in rows]

    def list_review_items(
        self,
        *,
        status: str | None = None,
        question_type: str | None = None,
        business_subtype: str | None = None,
        batch_id: str | None = None,
        keyword: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[dict], int]:
        clauses: list[str] = []
        params: list[object] = []
        if status:
            clauses.append("current_status = ?")
            params.append(status)
        if question_type:
            clauses.append("question_type = ?")
            params.append(question_type)
        if business_subtype:
            clauses.append("business_subtype = ?")
            params.append(business_subtype)
        if batch_id:
            clauses.append("batch_id = ?")
            params.append(batch_id)

        sql = "SELECT payload_json, created_at, updated_at FROM question_items"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY updated_at DESC"

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        items = [self._item_to_review_summary(json.loads(row["payload_json"]), created_at=row["created_at"], updated_at=row["updated_at"]) for row in rows]
        if keyword:
            lowered = keyword.lower()
            items = [
                item
                for item in items
                if lowered in (item.get("stem_preview") or "").lower()
                or lowered in (item.get("material_preview") or "").lower()
            ]

        total = len(items)
        start = max(0, (page - 1) * page_size)
        end = start + page_size
        return items[start:end], total

    def list_batches(self, *, limit: int = 50) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT batch_id, payload_json, updated_at FROM generation_batches ORDER BY updated_at DESC LIMIT ?",
                (limit,),
            ).fetchall()

        items: list[dict] = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            batch_meta = payload.get("batch_meta", {})
            items.append(
                {
                    "batch_id": row["batch_id"],
                    "requested_count": batch_meta.get("requested_count", 0),
                    "effective_count": batch_meta.get("effective_count", 0),
                    "question_type": batch_meta.get("question_type"),
                    "business_subtype": batch_meta.get("business_subtype"),
                    "difficulty_target": batch_meta.get("difficulty_target"),
                    "item_count": len(payload.get("items", [])),
                    "updated_at": row["updated_at"],
                }
            )
        return items

    def list_review_batches(
        self,
        *,
        status: str | None = None,
        created_by: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[dict], int]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT batch_id, payload_json, created_at, updated_at FROM generation_batches ORDER BY updated_at DESC"
            ).fetchall()

        batches: list[dict] = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            batch_meta = payload.get("batch_meta", {})
            items, _ = self.list_review_items(batch_id=row["batch_id"], page=1, page_size=500)
            stats = self._build_batch_stats(items)
            batch = {
                "batch_id": row["batch_id"],
                "created_by": batch_meta.get("created_by"),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                **stats,
            }
            if created_by and batch.get("created_by") != created_by:
                continue
            if status and batch["batch_status"] != status:
                continue
            batches.append(batch)

        total = len(batches)
        start = max(0, (page - 1) * page_size)
        end = start + page_size
        return batches[start:end], total

    def list_review_actions(self, *, item_id: str, limit: int = 100) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT action_id, item_id, action_type, from_version_no, to_version_no, result_status, operator, payload_json, created_at
                FROM question_review_actions
                WHERE item_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (item_id, limit),
            ).fetchall()

        return [
            {
                "action_id": row["action_id"],
                "item_id": row["item_id"],
                "action_type": row["action_type"],
                "from_version_no": row["from_version_no"],
                "to_version_no": row["to_version_no"],
                "result_status": row["result_status"],
                "operator": row["operator"],
                "created_at": row["created_at"],
                "payload": json.loads(row["payload_json"]),
            }
            for row in rows
        ]

    def list_feedback_backtest_units(
        self,
        *,
        item_id: str | None = None,
        question_type: str | None = None,
        question_card_id: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            if item_id:
                rows = conn.execute(
                    """
                    SELECT action_id, item_id, action_type, payload_json, created_at
                    FROM question_review_actions
                    WHERE item_id = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (item_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT action_id, item_id, action_type, payload_json, created_at
                    FROM question_review_actions
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()

        units: list[dict[str, Any]] = []
        for row in rows:
            payload = json.loads(row["payload_json"] or "{}")
            unit = payload.get("feedback_backtest_unit") or {}
            if not isinstance(unit, dict) or not unit:
                continue
            if question_type and unit.get("question_type") != question_type:
                continue
            if question_card_id and unit.get("question_card_id") != question_card_id:
                continue
            units.append(
                {
                    "action_id": row["action_id"],
                    "item_id": row["item_id"],
                    "action_type": row["action_type"],
                    "created_at": row["created_at"],
                    **unit,
                }
            )
        return units

    @staticmethod
    def _rate(numerator: int, denominator: int) -> float | None:
        if denominator <= 0:
            return None
        return round(numerator / denominator, 4)

    @staticmethod
    def _normalize_outcome_bucket(unit: dict[str, Any]) -> str:
        if unit.get("accepted_as_is"):
            return "confirm"
        if unit.get("revised_then_kept"):
            return "modify"
        if unit.get("discarded"):
            return "discard"
        return "other"

    @staticmethod
    def _top_penalties(units: list[dict[str, Any]], *, limit: int = 5) -> list[dict[str, Any]]:
        penalty_counts: dict[str, int] = {}
        for unit in units:
            penalties = unit.get("key_penalties") or {}
            if not isinstance(penalties, dict):
                continue
            for key, value in penalties.items():
                try:
                    numeric_value = float(value or 0.0)
                except (TypeError, ValueError):
                    numeric_value = 0.0
                if numeric_value <= 0:
                    continue
                penalty_counts[str(key)] = penalty_counts.get(str(key), 0) + 1
        return [
            {"penalty": key, "count": count}
            for key, count in sorted(penalty_counts.items(), key=lambda entry: entry[1], reverse=True)[:limit]
        ]

    @staticmethod
    def _selection_state_distribution(units: list[dict[str, Any]]) -> dict[str, Any]:
        distribution: dict[str, Any] = {}
        for state in ("recommended", "hold", "weak_candidate"):
            state_units = [unit for unit in units if unit.get("selection_state") == state]
            outcome_counts = {"confirm": 0, "modify": 0, "discard": 0, "other": 0}
            for unit in state_units:
                bucket = QuestionRepository._normalize_outcome_bucket(unit)
                outcome_counts[bucket] = outcome_counts.get(bucket, 0) + 1
            total = len(state_units)
            distribution[state] = {
                "count": total,
                "confirm_count": outcome_counts["confirm"],
                "modify_count": outcome_counts["modify"],
                "discard_count": outcome_counts["discard"],
                "other_count": outcome_counts["other"],
                "confirm_rate": QuestionRepository._rate(outcome_counts["confirm"], total),
                "modify_rate": QuestionRepository._rate(outcome_counts["modify"], total),
                "discard_rate": QuestionRepository._rate(outcome_counts["discard"], total),
            }
        return distribution

    @staticmethod
    def _difficulty_band_distribution(units: list[dict[str, Any]]) -> dict[str, Any]:
        distribution: dict[str, Any] = {}
        for band in ("easy", "medium", "hard"):
            band_units = [unit for unit in units if str(unit.get("difficulty_band_hint") or "").lower() == band]
            outcome_counts = {"confirm": 0, "modify": 0, "discard": 0, "other": 0}
            for unit in band_units:
                bucket = QuestionRepository._normalize_outcome_bucket(unit)
                outcome_counts[bucket] = outcome_counts.get(bucket, 0) + 1
            total = len(band_units)
            distribution[band] = {
                "count": total,
                "confirm_count": outcome_counts["confirm"],
                "revised_then_kept_count": outcome_counts["modify"],
                "discard_count": outcome_counts["discard"],
                "other_count": outcome_counts["other"],
                "confirm_rate": QuestionRepository._rate(outcome_counts["confirm"], total),
                "revised_then_kept_rate": QuestionRepository._rate(outcome_counts["modify"], total),
                "discard_rate": QuestionRepository._rate(outcome_counts["discard"], total),
            }
        return distribution

    @staticmethod
    def _preference_profiles_seen(units: list[dict[str, Any]]) -> list[dict[str, Any]]:
        preference_profiles_seen: dict[str, int] = {}
        for unit in units:
            profile = unit.get("preference_profile") or {}
            if not isinstance(profile, dict):
                continue
            signature = json.dumps(profile, ensure_ascii=False, sort_keys=True)
            preference_profiles_seen[signature] = preference_profiles_seen.get(signature, 0) + 1
        return [
            {"profile": json.loads(signature), "count": count}
            for signature, count in sorted(preference_profiles_seen.items(), key=lambda entry: entry[1], reverse=True)
        ]

    def _summarize_backtest_units(
        self,
        units: list[dict[str, Any]],
        *,
        include_group_breakdowns: bool = True,
        include_samples: bool = True,
    ) -> dict[str, Any]:
        total = len(units)
        recommended_units = [unit for unit in units if unit.get("selection_state") == "recommended"]
        hold_units = [unit for unit in units if unit.get("selection_state") == "hold"]
        weak_units = [unit for unit in units if unit.get("selection_state") == "weak_candidate"]
        suggested_repair_units = [unit for unit in units if unit.get("repair_suggested")]
        needs_review_units = [unit for unit in units if unit.get("needs_review") is True]
        discarded_units = [unit for unit in units if unit.get("discarded")]

        summary: dict[str, Any] = {
            "total_units": total,
            "recommended_confirmed_rate": self._rate(
                sum(1 for unit in recommended_units if unit.get("accepted_as_is")),
                len(recommended_units),
            ),
            "hold_revised_then_kept_rate": self._rate(
                sum(1 for unit in hold_units if unit.get("revised_then_kept")),
                len(hold_units),
            ),
            "repair_suggested_taken_rate": self._rate(
                sum(1 for unit in suggested_repair_units if unit.get("repair_path_taken")),
                len(suggested_repair_units),
            ),
            "needs_review_false_positive_rate": self._rate(
                sum(1 for unit in needs_review_units if unit.get("accepted_as_is")),
                len(needs_review_units),
            ),
            "weak_candidate_discard_rate": self._rate(
                sum(1 for unit in weak_units if unit.get("discarded")),
                len(weak_units),
            ),
            "top_discard_penalties": self._top_penalties(discarded_units),
            "preference_profiles_seen": self._preference_profiles_seen(units),
        }
        if include_group_breakdowns:
            summary["selection_state_outcomes"] = self._selection_state_distribution(units)
            summary["question_type_breakdown"] = {
                question_type: self._summarize_backtest_units(
                    [unit for unit in units if unit.get("question_type") == question_type],
                    include_group_breakdowns=False,
                    include_samples=False,
                )
                for question_type in ("main_idea", "sentence_fill", "sentence_order")
                if any(unit.get("question_type") == question_type for unit in units)
            }
            summary["difficulty_band_outcomes"] = self._difficulty_band_distribution(units)
        if include_samples:
            summary["sample_units"] = units[:5]
        return summary

    def get_feedback_backtest_summary(
        self,
        *,
        question_type: str | None = None,
        question_card_id: str | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        units = self.list_feedback_backtest_units(
            item_id=None,
            question_type=question_type,
            question_card_id=question_card_id,
            limit=limit,
        )
        return self._summarize_backtest_units(units, include_group_breakdowns=True, include_samples=True)

    @staticmethod
    def _normalize_penalty_diagnosis(
        *,
        count: int,
        discard_rate: float | None,
        revised_rate: float | None,
        accepted_rate: float | None,
        false_positive_rate: float | None,
    ) -> tuple[str, str]:
        discard_rate = float(discard_rate or 0.0)
        revised_rate = float(revised_rate or 0.0)
        accepted_rate = float(accepted_rate or 0.0)
        false_positive_rate = float(false_positive_rate or 0.0)
        if count < 2:
            return "insufficient_data", "Need more reviewed samples before reading this penalty as a stable signal."
        if discard_rate >= 0.6 and revised_rate <= 0.25:
            return "likely_high_signal", "This penalty co-occurs with discard often and rarely survives repair."
        if revised_rate >= 0.45:
            return "common_repairable_risk", "This penalty shows up frequently in candidates that are revised then kept."
        if accepted_rate >= 0.35 or false_positive_rate >= 0.3:
            return "possible_over_penalization", "This penalty often appears on candidates that are still accepted as-is."
        return "mixed_signal", "This penalty currently mixes discard and keep outcomes without a clear direction."

    def get_penalty_diagnostics(
        self,
        *,
        question_type: str | None = None,
        question_card_id: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        units = self.list_feedback_backtest_units(
            item_id=None,
            question_type=question_type,
            question_card_id=question_card_id,
            limit=limit,
        )
        diagnostics: dict[str, dict[str, Any]] = {}
        for unit in units:
            penalties = unit.get("key_penalties") or {}
            if not isinstance(penalties, dict):
                continue
            for penalty_name, raw_value in penalties.items():
                try:
                    numeric_value = float(raw_value or 0.0)
                except (TypeError, ValueError):
                    numeric_value = 0.0
                if numeric_value <= 0:
                    continue
                entry = diagnostics.setdefault(
                    str(penalty_name),
                    {
                        "penalty": str(penalty_name),
                        "count": 0,
                        "discard_count": 0,
                        "revised_then_kept_count": 0,
                        "accepted_as_is_count": 0,
                        "needs_review_false_positive_count": 0,
                        "question_types": set(),
                        "question_card_ids": set(),
                    },
                )
                entry["count"] += 1
                entry["question_types"].add(str(unit.get("question_type") or "unknown"))
                if unit.get("question_card_id"):
                    entry["question_card_ids"].add(str(unit.get("question_card_id")))
                if unit.get("discarded"):
                    entry["discard_count"] += 1
                if unit.get("revised_then_kept"):
                    entry["revised_then_kept_count"] += 1
                if unit.get("accepted_as_is"):
                    entry["accepted_as_is_count"] += 1
                if unit.get("needs_review") and unit.get("accepted_as_is"):
                    entry["needs_review_false_positive_count"] += 1

        results: list[dict[str, Any]] = []
        for entry in diagnostics.values():
            count = int(entry["count"])
            discard_rate = self._rate(int(entry["discard_count"]), count)
            revised_rate = self._rate(int(entry["revised_then_kept_count"]), count)
            accepted_rate = self._rate(int(entry["accepted_as_is_count"]), count)
            false_positive_rate = self._rate(int(entry["needs_review_false_positive_count"]), count)
            diagnosis, hint = self._normalize_penalty_diagnosis(
                count=count,
                discard_rate=discard_rate,
                revised_rate=revised_rate,
                accepted_rate=accepted_rate,
                false_positive_rate=false_positive_rate,
            )
            results.append(
                {
                    "penalty": entry["penalty"],
                    "count": count,
                    "discard_count": int(entry["discard_count"]),
                    "revised_then_kept_count": int(entry["revised_then_kept_count"]),
                    "accepted_as_is_count": int(entry["accepted_as_is_count"]),
                    "needs_review_false_positive_count": int(entry["needs_review_false_positive_count"]),
                    "discard_rate": discard_rate,
                    "revised_then_kept_rate": revised_rate,
                    "accepted_as_is_rate": accepted_rate,
                    "needs_review_false_positive_rate": false_positive_rate,
                    "question_types": sorted(entry["question_types"]),
                    "question_card_ids": sorted(entry["question_card_ids"]),
                    "diagnosis": diagnosis,
                    "hint": hint,
                }
            )
        return sorted(results, key=lambda entry: (entry["count"], entry["discard_count"]), reverse=True)

    @staticmethod
    def _normalize_threshold_diagnosis(
        *,
        failed_sample_count: int,
        kept_rate: float | None,
        discard_rate: float | None,
    ) -> tuple[str, str]:
        kept_rate = float(kept_rate or 0.0)
        discard_rate = float(discard_rate or 0.0)
        if failed_sample_count < 2:
            return "insufficient_data", "Need more threshold-hit samples before drawing a calibration conclusion."
        if kept_rate >= 0.5:
            return "likely_too_strict", "Many samples blocked by this threshold were later kept after review or repair."
        if discard_rate >= 0.6:
            return "likely_reasonable", "Most samples blocked by this threshold were later discarded."
        return "mixed_signal", "This threshold has mixed outcomes and should be watched, not auto-adjusted."

    def get_threshold_diagnostics(
        self,
        *,
        question_type: str | None = None,
        question_card_id: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        units = self.list_feedback_backtest_units(
            item_id=None,
            question_type=question_type,
            question_card_id=question_card_id,
            limit=limit,
        )
        diagnostics: dict[str, dict[str, Any]] = {}
        for unit in units:
            threshold_failures = unit.get("threshold_failures") or []
            if not isinstance(threshold_failures, list):
                continue
            for failure in threshold_failures:
                if not isinstance(failure, dict):
                    continue
                threshold_name = str(failure.get("threshold_name") or failure.get("check_name") or "").strip()
                if not threshold_name:
                    continue
                entry = diagnostics.setdefault(
                    threshold_name,
                    {
                        "threshold_name": threshold_name,
                        "failed_sample_count": 0,
                        "discard_count": 0,
                        "revised_then_kept_count": 0,
                        "accepted_as_is_count": 0,
                        "question_types": set(),
                        "question_card_ids": set(),
                        "sources": set(),
                        "sample_failures": [],
                    },
                )
                entry["failed_sample_count"] += 1
                entry["question_types"].add(str(unit.get("question_type") or "unknown"))
                if unit.get("question_card_id"):
                    entry["question_card_ids"].add(str(unit.get("question_card_id")))
                if failure.get("source"):
                    entry["sources"].add(str(failure.get("source")))
                if len(entry["sample_failures"]) < 5:
                    entry["sample_failures"].append(
                        {
                            "item_id": unit.get("item_id"),
                            "selection_state": unit.get("selection_state"),
                            "review_action": unit.get("review_action"),
                            "actual": failure.get("actual"),
                            "threshold": failure.get("threshold"),
                            "allowed_range": failure.get("allowed_range"),
                            "difficulty_band": failure.get("difficulty_band"),
                        }
                    )
                if unit.get("discarded"):
                    entry["discard_count"] += 1
                if unit.get("revised_then_kept"):
                    entry["revised_then_kept_count"] += 1
                if unit.get("accepted_as_is"):
                    entry["accepted_as_is_count"] += 1

        results: list[dict[str, Any]] = []
        for entry in diagnostics.values():
            failed_sample_count = int(entry["failed_sample_count"])
            kept_count = int(entry["accepted_as_is_count"]) + int(entry["revised_then_kept_count"])
            kept_rate = self._rate(kept_count, failed_sample_count)
            discard_rate = self._rate(int(entry["discard_count"]), failed_sample_count)
            diagnosis, hint = self._normalize_threshold_diagnosis(
                failed_sample_count=failed_sample_count,
                kept_rate=kept_rate,
                discard_rate=discard_rate,
            )
            results.append(
                {
                    "threshold_name": entry["threshold_name"],
                    "failed_sample_count": failed_sample_count,
                    "discard_count": int(entry["discard_count"]),
                    "revised_then_kept_count": int(entry["revised_then_kept_count"]),
                    "accepted_as_is_count": int(entry["accepted_as_is_count"]),
                    "discard_rate": discard_rate,
                    "revised_then_kept_rate": self._rate(int(entry["revised_then_kept_count"]), failed_sample_count),
                    "accepted_as_is_rate": self._rate(int(entry["accepted_as_is_count"]), failed_sample_count),
                    "question_types": sorted(entry["question_types"]),
                    "question_card_ids": sorted(entry["question_card_ids"]),
                    "sources": sorted(entry["sources"]),
                    "diagnosis": diagnosis,
                    "hint": hint,
                    "sample_failures": entry["sample_failures"],
                }
            )
        return sorted(results, key=lambda entry: entry["failed_sample_count"], reverse=True)

    @staticmethod
    def _comparison_candidate_id(candidate: dict[str, Any], fallback_index: int) -> str:
        for key in ("candidate_id", "material_id", "item_id", "id", "label", "name"):
            value = candidate.get(key)
            if value not in (None, ""):
                return str(value)
        return f"candidate_{fallback_index}"

    @staticmethod
    def _is_boundary_candidate(candidate: dict[str, Any]) -> bool:
        selection_state = str(candidate.get("selection_state") or "")
        if selection_state == "hold":
            return True
        try:
            final_score = float(candidate.get("final_candidate_score") or 0.0)
        except (TypeError, ValueError):
            final_score = 0.0
        return selection_state != "weak_candidate" and 0.3 <= final_score <= 0.7

    def compare_preference_effect(
        self,
        *,
        neutral_candidates: list[dict[str, Any]],
        preference_candidates: list[dict[str, Any]],
        preference_profile: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        neutral_index: dict[str, tuple[int, dict[str, Any]]] = {
            self._comparison_candidate_id(candidate, idx): (idx, candidate)
            for idx, candidate in enumerate(neutral_candidates)
        }
        preference_index: dict[str, tuple[int, dict[str, Any]]] = {
            self._comparison_candidate_id(candidate, idx): (idx, candidate)
            for idx, candidate in enumerate(preference_candidates)
        }
        common_ids = [candidate_id for candidate_id in neutral_index.keys() if candidate_id in preference_index]

        reordered_candidates: list[dict[str, Any]] = []
        selection_state_changes: list[dict[str, Any]] = []
        unsafe_weak_promotion = False
        boundary_only_shift = True

        for candidate_id in common_ids:
            neutral_rank, neutral_candidate = neutral_index[candidate_id]
            preference_rank, preference_candidate = preference_index[candidate_id]
            neutral_state = str(neutral_candidate.get("selection_state") or "")
            preference_state = str(preference_candidate.get("selection_state") or "")
            if neutral_state != preference_state:
                selection_state_changes.append(
                    {
                        "candidate_id": candidate_id,
                        "from_state": neutral_state,
                        "to_state": preference_state,
                    }
                )
            if neutral_rank != preference_rank:
                reordered_candidates.append(
                    {
                        "candidate_id": candidate_id,
                        "from_rank": neutral_rank,
                        "to_rank": preference_rank,
                        "rank_shift": neutral_rank - preference_rank,
                        "selection_state": preference_state or neutral_state,
                    }
                )
                if not self._is_boundary_candidate(neutral_candidate) and not self._is_boundary_candidate(preference_candidate):
                    boundary_only_shift = False
            if neutral_state == "weak_candidate" and preference_state != "weak_candidate":
                unsafe_weak_promotion = True

        nonweak_neutral_order = [
            candidate_id
            for candidate_id in common_ids
            if str(neutral_index[candidate_id][1].get("selection_state") or "") != "weak_candidate"
        ]
        for candidate_id in common_ids:
            neutral_rank, neutral_candidate = neutral_index[candidate_id]
            preference_rank, preference_candidate = preference_index[candidate_id]
            if str(neutral_candidate.get("selection_state") or "") != "weak_candidate":
                continue
            for other_id in nonweak_neutral_order:
                other_neutral_rank, _ = neutral_index[other_id]
                other_preference_rank, _ = preference_index[other_id]
                if neutral_rank > other_neutral_rank and preference_rank < other_preference_rank:
                    unsafe_weak_promotion = True
                    break
            if unsafe_weak_promotion:
                break

        if unsafe_weak_promotion:
            diagnosis = "unsafe_shift"
            hint = "Preference changed ordering in a way that promotes weak candidates beyond safe boundary use."
        elif reordered_candidates and not selection_state_changes and boundary_only_shift:
            diagnosis = "safe_preference_shift"
            hint = "Preference only adjusted boundary ordering and kept selection states stable."
        elif reordered_candidates or selection_state_changes:
            diagnosis = "borderline_shift"
            hint = "Preference changed ordering or state near the boundary, but no weak promotion was detected."
        else:
            diagnosis = "no_material_change"
            hint = "Preference did not materially change ranking or behavior on this sample set."

        return {
            "preference_profile": preference_profile or {},
            "candidate_count": len(common_ids),
            "reordered_candidates": sorted(reordered_candidates, key=lambda entry: abs(int(entry["rank_shift"])), reverse=True),
            "selection_state_changes": selection_state_changes,
            "boundary_only_shift": boundary_only_shift,
            "unsafe_weak_promotion": unsafe_weak_promotion,
            "diagnosis": diagnosis,
            "hint": hint,
            "neutral_top_order": common_ids[: min(5, len(common_ids))],
            "preference_top_order": [
                self._comparison_candidate_id(candidate, idx) for idx, candidate in enumerate(preference_candidates[:5])
            ],
        }

    def get_backtest_calibration_recommendations(
        self,
        *,
        question_type: str | None = None,
        question_card_id: str | None = None,
        limit: int = 500,
        preference_effect: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        summary = self.get_feedback_backtest_summary(
            question_type=question_type,
            question_card_id=question_card_id,
            limit=limit,
        )
        penalty_diagnostics = self.get_penalty_diagnostics(
            question_type=question_type,
            question_card_id=question_card_id,
            limit=limit,
        )
        threshold_diagnostics = self.get_threshold_diagnostics(
            question_type=question_type,
            question_card_id=question_card_id,
            limit=limit,
        )

        threshold_recommendations = [
            {
                "type": "threshold",
                "threshold_name": diagnostic["threshold_name"],
                "diagnosis": diagnostic["diagnosis"],
                "suggestion": diagnostic["hint"],
            }
            for diagnostic in threshold_diagnostics
            if diagnostic["diagnosis"] in {"likely_too_strict", "likely_reasonable"}
        ]
        penalty_recommendations = [
            {
                "type": "penalty",
                "penalty": diagnostic["penalty"],
                "diagnosis": diagnostic["diagnosis"],
                "suggestion": diagnostic["hint"],
            }
            for diagnostic in penalty_diagnostics
            if diagnostic["diagnosis"] in {"likely_high_signal", "common_repairable_risk", "possible_over_penalization"}
        ]

        preference_recommendations: list[dict[str, Any]] = []
        if preference_effect:
            diagnosis = str(preference_effect.get("diagnosis") or "")
            if diagnosis == "safe_preference_shift":
                preference_recommendations.append(
                    {
                        "type": "preference",
                        "diagnosis": diagnosis,
                        "suggestion": "Current preference profile appears to improve boundary ordering without unsafe promotion.",
                    }
                )
            elif diagnosis == "borderline_shift":
                preference_recommendations.append(
                    {
                        "type": "preference",
                        "diagnosis": diagnosis,
                        "suggestion": "Preference changed boundary behavior; watch hold/repair sensitivity before broadening it.",
                    }
                )
            elif diagnosis == "unsafe_shift":
                preference_recommendations.append(
                    {
                        "type": "preference",
                        "diagnosis": diagnosis,
                        "suggestion": "Preference is too aggressive and risks pulling weak candidates upward.",
                    }
                )

        return {
            "summary_reference": {
                "total_units": summary.get("total_units"),
                "recommended_confirmed_rate": summary.get("recommended_confirmed_rate"),
                "hold_revised_then_kept_rate": summary.get("hold_revised_then_kept_rate"),
                "repair_suggested_taken_rate": summary.get("repair_suggested_taken_rate"),
                "needs_review_false_positive_rate": summary.get("needs_review_false_positive_rate"),
                "weak_candidate_discard_rate": summary.get("weak_candidate_discard_rate"),
            },
            "threshold_recommendations": threshold_recommendations,
            "penalty_recommendations": penalty_recommendations,
            "preference_recommendations": preference_recommendations,
        }

    def get_version_pair_diff(self, item_id: str, from_version: int, to_version: int) -> dict | None:
        versions = self.list_versions(item_id)
        by_no = {version["version_no"]: version for version in versions}
        old = by_no.get(from_version)
        new = by_no.get(to_version)
        if old is None or new is None:
            return None
        return self._build_diff_response(item_id, old, new)

    def get_material_usage_stats(self, material_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS usage_count, MAX(created_at) AS last_used_at
                FROM question_item_versions
                WHERE material_id = ?
                """,
                (material_id,),
            ).fetchone()
        usage_count = int(row["usage_count"] or 0) if row else 0
        return {
            "usage_count_before": usage_count,
            "previously_used": usage_count > 0,
            "last_used_at": row["last_used_at"] if row else None,
        }

    def save_review_action(
        self,
        action_id: str,
        item_id: str,
        action_type: str,
        payload: dict,
        *,
        from_version_no: int | None = None,
        to_version_no: int | None = None,
        result_status: str | None = None,
        operator: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO question_review_actions (
                    action_id, item_id, action_type, from_version_no, to_version_no,
                    result_status, operator, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    action_id,
                    item_id,
                    action_type,
                    from_version_no,
                    to_version_no,
                    result_status,
                    operator,
                    json.dumps(payload, ensure_ascii=False, default=self._json_default),
                    self._utc_now(),
                ),
            )

    def save_usage_event(
        self,
        event_id: str,
        item_id: str,
        event_type: str,
        payload: dict,
        *,
        operator: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO question_usage_events (
                    event_id, item_id, event_type, operator, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    item_id,
                    event_type,
                    operator,
                    json.dumps(payload, ensure_ascii=False, default=self._json_default),
                    self._utc_now(),
                ),
            )

    def list_usage_events(self, *, item_id: str, limit: int = 100, event_type: str | None = None) -> list[dict]:
        sql = """
            SELECT event_id, item_id, event_type, operator, payload_json, created_at
            FROM question_usage_events
            WHERE item_id = ?
        """
        params: list[object] = [item_id]
        if event_type:
            sql += " AND event_type = ?"
            params.append(event_type)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "item_id": row["item_id"],
                "event_type": row["event_type"],
                "operator": row["operator"],
                "created_at": row["created_at"],
                "payload": json.loads(row["payload_json"] or "{}"),
            }
            for row in rows
        ]

    def upsert_source_question_asset(
        self,
        *,
        asset_id: str,
        source_type: str,
        payload: dict,
        metadata: dict,
        question_card_id: str | None = None,
        question_type: str | None = None,
        business_subtype: str | None = None,
        pattern_id: str | None = None,
        difficulty_target: str | None = None,
        topic: str | None = None,
    ) -> dict[str, Any]:
        now = self._utc_now()
        source_hash = self._source_question_hash(payload)
        serialized_payload = json.dumps(payload, ensure_ascii=False, default=self._json_default)
        serialized_metadata = json.dumps(metadata, ensure_ascii=False, default=self._json_default)
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT asset_id, created_at, metadata_json FROM source_question_assets WHERE source_hash = ?",
                (source_hash,),
            ).fetchone()
            resolved_asset_id = existing["asset_id"] if existing else asset_id
            created_at = existing["created_at"] if existing else now
            existing_metadata = json.loads(existing["metadata_json"] or "{}") if existing else {}
            merged_metadata = dict(existing_metadata if isinstance(existing_metadata, dict) else {})
            merged_metadata.update(metadata)
            merged_metadata["usage_count"] = int((existing_metadata or {}).get("usage_count") or 0) + int(metadata.get("usage_count") or 1)
            conn.execute(
                """
                INSERT OR REPLACE INTO source_question_assets (
                    asset_id, source_hash, source_type, question_card_id, question_type,
                    business_subtype, pattern_id, difficulty_target, topic, payload_json,
                    metadata_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    resolved_asset_id,
                    source_hash,
                    source_type,
                    question_card_id,
                    question_type,
                    business_subtype,
                    pattern_id,
                    difficulty_target,
                    topic,
                    serialized_payload,
                    json.dumps(merged_metadata, ensure_ascii=False, default=self._json_default),
                    created_at,
                    now,
                ),
            )
        return {
            "asset_id": resolved_asset_id,
            "source_hash": source_hash,
            "source_type": source_type,
            "question_card_id": question_card_id,
            "question_type": question_type,
            "business_subtype": business_subtype,
            "pattern_id": pattern_id,
            "difficulty_target": difficulty_target,
            "topic": topic,
            "payload": payload,
            "metadata": merged_metadata,
            "created_at": created_at,
            "updated_at": now,
        }

    def list_source_question_assets(
        self,
        *,
        limit: int = 100,
        source_type: str | None = None,
        question_card_id: str | None = None,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT asset_id, source_hash, source_type, question_card_id, question_type,
                   business_subtype, pattern_id, difficulty_target, topic, payload_json,
                   metadata_json, created_at, updated_at
            FROM source_question_assets
            WHERE 1 = 1
        """
        params: list[object] = []
        if source_type:
            sql += " AND source_type = ?"
            params.append(source_type)
        if question_card_id:
            sql += " AND question_card_id = ?"
            params.append(question_card_id)
        sql += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            metadata = json.loads(row["metadata_json"] or "{}")
            usage_count = int(metadata.get("usage_count") or 0) if isinstance(metadata, dict) else 0
            items.append(
                {
                    "asset_id": row["asset_id"],
                    "source_hash": row["source_hash"],
                    "source_type": row["source_type"],
                    "question_card_id": row["question_card_id"],
                    "question_type": row["question_type"],
                    "business_subtype": row["business_subtype"],
                    "pattern_id": row["pattern_id"],
                    "difficulty_target": row["difficulty_target"],
                    "topic": row["topic"],
                    "usage_count": usage_count,
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "payload": json.loads(row["payload_json"] or "{}"),
                    "metadata": metadata if isinstance(metadata, dict) else {},
                }
            )
        return items

    def get_review_metrics_summary(
        self,
        *,
        question_type: str | None = None,
        target_difficulty: str | None = None,
        document_genre: str | None = None,
        latest_action: str | None = None,
    ) -> dict[str, Any]:
        items, _ = self.list_review_items(page=1, page_size=5000)
        filtered = []
        for item in items:
            current = self.get_item(item["item_id"])
            if current is None:
                continue
            if question_type and item["question_type"] != question_type:
                continue
            if target_difficulty and current.get("difficulty_target") != target_difficulty:
                continue
            if document_genre and (current.get("material_selection") or {}).get("document_genre") != document_genre:
                continue
            if latest_action and current.get("latest_action") != latest_action:
                continue
            filtered.append(current)

        total_count = len(filtered)
        approved_count = sum(1 for item in filtered if item.get("current_status") == "approved")
        discarded_count = sum(1 for item in filtered if item.get("current_status") == "discarded")
        auto_failed_count = sum(1 for item in filtered if item.get("current_status") == "auto_failed")
        avg_review_rounds = (
            round(sum(int(item.get("revision_count", 0)) for item in filtered) / total_count, 2) if total_count else 0.0
        )
        actions = []
        for item in filtered:
            actions.extend(self.list_review_actions(item_id=item["item_id"], limit=200))
        action_success_rate = (
            round(
                sum(1 for action in actions if action.get("result_status") in {"approved", "pending_review"}) / len(actions),
                2,
            )
            if actions
            else 0.0
        )
        return {
            "total_count": total_count,
            "approved_count": approved_count,
            "discarded_count": discarded_count,
            "auto_failed_count": auto_failed_count,
            "avg_review_rounds": avg_review_rounds,
            "action_success_rate": action_success_rate,
        }

    def _item_row_to_summary(self, row: sqlite3.Row) -> dict:
        payload = self._normalize_item_payload(json.loads(row["payload_json"]), created_at=row["created_at"], updated_at=row["updated_at"])
        material_selection = payload.get("material_selection") or {}
        generated_question = payload.get("generated_question") or {}
        return {
            "item_id": payload["item_id"],
            "batch_id": payload["batch_id"],
            "question_type": payload["question_type"],
            "business_subtype": payload.get("business_subtype"),
            "pattern_id": payload.get("pattern_id"),
            "current_version_no": int(payload.get("current_version_no", 1)),
            "current_status": payload.get("current_status", "draft"),
            "latest_action": payload.get("latest_action"),
            "latest_action_at": payload.get("latest_action_at"),
            "review_status": payload.get("statuses", {}).get("review_status", "draft"),
            "generation_status": payload.get("statuses", {}).get("generation_status", "not_started"),
            "difficulty_target": payload.get("difficulty_target"),
            "revision_count": payload.get("revision_count", 0),
            "material_id": material_selection.get("material_id"),
            "document_genre": material_selection.get("document_genre"),
            "stem_preview": self._preview(generated_question.get("stem")),
            "material_preview": self._preview(material_selection.get("text")),
            "created_at": payload.get("created_at"),
            "updated_at": payload.get("updated_at") or row["updated_at"],
        }

    def _item_to_review_summary(
        self,
        item: dict,
        *,
        created_at: str | None = None,
        updated_at: str | None = None,
    ) -> dict:
        payload = self._normalize_item_payload(item, created_at=created_at, updated_at=updated_at)
        material_selection = payload.get("material_selection") or {}
        generated_question = payload.get("generated_question") or {}
        return {
            "item_id": payload["item_id"],
            "batch_id": payload["batch_id"],
            "question_type": payload["question_type"],
            "business_subtype": payload.get("business_subtype"),
            "target_difficulty": payload.get("difficulty_target"),
            "current_status": payload.get("current_status", "draft"),
            "current_version_no": int(payload.get("current_version_no", 1)),
            "latest_action": payload.get("latest_action"),
            "latest_action_at": payload.get("latest_action_at"),
            "created_at": payload.get("created_at"),
            "updated_at": payload.get("updated_at"),
            "stem_preview": self._preview(generated_question.get("stem")),
            "material_preview": self._preview(material_selection.get("text")),
        }

    def _version_row_to_dict(self, row: sqlite3.Row) -> dict:
        return {
            "version_id": row["version_id"],
            "item_id": row["item_id"],
            "version_no": row["version_no"],
            "parent_version_no": row["parent_version_no"],
            "source_action": row["source_action"],
            "current_status": self._infer_version_status(json.loads(row["validation_result_json"] or "{}")),
            "target_difficulty": row["target_difficulty"],
            "material_id": row["material_id"],
            "stem_preview": self._preview(row["stem"]),
            "answer": row["answer"],
            "validation_result": json.loads(row["validation_result_json"] or "{}"),
            "evaluation_result": json.loads(row["evaluation_result_json"] or "{}"),
            "prompt_template_name": row["prompt_template_name"],
            "prompt_template_version": row["prompt_template_version"],
            "runtime_snapshot": json.loads(row["runtime_snapshot_json"] or "{}"),
            "created_at": row["created_at"],
        }

    def _infer_version_status(self, validation_result: dict) -> str:
        if validation_result and not validation_result.get("passed", True):
            return "auto_failed"
        return "pending_review"

    def _build_batch_stats(self, items: list[dict]) -> dict[str, Any]:
        total_count = len(items)
        approved_count = sum(1 for item in items if item.get("current_status") == "approved")
        discarded_count = sum(1 for item in items if item.get("current_status") == "discarded")
        revising_count = sum(1 for item in items if item.get("current_status") == "revising")
        pending_count = sum(1 for item in items if item.get("current_status") in {"generated", "pending_review", "auto_failed"})
        if total_count and approved_count == total_count:
            batch_status = "approved"
        elif total_count and discarded_count == total_count:
            batch_status = "discarded"
        elif revising_count > 0:
            batch_status = "revising"
        else:
            batch_status = "pending_review"
        return {
            "batch_status": batch_status,
            "total_count": total_count,
            "pending_count": pending_count,
            "approved_count": approved_count,
            "discarded_count": discarded_count,
            "revising_count": revising_count,
        }

    def _build_status_counts(self, items: list[dict], field_name: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in items:
            key = str(item.get(field_name) or "unknown")
            counts[key] = counts.get(key, 0) + 1
        return counts

    def _build_legacy_version_from_item(self, item: dict) -> dict:
        generated_question = item.get("generated_question") or {}
        return {
            "version_id": f"{item['item_id']}:v1",
            "item_id": item["item_id"],
            "version_no": int(item.get("current_version_no", 1)),
            "parent_version_no": None,
            "source_action": item.get("latest_action") or "generate",
            "current_status": item.get("current_status", "draft"),
            "target_difficulty": item.get("difficulty_target"),
            "material_id": (item.get("material_selection") or {}).get("material_id"),
            "stem_preview": self._preview(generated_question.get("stem")),
            "answer": generated_question.get("answer"),
            "validation_result": item.get("validation_result") or {},
            "evaluation_result": item.get("evaluation_result") or {},
            "prompt_template_name": None,
            "prompt_template_version": None,
            "diff_summary": {},
            "runtime_snapshot": {},
            "created_at": item.get("created_at") or self._utc_now(),
        }

    def _attach_diff_summaries(self, versions: list[dict]) -> list[dict]:
        by_no = {version["version_no"]: version for version in versions}
        for version in versions:
            parent_no = version.get("parent_version_no")
            if parent_no is None or parent_no not in by_no:
                version["diff_summary"] = {}
                continue
            diff = self._build_diff_response(version["item_id"], by_no[parent_no], version)
            version["diff_summary"] = {
                "changed_fields": diff["changed_fields"],
                "material_changed": diff["material_changed"],
                "difficulty_changed": diff["difficulty_changed"],
                "prompt_changed": diff["prompt_changed"],
                "stem_changed": diff["stem_changed"],
                "options_changed": diff["options_changed"],
                "analysis_changed": diff["analysis_changed"],
            }
        return versions

    def _build_diff_response(self, item_id: str, old: dict, new: dict) -> dict:
        old_rt = old.get("runtime_snapshot", {})
        new_rt = new.get("runtime_snapshot", {})
        old_material = ((old_rt.get("material_snapshot") or {}).get("material_id")) or old.get("material_id")
        new_material = ((new_rt.get("material_snapshot") or {}).get("material_id")) or new.get("material_id")
        old_prompt = old_rt.get("prompt_snapshot") or {}
        new_prompt = new_rt.get("prompt_snapshot") or {}
        old_model = (old_rt.get("model_output_snapshot") or {}).get("parsed_structured_output") or {}
        new_model = (new_rt.get("model_output_snapshot") or {}).get("parsed_structured_output") or {}

        changed_fields: list[str] = []
        material_changed = old_material != new_material
        difficulty_changed = old.get("target_difficulty") != new.get("target_difficulty")
        prompt_changed = (
            old.get("prompt_template_name") != new.get("prompt_template_name")
            or old.get("prompt_template_version") != new.get("prompt_template_version")
            or old_prompt.get("selected_pattern") != new_prompt.get("selected_pattern")
        )
        stem_changed = old_model.get("stem") != new_model.get("stem")
        options_changed = old_model.get("options") != new_model.get("options")
        analysis_changed = old_model.get("analysis") != new_model.get("analysis")

        if material_changed:
            changed_fields.append("material")
        if difficulty_changed:
            changed_fields.append("difficulty")
        if prompt_changed:
            changed_fields.append("prompt")
        if stem_changed:
            changed_fields.append("stem")
        if options_changed:
            changed_fields.append("options")
        if analysis_changed:
            changed_fields.append("analysis")

        return {
            "item_id": item_id,
            "from_version": old["version_no"],
            "to_version": new["version_no"],
            "changed_fields": changed_fields,
            "material_changed": material_changed,
            "difficulty_changed": difficulty_changed,
            "prompt_changed": prompt_changed,
            "stem_changed": stem_changed,
            "options_changed": options_changed,
            "analysis_changed": analysis_changed,
            "old_summary": {
                "material_id": old_material,
                "target_difficulty": old.get("target_difficulty"),
                "stem_preview": old.get("stem_preview"),
                "prompt_template_name": old.get("prompt_template_name"),
                "prompt_template_version": old.get("prompt_template_version"),
            },
            "new_summary": {
                "material_id": new_material,
                "target_difficulty": new.get("target_difficulty"),
                "stem_preview": new.get("stem_preview"),
                "prompt_template_name": new.get("prompt_template_name"),
                "prompt_template_version": new.get("prompt_template_version"),
            },
        }

    def _normalize_item_payload(
        self,
        item: dict,
        *,
        now: str | None = None,
        created_at: str | None = None,
        updated_at: str | None = None,
    ) -> dict:
        payload = dict(item)
        generated_question = payload.get("generated_question") or {}
        material_selection = payload.get("material_selection") or {}
        payload.setdefault("current_version_no", 1)
        payload.setdefault("current_status", self._derive_current_status(payload))
        payload.setdefault("latest_action", "generate")
        payload.setdefault("latest_action_at", updated_at or now)
        payload.setdefault("stem_text", generated_question.get("stem"))
        payload.setdefault("material_text", material_selection.get("text"))
        payload.setdefault("material_source", material_selection.get("source") or {})
        payload.setdefault("material_usage_count_before", int(material_selection.get("usage_count_before") or 0))
        payload.setdefault("material_previously_used", bool(material_selection.get("previously_used", False)))
        payload.setdefault("material_last_used_at", material_selection.get("last_used_at"))
        if created_at is not None:
            payload["created_at"] = created_at
        else:
            payload.setdefault("created_at", payload.get("created_at") or now)
        if updated_at is not None:
            payload["updated_at"] = updated_at
        else:
            payload.setdefault("updated_at", now)
        payload.setdefault("revision_count", max(0, int(payload.get("revision_count", 0))))
        return payload

    def _derive_current_status(self, item: dict) -> str:
        if item.get("current_status"):
            return item["current_status"]
        statuses = item.get("statuses", {})
        validation_result = item.get("validation_result") or {}
        if statuses.get("review_status") == "approved":
            return "approved"
        if statuses.get("review_status") == "rejected":
            return "discarded"
        if validation_result and not validation_result.get("passed", True):
            return "auto_failed"
        if statuses.get("review_status") == "needs_revision":
            return "revising"
        if statuses.get("generation_status") == "success":
            return "pending_review"
        return "generated"

    def _preview(self, text: str | None, limit: int = 80) -> str | None:
        if not text:
            return None
        clean = text.replace("\n", " ").strip()
        return clean if len(clean) <= limit else clean[: limit - 3] + "..."

    @staticmethod
    def _source_question_hash(payload: dict[str, Any]) -> str:
        serialized = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def _utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _json_default(self, value):
        if isinstance(value, BaseModel):
            return value.model_dump()
        raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")
