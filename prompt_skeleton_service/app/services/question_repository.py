from __future__ import annotations

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

    def _utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _json_default(self, value):
        if isinstance(value, BaseModel):
            return value.model_dump()
        raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")
