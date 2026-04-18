from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.infra.db.base import Base


settings = get_settings()


def _build_engine():
    connect_args = {"timeout": 30}
    engine_kwargs = {
        "future": True,
        "connect_args": connect_args,
        "pool_pre_ping": True,
        "pool_size": max(1, int(settings.db_pool_size)),
        "max_overflow": max(0, int(settings.db_max_overflow)),
        "pool_timeout": max(1.0, float(settings.db_pool_timeout_seconds)),
        "pool_recycle": max(1, int(settings.db_pool_recycle_seconds)),
        "pool_use_lifo": True,
    }
    if settings.resolved_database_url.startswith("sqlite:///"):
        connect_args["check_same_thread"] = False
    return create_engine(settings.resolved_database_url, **engine_kwargs)


engine = _build_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)


@event.listens_for(engine, "connect")
def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA busy_timeout=30000")
    cursor.close()


def init_db() -> None:
    import app.infra.db.orm.article  # noqa: F401
    import app.infra.db.orm.audit  # noqa: F401
    import app.infra.db.orm.candidate_span  # noqa: F401
    import app.infra.db.orm.feedback  # noqa: F401
    import app.infra.db.orm.job  # noqa: F401
    import app.infra.db.orm.material_span  # noqa: F401
    import app.infra.db.orm.paragraph  # noqa: F401
    import app.infra.db.orm.review  # noqa: F401
    import app.infra.db.orm.sentence  # noqa: F401

    Base.metadata.create_all(bind=engine)
    _apply_lightweight_migrations()


def get_session() -> Session:
    return SessionLocal()


def _apply_lightweight_migrations() -> None:
    migrations = {
        "material_spans": {
            "universal_profile": "ALTER TABLE material_spans ADD COLUMN universal_profile JSON DEFAULT '{}'",
            "family_scores": "ALTER TABLE material_spans ADD COLUMN family_scores JSON DEFAULT '{}'",
            "capability_scores": "ALTER TABLE material_spans ADD COLUMN capability_scores JSON DEFAULT '{}'",
            "parallel_families": "ALTER TABLE material_spans ADD COLUMN parallel_families JSON DEFAULT '[]'",
            "structure_features": "ALTER TABLE material_spans ADD COLUMN structure_features JSON DEFAULT '{}'",
            "family_profiles": "ALTER TABLE material_spans ADD COLUMN family_profiles JSON DEFAULT '{}'",
            "subtype_candidates": "ALTER TABLE material_spans ADD COLUMN subtype_candidates JSON DEFAULT '[]'",
            "secondary_candidates": "ALTER TABLE material_spans ADD COLUMN secondary_candidates JSON DEFAULT '[]'",
            "candidate_labels": "ALTER TABLE material_spans ADD COLUMN candidate_labels JSON DEFAULT '[]'",
            "primary_label": "ALTER TABLE material_spans ADD COLUMN primary_label VARCHAR",
            "decision_trace": "ALTER TABLE material_spans ADD COLUMN decision_trace JSON DEFAULT '{}'",
            "primary_route": "ALTER TABLE material_spans ADD COLUMN primary_route JSON DEFAULT '{}'",
            "reject_reason": "ALTER TABLE material_spans ADD COLUMN reject_reason VARCHAR",
            "normalized_text_hash": "ALTER TABLE material_spans ADD COLUMN normalized_text_hash VARCHAR",
            "material_family_id": "ALTER TABLE material_spans ADD COLUMN material_family_id VARCHAR",
            "is_primary": "ALTER TABLE material_spans ADD COLUMN is_primary BOOLEAN DEFAULT 1",
            "variants": "ALTER TABLE material_spans ADD COLUMN variants JSON DEFAULT '[]'",
            "source": "ALTER TABLE material_spans ADD COLUMN source JSON DEFAULT '{}'",
            "source_tail": "ALTER TABLE material_spans ADD COLUMN source_tail VARCHAR",
            "integrity": "ALTER TABLE material_spans ADD COLUMN integrity JSON DEFAULT '{}'",
            "quality_flags": "ALTER TABLE material_spans ADD COLUMN quality_flags JSON DEFAULT '[]'",
            "v2_index_version": "ALTER TABLE material_spans ADD COLUMN v2_index_version VARCHAR",
            "v2_business_family_ids": "ALTER TABLE material_spans ADD COLUMN v2_business_family_ids JSON DEFAULT '[]'",
            "v2_index_payload": "ALTER TABLE material_spans ADD COLUMN v2_index_payload JSON DEFAULT '{}'",
        }
    }
    with engine.begin() as connection:
        for table_name, columns in migrations.items():
            existing = {
                row[1]
                for row in connection.exec_driver_sql(f"PRAGMA table_info({table_name})").fetchall()
            }
            for column_name, ddl in columns.items():
                if column_name not in existing:
                    connection.exec_driver_sql(ddl)
