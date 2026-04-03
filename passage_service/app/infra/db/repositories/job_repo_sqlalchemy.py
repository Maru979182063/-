from typing import Any

from sqlalchemy.orm import Session

from app.infra.db.orm.job import JobORM
from app.infra.db.repositories.utils import new_id


class SQLAlchemyJobRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create(self, kind: str, payload: dict[str, Any] | None = None) -> JobORM:
        job = JobORM(id=new_id("job"), kind=kind, status="running", payload=payload or {}, result={})
        self.session.add(job)
        self.session.commit()
        self.session.refresh(job)
        return job

    def get(self, job_id: str) -> JobORM | None:
        return self.session.get(JobORM, job_id)

    def mark_finished(self, job_id: str, status: str, result: dict[str, Any] | None = None) -> JobORM:
        job = self.session.get(JobORM, job_id)
        job.status = status
        job.result = result or {}
        self.session.commit()
        self.session.refresh(job)
        return job
