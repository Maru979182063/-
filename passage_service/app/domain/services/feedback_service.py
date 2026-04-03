from app.core.clock import utc_now
from app.core.enums import MaterialStatus, ReleaseChannel
from app.domain.services._common import ServiceBase


class FeedbackService(ServiceBase):
    def record_feedback(self, payload: dict) -> dict:
        record = self.feedback_repo.create_record(**payload)
        material = self.material_repo.get(payload["material_id"])
        if material is None:
            return {"feedback_id": record.id, "material_id": payload["material_id"], "status": "not_found"}
        aggregate = self.feedback_repo.get_aggregate(payload["material_id"])

        accept_count = material.accept_count
        reject_count = material.reject_count
        usage_count = material.usage_count + 1
        if payload["feedback_type"] == "accepted":
            accept_count += 1
        else:
            reject_count += 1
        accept_rate = accept_count / usage_count if usage_count else 0.0
        bad_case_count = (aggregate.bad_case_count if aggregate else 0) + (0 if payload["feedback_type"] == "accepted" else 1)

        self.feedback_repo.upsert_aggregate(
            payload["material_id"],
            accept_rate=accept_rate,
            type_match_score=accept_rate,
            difficulty_match_score=accept_rate,
            bad_case_count=bad_case_count,
            last_feedback_at=utc_now(),
        )

        status = material.status
        channel = material.release_channel
        if material.status == MaterialStatus.GRAY.value and accept_rate >= 0.8 and usage_count >= 3:
            status = MaterialStatus.PROMOTED.value
            channel = ReleaseChannel.STABLE.value
        elif bad_case_count >= 3:
            status = MaterialStatus.DEPRECATED.value
        updated = self.material_repo.update_metrics(
            payload["material_id"],
            usage_count=usage_count,
            accept_count=accept_count,
            reject_count=reject_count,
            quality_score=accept_rate,
            last_used_at=utc_now(),
            status=status,
            release_channel=channel,
        )
        self.audit_repo.log("material", payload["material_id"], "feedback", payload)
        return {"feedback_id": record.id, "material_id": updated.id, "status": updated.status, "release_channel": updated.release_channel}
