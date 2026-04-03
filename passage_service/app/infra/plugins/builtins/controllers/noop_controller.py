from app.domain.models.plugin_contracts import BaseController, ControlProfile, RunContext


class NoopController(BaseController):
    name = "noop_controller"
    version = "0.1.0"

    def build_control_profile(self, material_span: dict, request_payload: dict, run_context: RunContext) -> ControlProfile:
        return ControlProfile(plugin_name=self.name, payload={"message": "controller placeholder"})
