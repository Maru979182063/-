from app.domain.models.plugin_contracts import BaseBuilder, ControlProfile, PromptBundle, RunContext, ValidationResult


class NoopBuilder(BaseBuilder):
    name = "noop_builder"
    version = "0.1.0"

    def build_prompt_bundle(self, material_span: dict, control_profile: ControlProfile, run_context: RunContext) -> PromptBundle:
        return PromptBundle(plugin_name=self.name, payload={"message": "builder placeholder"})

    def validate_output(self, output_payload: dict, run_context: RunContext) -> ValidationResult:
        return ValidationResult(valid=True, issues=[])
