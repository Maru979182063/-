from __future__ import annotations

from app.services.material_bridge_v2 import MaterialBridgeV2Service


class MaterialBridgeService(MaterialBridgeV2Service):
    """Compatibility wrapper: the generation flow now uses the V2 material pool."""

