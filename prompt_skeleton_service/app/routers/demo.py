from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

DEMO_INDEX = Path(__file__).resolve().parent.parent / "demo_static" / "index.html"
FORCED_USER_MATERIAL_DEMO_INDEX = Path(__file__).resolve().parent.parent / "demo_static" / "user_material_demo.html"
DEMO_ASSET_VERSION = "20260410a"

router = APIRouter(tags=["demo"])


@router.get("/demo", include_in_schema=False)
def demo_shell() -> HTMLResponse:
    html = DEMO_INDEX.read_text(encoding="utf-8")
    html = html.replace("/demo-static/styles.css?v=20260408a", f"/demo-static/styles.css?v={DEMO_ASSET_VERSION}")
    html = html.replace("/demo-static/app_v2.js?v=20260408a", f"/demo-static/app_v2.js?v={DEMO_ASSET_VERSION}")
    if f"/demo-static/app_v2_zh_patch.js?v={DEMO_ASSET_VERSION}" not in html:
        html = html.replace(
            "</body>",
            f'    <script src="/demo-static/app_v2_zh_patch.js?v={DEMO_ASSET_VERSION}"></script>\n  </body>',
        )
    return HTMLResponse(
        content=html,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@router.get("/demo/user-material", include_in_schema=False)
def forced_user_material_demo_shell() -> HTMLResponse:
    html = FORCED_USER_MATERIAL_DEMO_INDEX.read_text(encoding="utf-8")
    html = html.replace("/demo-static/styles.css?v=20260408a", f"/demo-static/styles.css?v={DEMO_ASSET_VERSION}")
    html = html.replace("/demo-static/user_material_demo.js?v=20260408a", f"/demo-static/user_material_demo.js?v={DEMO_ASSET_VERSION}")
    return HTMLResponse(
        content=html,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )
