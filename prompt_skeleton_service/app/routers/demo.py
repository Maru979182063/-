from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

DEMO_INDEX = Path(__file__).resolve().parent.parent / "demo_static" / "index.html"

router = APIRouter(tags=["demo"])


@router.get("/demo", include_in_schema=False)
def demo_shell() -> HTMLResponse:
    html = DEMO_INDEX.read_text(encoding="utf-8")
    html = html.replace("/demo-static/styles.css?v=20260402f", "/demo-static/styles.css?v=20260403g")
    html = html.replace("/demo-static/styles.css?v=20260403a", "/demo-static/styles.css?v=20260403g")
    html = html.replace("/demo-static/styles.css?v=20260403b", "/demo-static/styles.css?v=20260403g")
    html = html.replace("/demo-static/styles.css?v=20260403c", "/demo-static/styles.css?v=20260403g")
    html = html.replace("/demo-static/styles.css?v=20260403d", "/demo-static/styles.css?v=20260403g")
    html = html.replace("/demo-static/styles.css?v=20260403e", "/demo-static/styles.css?v=20260403g")
    html = html.replace("/demo-static/styles.css?v=20260403f", "/demo-static/styles.css?v=20260403g")
    html = html.replace("/demo-static/app_v2.js?v=20260402f", "/demo-static/app_v2.js?v=20260403g")
    html = html.replace("/demo-static/app_v2.js?v=20260403a", "/demo-static/app_v2.js?v=20260403g")
    html = html.replace("/demo-static/app_v2.js?v=20260403b", "/demo-static/app_v2.js?v=20260403g")
    html = html.replace("/demo-static/app_v2.js?v=20260403c", "/demo-static/app_v2.js?v=20260403g")
    html = html.replace("/demo-static/app_v2.js?v=20260403d", "/demo-static/app_v2.js?v=20260403g")
    html = html.replace("/demo-static/app_v2.js?v=20260403e", "/demo-static/app_v2.js?v=20260403g")
    html = html.replace("/demo-static/app_v2.js?v=20260403f", "/demo-static/app_v2.js?v=20260403g")
    if "/demo-static/app_v2_zh_patch.js?v=20260403g" not in html:
        html = html.replace(
            "</body>",
            '    <script src="/demo-static/app_v2_zh_patch.js?v=20260403g"></script>\n  </body>',
        )
    return HTMLResponse(
        content=html,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )
