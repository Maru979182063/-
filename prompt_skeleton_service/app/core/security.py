from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.settings import get_settings


logger = logging.getLogger(__name__)


class _RateLimiter:
    def __init__(self) -> None:
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, key: str, limit: int, *, window_seconds: int = 60) -> tuple[bool, int]:
        if limit <= 0:
            return True, 0

        now = time.time()
        queue = self._hits[key]
        while queue and queue[0] <= now - window_seconds:
            queue.popleft()
        if len(queue) >= limit:
            retry_after = max(1, int(window_seconds - (now - queue[0])))
            return False, retry_after
        queue.append(now)
        return True, 0


_RATE_LIMITER = _RateLimiter()


class SecurityMiddleware(BaseHTTPMiddleware):
    EXEMPT_PATHS = {"/healthz", "/readyz", "/docs", "/openapi.json", "/redoc"}
    EXEMPT_PREFIXES = ("/docs/oauth2-redirect", "/demo", "/demo-static")

    async def dispatch(self, request: Request, call_next):
        settings = get_settings()
        request_id = request.headers.get("X-Request-ID") or str(uuid4())
        request.state.request_id = request_id
        start = time.perf_counter()

        if self._should_protect(request.url.path):
            auth_response = self._check_auth(request, request_id, settings.security.enabled, settings.security.api_token)
            if auth_response is not None:
                return auth_response
            rate_response = self._check_rate_limit(request, request_id, settings.security.rate_limit_per_minute)
            if rate_response is not None:
                return rate_response

        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        self._attach_generation_gate_headers(request, response)
        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        logger.info(
            "request_complete request_id=%s method=%s path=%s status=%s duration_ms=%s",
            request_id,
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
        )
        return response

    def _should_protect(self, path: str) -> bool:
        if path in self.EXEMPT_PATHS:
            return False
        return not any(path.startswith(prefix) for prefix in self.EXEMPT_PREFIXES)

    def _check_auth(
        self,
        request: Request,
        request_id: str,
        enabled: bool,
        api_token: str | None,
    ) -> JSONResponse | None:
        if not enabled:
            return None

        expected = f"Bearer {api_token}" if api_token else None
        provided = request.headers.get("Authorization")
        if expected and provided == expected:
            return None

        return JSONResponse(
            status_code=401,
            content={
                "error": {
                    "message": "Unauthorized request.",
                    "details": {"request_id": request_id},
                }
            },
            headers={"X-Request-ID": request_id},
        )

    def _check_rate_limit(self, request: Request, request_id: str, limit: int) -> JSONResponse | None:
        client_host = request.client.host if request.client else "unknown"
        key = f"{client_host}:{request.url.path}"
        allowed, retry_after = _RATE_LIMITER.allow(key, limit)
        if allowed:
            return None
        return JSONResponse(
            status_code=429,
            content={
                "error": {
                    "message": "Too many requests.",
                    "details": {"request_id": request_id, "retry_after_seconds": retry_after},
                }
            },
            headers={"Retry-After": str(retry_after), "X-Request-ID": request_id},
        )

    def _attach_generation_gate_headers(self, request: Request, response: JSONResponse) -> None:
        gate_state = getattr(request.state, "generation_gate", None)
        if not isinstance(gate_state, dict):
            return

        header_mapping = {
            "queue_position": "X-Generation-Queue-Position",
            "wait_seconds": "X-Generation-Wait-Seconds",
            "active_requests": "X-Generation-Active",
            "waiting_requests": "X-Generation-Waiting",
            "max_concurrent": "X-Generation-Max-Concurrent",
            "max_waiting": "X-Generation-Max-Waiting",
        }
        for key, header_name in header_mapping.items():
            value = gate_state.get(key)
            if value is None:
                continue
            response.headers[header_name] = str(value)


def install_security_middleware(app: FastAPI) -> None:
    app.add_middleware(SecurityMiddleware)
