"""Helpers for the common tool envelope.

Request:  {...params, request_id?, idempotency_key?}
Response: {ok, data?, error?{code,message}, meta{as_of, source, freshness, algo_version?}}
"""

from __future__ import annotations

import dataclasses
import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from .errors import RasatError

FRESHNESS_FRESH = "fresh"
FRESHNESS_STALE = "stale"

SOURCE_DAEMON = "daemon"


def utc_iso(ts: float | None = None) -> str:
    ts = ts if ts is not None else time.time()
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="milliseconds")


def new_request_id() -> str:
    return uuid.uuid4().hex


@dataclasses.dataclass
class Meta:
    as_of: str
    source: str = SOURCE_DAEMON
    freshness: str = FRESHNESS_FRESH
    algo_version: Optional[str] = None
    extra: dict = dataclasses.field(default_factory=dict)

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "as_of": self.as_of,
            "source": self.source,
            "freshness": self.freshness,
        }
        if self.algo_version is not None:
            d["algo_version"] = self.algo_version
        d.update(self.extra)
        return d


def ok_response(data: Any = None, meta: Meta | None = None, request_id: str | None = None) -> dict:
    resp: dict[str, Any] = {"ok": True}
    if data is not None:
        resp["data"] = data
    resp["meta"] = (meta or Meta(as_of=utc_iso())).to_dict()
    if request_id:
        resp["request_id"] = request_id
    return resp


def error_response(
    code: str,
    message: str,
    details: Any = None,
    meta: Meta | None = None,
    request_id: str | None = None,
) -> dict:
    resp: dict[str, Any] = {"ok": False}
    err: dict[str, Any] = {"code": code, "message": message}
    if details is not None:
        err["details"] = details
    resp["error"] = err
    resp["meta"] = (meta or Meta(as_of=utc_iso())).to_dict()
    if request_id:
        resp["request_id"] = request_id
    return resp


def error_response_from_exc(exc: Exception, request_id: str | None = None) -> dict:
    if isinstance(exc, RasatError):
        return error_response(exc.code, exc.message, exc.details, request_id=request_id)
    return error_response("INTERNAL_ERROR", str(exc), request_id=request_id)


def dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)
