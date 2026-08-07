"""Defense-in-depth JSON Schema subset validator (T01).

Tool registry `input_schema`'ları için yeterli alt küme: `type`
(object/array/string/number/integer/boolean), `properties`, `required`,
`additionalProperties`, `enum`, `minimum`/`maximum`/`exclusiveMinimum`/
`exclusiveMaximum`, `minLength`/`maxLength`, `items`. `default` uygulanmaz
(handler'lar `params.get(key, default)` ile uygular).

Python json NaN/Infinity kabul ettiği için number/integer değerler sonlu
(finite) olmalıdır — bu sınırda NaN/Infinity girişi canonical INVALID_REQUEST
ile kesilir. `request_id`/`idempotency_key` transport seviyesi alanları olarak
her tool'da ekstra alan reddine takılmaz.
"""

from __future__ import annotations

import math

from .errors import ErrorCode, RasatError

#: Envelope'ın transport seviyesi alanları — tool params'ında her zaman kabul edilir.
TRANSPORT_FIELDS = ("request_id", "idempotency_key")


def _error(path: str, message: str) -> None:
    raise RasatError(ErrorCode.INVALID_REQUEST, f"{path}: {message}")


def _check_number(value, schema: dict, path: str, *, integer: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _error(path, "sayı olmalı")
    num = float(value)
    if not math.isfinite(num):
        _error(path, "sonlu (finite) bir sayı olmalı")
    if integer and not isinstance(value, int):
        _error(path, "integer olmalı")
    if "minimum" in schema and num < schema["minimum"]:
        _error(path, f"en az {schema['minimum']} olmalı")
    if "maximum" in schema and num > schema["maximum"]:
        _error(path, f"en fazla {schema['maximum']} olmalı")
    if "exclusiveMinimum" in schema and num <= schema["exclusiveMinimum"]:
        _error(path, f"{schema['exclusiveMinimum']} değerinden büyük olmalı")
    if "exclusiveMaximum" in schema and num >= schema["exclusiveMaximum"]:
        _error(path, f"{schema['exclusiveMaximum']} değerinden küçük olmalı")


def _validate(value, schema, path: str) -> None:
    if not isinstance(schema, dict):
        return
    stype = schema.get("type")

    if "enum" in schema:
        if value not in schema["enum"]:
            _error(path, f"geçerli değerlerden biri olmalı: {schema['enum']}")

    if stype == "object":
        if not isinstance(value, dict):
            _error(path, "nesne olmalı")
        props = schema.get("properties") or {}
        if schema.get("additionalProperties") is False:
            allowed = set(props)
            extra = [k for k in value if k not in allowed and k not in TRANSPORT_FIELDS]
            if extra:
                _error(path, f"bilinmeyen alan(lar): {sorted(extra)}")
        for key, sub in props.items():
            if key in value:
                _validate(value[key], sub, f"{path}.{key}")
        for req in schema.get("required") or []:
            if req not in value:
                _error(path, f"eksik zorunlu alan: {req}")
    elif stype == "array":
        if not isinstance(value, list):
            _error(path, "dizi olmalı")
        items = schema.get("items") or {}
        for i, item in enumerate(value):
            _validate(item, items, f"{path}[{i}]")
    elif stype == "string":
        if not isinstance(value, str):
            _error(path, "string olmalı")
        if "minLength" in schema and len(value) < schema["minLength"]:
            _error(path, f"en az {schema['minLength']} karakter olmalı")
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            _error(path, f"en fazla {schema['maxLength']} karakter olmalı")
    elif stype == "number":
        _check_number(value, schema, path)
    elif stype == "integer":
        _check_number(value, schema, path, integer=True)
    elif stype == "boolean":
        if not isinstance(value, bool):
            _error(path, "boolean olmalı")


def validate_params(params, schema) -> None:
    """Tool input_schema'ya göre params'ı doğrular; ihlalde INVALID_REQUEST."""
    if not isinstance(schema, dict):
        return
    _validate(params, schema, "params")
