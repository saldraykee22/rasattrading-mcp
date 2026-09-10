"""Defense-in-depth JSON Schema subset validator (T01).

Sufficient subset for the tool registry's `input_schema`: `type`
(object/array/string/number/integer/boolean), `properties`, `required`,
`additionalProperties`, `enum`, `minimum`/`maximum`/`exclusiveMinimum`/
`exclusiveMaximum`, `minLength`/`maxLength`, `items`. `default` is not applied
(handlers apply defaults with `params.get(key, default)`).

Because Python json accepts NaN/Infinity, number/integer values must be finite;
at this boundary NaN/Infinity input is rejected with canonical INVALID_REQUEST.
`request_id`/`idempotency_key` are transport-level fields and are exempt from
each tool's additional-field rejection.
"""

from __future__ import annotations

import math

from .errors import ErrorCode, RasatError

#: Envelope transport-level fields — always accepted in tool params.
TRANSPORT_FIELDS = ("request_id", "idempotency_key")


def _error(path: str, message: str) -> None:
    raise RasatError(ErrorCode.INVALID_REQUEST, f"{path}: {message}")


def _check_number(value, schema: dict, path: str, *, integer: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _error(path, "must be a number")
    num = float(value)
    if not math.isfinite(num):
        _error(path, "must be a finite number")
    if integer and not isinstance(value, int):
        _error(path, "must be an integer")
    if "minimum" in schema and num < schema["minimum"]:
        _error(path, f"must be at least {schema['minimum']}")
    if "maximum" in schema and num > schema["maximum"]:
        _error(path, f"must be at most {schema['maximum']}")
    if "exclusiveMinimum" in schema and num <= schema["exclusiveMinimum"]:
        _error(path, f"must be greater than {schema['exclusiveMinimum']}")
    if "exclusiveMaximum" in schema and num >= schema["exclusiveMaximum"]:
        _error(path, f"must be less than {schema['exclusiveMaximum']}")


def _validate(value, schema, path: str) -> None:
    if not isinstance(schema, dict):
        return
    stype = schema.get("type")

    if "enum" in schema:
        if value not in schema["enum"]:
            _error(path, f"must be one of the valid values: {schema['enum']}")

    if stype == "object":
        if not isinstance(value, dict):
            _error(path, "must be an object")
        props = schema.get("properties") or {}
        if schema.get("additionalProperties") is False:
            allowed = set(props)
            extra = [k for k in value if k not in allowed and k not in TRANSPORT_FIELDS]
            if extra:
                _error(path, f"unknown field(s): {sorted(extra)}")
        for key, sub in props.items():
            if key in value:
                _validate(value[key], sub, f"{path}.{key}")
        for req in schema.get("required") or []:
            if req not in value:
                _error(path, f"missing required field: {req}")
    elif stype == "array":
        if not isinstance(value, list):
            _error(path, "must be an array")
        items = schema.get("items") or {}
        for i, item in enumerate(value):
            _validate(item, items, f"{path}[{i}]")
    elif stype == "string":
        if not isinstance(value, str):
            _error(path, "must be a string")
        if "minLength" in schema and len(value) < schema["minLength"]:
            _error(path, f"must be at least {schema['minLength']} characters")
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            _error(path, f"must be at most {schema['maxLength']} characters")
    elif stype == "number":
        _check_number(value, schema, path)
    elif stype == "integer":
        _check_number(value, schema, path, integer=True)
    elif stype == "boolean":
        if not isinstance(value, bool):
            _error(path, "must be a boolean")


def validate_params(params, schema) -> None:
    """Validate params against the tool input_schema; raise INVALID_REQUEST on violation."""
    if not isinstance(schema, dict):
        return
    _validate(params, schema, "params")
