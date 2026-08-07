"""Persisted state contracts shared by alarm and execution flows."""

from __future__ import annotations

PENDING_AWAITING_APPROVAL = "awaiting_approval"
PENDING_APPROVED = "approved"
PENDING_EXECUTING = "executing"
PENDING_EXECUTED = "executed"
PENDING_REJECTED = "rejected"
PENDING_RECONCILE_REQUIRED = "reconcile_required"
PENDING_EXPIRED = "expired"

PENDING_ACTIVE_STATUSES = frozenset(
    {
        PENDING_AWAITING_APPROVAL,
        PENDING_APPROVED,
        PENDING_EXECUTING,
        PENDING_RECONCILE_REQUIRED,
    }
)

PENDING_TERMINAL_STATUSES = frozenset(
    {
        PENDING_EXECUTED,
        PENDING_REJECTED,
        PENDING_EXPIRED,
    }
)

ORDER_RECONCILE_REQUIRED = "RECONCILE_REQUIRED"
