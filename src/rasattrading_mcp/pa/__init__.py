"""Price-action, screener, and alarm computation layer.

Pure, deterministic computation functions (swings, liquidity, OB/FVG, VWAP,
session) plus analysis/annotation/screener/alarm services that manage persistent
immutable records. All thresholds are versioned: when a threshold changes,
`algo_version` increases and old records are closed with `effective_to` (never overwritten).
"""
