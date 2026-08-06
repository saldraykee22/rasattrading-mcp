"""Modül 2 — Price Action & Screener/Alarm hesaplama katmanı.

Saf, deterministik hesaplama fonksiyonları (swings, likidite, OB/FVG, VWAP,
session) + kalıcı immutable kayıtları yöneten analiz/annotation/screener/alarm
servisleri. Tüm eşikler sürümlenir: bir eşik değişirse `algo_version` artar,
eski kayıtlar `effective_to` ile kapatılır (üzerine yazma yok).
"""
