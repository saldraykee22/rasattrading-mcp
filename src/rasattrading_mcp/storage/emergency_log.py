"""Bağımsız `emergency_stop` hash-chain log (ticket 3.6).

Daemon çökse/erişilemez olsa bile emergency_stop kendi append-only log dosyasına
yazar — audit bütünlüğü daemon'un ayakta olmasına bağlı kalmaz. Format audit_log
ile AYNI hash-chain desenini kullanır (`hash_entry`), böylece daemon açılışta bu
dosyayı okuyup `audit_log`'a birebir reconcile edebilir.

Dosya: data_dir/emergency_stop.log — her satır tek JSON entry'si.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from .audit import GENESIS_HASH, _redact, hash_entry

logger = logging.getLogger("rasattrading.storage.emergency_log")


class EmergencyLog:
    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, *, actor: str, action: str, details: dict | None = None) -> str:
        """Bir satır ekler; entry'nin hash'ini döner (reconcile dedup anahtarı)."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            prev = self._tail()
            prev_seq = prev["seq"] if prev else 0
            prev_hash = prev["hash"] if prev else GENESIS_HASH
            seq = prev_seq + 1
            safe_details = _redact(details or {})
            details_json = json.dumps(safe_details, ensure_ascii=False, sort_keys=True, default=str)
            created_at = int(time.time())
            h = hash_entry(prev_hash, seq, actor, action, details_json, created_at)
            entry = {
                "seq": seq,
                "actor": actor,
                "action": action,
                "details": safe_details,
                "prev_hash": prev_hash,
                "hash": h,
                "created_at": created_at,
            }
            fh.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
            fh.flush()
            return h

    def _tail(self) -> dict | None:
        entries = self.entries()
        return entries[-1] if entries else None

    def entries(self) -> list[dict]:
        if not self.path.exists():
            return []
        rows: list[dict] = []
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    rows.append({"_corrupt": True, "raw": line})
        return rows

    def verify(self) -> list[dict]:
        """Zincir bütünlüğünü doğrular; kırık satırları döner (boş = sağlam)."""
        broken: list[dict] = []
        prev_hash = GENESIS_HASH
        prev_seq = 0
        for row in self.entries():
            if row.get("_corrupt"):
                broken.append({"seq": row.get("seq"), "reason": "bozuk satır (JSON değil)"})
                continue
            seq = int(row["seq"])
            details_json = json.dumps(row.get("details", {}), ensure_ascii=False, sort_keys=True, default=str)
            created_at = int(row["created_at"])
            if seq != prev_seq + 1:
                broken.append({"seq": seq, "reason": f"seq atlama/silinme (beklenen {prev_seq + 1})"})
            if str(row.get("prev_hash")) != prev_hash:
                broken.append({"seq": seq, "reason": "zincir kopması (prev_hash uyuşmuyor)"})
            expected = hash_entry(prev_hash, seq, str(row["actor"]), str(row["action"]), details_json, created_at)
            if str(row.get("hash")) != expected:
                broken.append({"seq": seq, "reason": "hash uyuşmazlığı (satır değiştirilmiş)"})
            prev_hash = str(row["hash"])
            prev_seq = seq
        return broken

    def is_action_done(self, action: str, idem_key: str) -> bool:
        """Aynı (action, idem_key) daha önce loglandı mı? — idempotency için."""
        for row in self.entries():
            if row.get("_corrupt"):
                continue
            details = row.get("details") or {}
            if row.get("action") == action and details.get("idem_key") == idem_key:
                return True
        return False

    def clear(self) -> None:
        self.path.unlink(missing_ok=True)


async def reconcile_emergency_log(db, audit, log: EmergencyLog) -> dict:
    """Daemon açılışında emergency_stop log'unu `audit_log`'a mutabakat eder.

    - Her emergency entry `audit_log`'a `action="emergency_stop"` olarak yazılır.
    - Idempotent: `emergency_reconciled` tablosunda kayıtlı hash'ler tekrar yazılmaz.
    - Log dosyası bozuksa (hash kırılması) bu durum da audit'e not düşülür;
      yine de satırlar yazılır, bozulma rapor edilir.
    """
    from .audit import AuditLog

    broken = log.verify()

    def _reconcile(conn) -> tuple[int, int]:
        existing = {
            r["entry_hash"]
            for r in conn.execute("SELECT entry_hash FROM emergency_reconciled").fetchall()
        }
        written = 0
        for entry in log.entries():
            if entry.get("_corrupt"):
                continue
            h = entry.get("hash")
            if h in existing:
                continue
            audit.append_in_connection(
                conn,
                actor="emergency_stop",
                action="emergency_stop",
                details={
                    "source": "emergency_stop.log",
                    "entry_seq": entry["seq"],
                    "action": entry["action"],
                    "entry": entry.get("details", {}),
                },
            )
            conn.execute(
                "INSERT INTO emergency_reconciled (entry_hash, seq, reconciled_at) VALUES (?, ?, ?)",
                (h, entry["seq"], int(time.time())),
            )
            written += 1
        return written, len(broken)

    written, broken_count = await db.write(_reconcile)
    result = {"reconciled": written, "log_broken": broken}
    if broken:
        logger.warning("emergency_stop.log zincir kırılması tespit edildi: %s", broken)
    return result
