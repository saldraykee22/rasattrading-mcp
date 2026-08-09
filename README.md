# Rasattrading MCP

AI ajanları için Binance price-action/likidite odaklı bir MCP (Model Context Protocol) sunucusu. Bir arka plan daemon'ı Binance REST/WS verisini toplar, price-action analizi (yapı, likidite bölgeleri, order block'lar) ve alarm/screener mantığını çalıştırır; ince bir MCP stdio adapter bu daemon'a yerel HTTP RPC ile bağlanıp AI ajanlarına araç (tool) olarak sunar.

## Özellikler

- **Price-action analizi**: piyasa yapısı, likidite bölgeleri, order block'lar, VWAP session'ları — yalnızca kapanmış mumlar üzerinden, immutable/append-only kayıtlarla.
- **Screener**: allowlisted filtre AST'i ile serbest ama güvenli tarama (`scan_market`), skorlama.
- **Alarm motoru**: event-driven tetikleme, dedup, cooldown; tetiklenince onay bekleyen emir (`pending_orders`) oluşturabilir.
- **Execution**: idempotent emir gönderimi, OCO desteği, reconcile-before-retry, fail-closed risk politikası. Emirler **otomatik açılmaz** — alarm/order_spec üretir, insan onayı (`approve_pending_order`) ile açılır.
- **Paper/real ayrımı**: her hesap `paper` modda başlar; `enable_real_trading` tek yönlü, geri alınamaz bir unlock'tır.
- **Emergency stop**: daemon'dan bağımsız çalışabilen, standalone bir kill-switch script'i (`emergency_stop.py`).

## Kurulum

```
python -m venv .venv
.venv\Scripts\pip install -e .[dev]
.venv\Scripts\python -m pytest -q
```

Gereksinimler: Python 3.11+, Windows (credential şifreleme için DPAPI kullanılıyor — `keyring` ile başka platformlara taşınabilir ama şu an test/geliştirme Windows üzerinde).

## Mimari

```
src/rasattrading_mcp/
  config.py, errors.py, envelope.py, tools.py, logging_util.py   # ortak
  daemon/    # HTTP IPC sunucusu (localhost, bearer token), lock/readiness/handlers
  storage/   # SQLite (WAL), migrations, audit hash-chain, credentials (DPAPI), orders, risk_policy
  data/      # Binance REST/WS client, rate limit, universe, kline pipeline, futures, order broker
  pa/        # price-action zinciri: swings -> liquidity -> obfvg -> vwap_sessions -> analysis -> screener -> alarms -> worker
  adapter/   # MCP stdio subprocess (daemon'a HTTP RPC ile bağlanır)
  emergency_stop.py   # daemon'dan bağımsız, standalone kill switch script'i
tests/
```

Detaylı geliştirme/güvenlik kuralları için `AGENTS.md`'ye bakın.

## ⚠️ Uyarı

Bu proje gerçek parayla emir açıp kapatabilen bir sistemdir. Kendi sorumluluğunuzdadır — canlı bir hesaba bağlamadan önce testnet/paper modda test edin, küçük sermayeyle başlayın, `emergency_stop.py`'nin kendi ortamınızda çalıştığından emin olun. Geliştirici veya katkıda bulunanlar, bu yazılımın kullanımından doğacak finansal kayıplardan sorumlu değildir.

## Lisans

MIT — bkz. [LICENSE](LICENSE).
