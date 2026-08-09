# AGENTS.md — Rasattrading MCP

AI ajanları için Binance price-action/likidite odaklı MCP sunucusu. Python 3.11, `daemon + ince MCP stdio adapter` mimarisi.

## Proje yapısı

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

Envelope her tool cevabında ortak: `{ok, data?, error?{code,message}, meta{as_of, source, freshness, algo_version?}}`.

## Ortam kurulumu ve test

```
python -m venv .venv
.venv\Scripts\pip install -e .[dev]
.venv\Scripts\python -m pytest -q
```

`.venv` gitignore'lu — her yeni worktree'de sıfırdan kurulmalı (dosya kopyalanmaz).

Referans test sayısı sürekli artıyor (şu an ~325); herhangi bir değişiklikten önce baseline'ı çalıştırıp mevcut sayıyı doğrula, değişiklikten sonra hepsinin geçtiğini (ve baseline + yeni regresyon testleri kadar arttığını) teyit et.

## Kritik güvenlik/tasarım kuralları — asla bozma

- **Paper-mode varsayılan, real-trading tek yönlü kilit açma:** her hesap `trading_lock=paper` ile başlar; `enable_real_trading` tek seferlik, geri alınamaz bir unlock'tır. Bu akışı gevşetecek/atlayacak hiçbir "kolaylık" eklenmez.
- **Kimlik bilgileri:** API key/secret asla loglanmaz, asla düz metin kalıcı depoda tutulmaz — DPAPI ile şifrelenir (`storage/credentials.py`). Elle aktarılan geçici düz-metin dosyalar iş bitince silinir.
- **Immutable PA kayıtları:** `market_structure`/`liquidity_zones`/`order_blocks`/`annotations` tabloları `effective_from`/`effective_to` + `algo_version` ile append-only'dir — üzerine yazma yok. Yeni bir hesaplama eskisini kapatır (`effective_to` set eder), asla mutasyona uğratmaz.
- **`scan_market` filtreleri serbest SQL değildir** — allowlisted bir AST (`pa/screener.py: FILTER_KEYS`). Yeni bir filtre türü eklerken bu allowlist deseni korunmalı, hiçbir zaman ham string/SQL enjeksiyonuna açık bir yol eklenmemeli.
- **`emergency_stop.py` daemon'dan bağımsız çalışabilmeli** — daemon çökmüş olsa bile kendi başına pozisyon kapatabilmeli/`disable_real_trading` yapabilmeli. Bu script'e daemon'a sıkı bağımlılık getiren bir değişiklik yapılmaz.
- **Kapalı mum kuralı:** PA hesaplamaları yalnızca kapanmış (closed) mumlar üzerinden yapılır — `/klines` cevabındaki henüz oluşmakta olan son bar hiçbir zaman `candles` tablosuna veya PA context'ine dahil edilmez (`filter_closed_candles`, `_store` closed-bar filtresi — bkz. ticket 1.6).
- **Binance imza sırası:** HMAC imzası, aiohttp/yarl'ın gönderdiği gerçek query-string sırasıyla eşleşmeli — `sorted()` ile yeniden sıralama YAPMA (geçmişte tüm signed istekleri kıran bir bug'dı).
- **Timestamp birimi:** dahili her yerde saniye (epoch seconds) standardı — Binance'ten gelen ms değerleri girişte `timeutil.to_epoch_seconds()` ile normalize edilir, karışık birim asla saklanmaz.

## Canlı daemon ile çalışırken

Geliştirme sırasında `C:\Denemeler\rasattrading-mcp` (main) üzerinde çoğunlukla **kullanıcının gerçek Binance hesap anahtarlarıyla çalışan canlı bir daemon** olabilir (`127.0.0.1:8751`, `~/.rasattrading/daemon.lock`'ta bearer token).

- main dizininde çalışırken önce `git status` ile working tree'yi kontrol et; commit'lenmemiş/hassas bir dosya (örn. `.testkey` benzeri) görürsen **dokunma**, sorulmadan silme/üzerine yazma.
- Kod değişikliği yapan ajanlar kendi git worktree'sinde çalışır, main'deki canlı daemon'a dosya sistemi seviyesinde dokunmaz.
- Daemon'a salt-okunur RPC ile bağlanmak (analiz/tarama amaçlı) güvenlidir — token `daemon.lock`'tan okunur, `POST /rpc` `{tool, params}` gövdesiyle çağrılır. Gerçek emir açan (`place_order`, `enable_real_trading`, vb.) tool'lar yalnızca kullanıcının açık onayıyla çağrılır.
- Daemon'ı yeniden başlatmak gerekiyorsa: `daemon.lock`'u kaldır (graceful shutdown tetikler) → `adapter/launcher.py`'nin `ensure_daemon` akışıyla yeniden başlat → `list_accounts` (hesapların DPAPI'den doğru okunduğunu) ve `get_readiness` ile doğrula.
- Yoğun eşzamanlı kullanımda (birden fazla ajan aynı anda tarama yapıyorsa) HTTP RPC istekleri varsayılan ~20-30s timeout'u aşabilir — `scan_market` gibi tüm evreni tarayan çağrılarda istemci timeout'unu yükselt (180-300s).

## Git worktree iş akışı

Paralel ajan çalışması her zaman ayrı bir git worktree'de yapılır (aynı fiziksel dizinde iki ajan aynı anda çalışmaz — commit çakışması/bozulmuş çalışma ağacı riski). Yeni bir fix/ticket için: `main`'den yeni bir branch+worktree oluştur, orada çalış, işin bitince ilgili ajan (merge sorumlusu) `main`'e fast-forward/merge eder ve tam suite'i tekrar doğrular.

## Migration kuralları

`storage/migrations.py` sıralı, numaralandırılmış migration listesi kullanır. Paralel dallarda aynı migration numarasını **kullanma** — birleştirmeden önce çakışma varsa sıraya koy (numarayı yeniden numarala), migration runner isim bazlı skip-safety içerir (aynı isimli migration'ın branch'e özgü numarayla iki kez uygulanmasını engeller) ama yeni migration eklerken yine de mevcut en yüksek numaradan devam et.

## Ticket/plan ile çalışma

Her ticket `C:\Users\alper\.traycer\epics\...\tickets\<id>\index.md` altında `kind: ticket`, `status` (0=todo,1=in-progress,2=done) frontmatter'ıyla tutulur. Bir ticket üzerinde çalışırken: ticket'ı ve referans verdiği review/kanıt artifact'ını oku, sırayla uygula, her ticket sonunda ayrı commit at, tam test suite'ini çalıştır, ticket dosyasının `status`'unu günceller, koordinatör ajana özet rapor gönder.
