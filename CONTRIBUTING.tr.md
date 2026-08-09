# Rasattrading MCP'ye katkıda bulunma

Katkı düşündüğünüz için teşekkürler. Bu proje gerçek parayla işlem yapar; icra, kimlik bilgisi
veya risk işleyen kodlar çok dikkatli incelenir. Pull request açmadan önce lütfen bu rehberi
okuyun.

## Geliştirme ortamı

```
python -m venv .venv
.venv\Scripts\pip install -e .[dev]
.venv\Scripts\python -m pytest -q
```

Tüm test suite'i şu an **525 test** çalıştırır ve her birleştirmeden önce geçmelidir. Başlamadan
önce (baseline'ınızı doğrulamak için) ve pull request açmadan önce tekrar çalıştırın. Bir fix
için regresyon testi eklediyseniz suite en az o kadar artmalıdır.

## Branch ve pull request akışı

1. Değişikliğiniz için `main`'den bir branch oluşturun:
   ```
   git checkout main
   git checkout -b fix/your-change
   ```
2. Odaklı, küçük commit'ler yapın. Her commit tek bir mantıksal değişiklik içersin.
3. Tüm test suite'ini yerelde çalıştırıp geçtiğinden emin olun.
4. `main`'e bir pull request açın. Değişikliğin ne yaptığını ve nedenini anlatın; davranış
   değişiklikleri için eklediğiniz test kapsamına referans verin.

### Paralel çalışma ve worktree'ler

Paralel ajan çalışması her zaman ayrı bir Git worktree'de yapılır — iki kişi aynı fiziksel
dizinde aynı anda düzenleme yapmaz. Diğer katkıda bulunanlarla birlikte çalışıyorsanız, güncel
`main`'den oluşturulmuş bir worktree kullanmayı tercih edin.

## Kod stili

- Mevcut kodu örnek alın. Düzenlemeden önce ilgili modülü okuyun; kod tabanı bilinçli olarak
  sade ve gösterişsiz bir stile sahiptir.
- Projede minimal yorum kültürü vardır. Kod kendini açıklamalıdır; yorumu yalnızca kodun
  amacı açıkça anlaşılmıyorsa ekleyin. Kodun zaten söylediğini açıklayan yorumlar serpmeyin.
- Üretim yollarında test amaçlı veya geçici kod olmaz.
- Dahili her yerde zaman damgası epoch saniyedir. Binance'ten gelen ms değerleri girişte
  saniyeye normalize edilmelidir. Karışık birim asla saklanmaz.

## Testler

- `tests/` paket düzenini yansıtır. Değişikliğinizin testini, test ettiği modülün yanına koyun.
- Her davranışsal değişiklik bir regresyon testi taşımalıdır.
- Pull request açılmadan önce tüm suite (`python -m pytest -q`) geçmelidir.

## Geliştirme kuralları ve güvenlik kısıtları

`AGENTS.md` (repo kökünde) katkıda bulunanlar için yetkili geliştirme ve güvenlik kuralları
setidir. Hem AI ajanları hem insanlar için yazılmıştır ve zayıflatılmamalıdır. Özellikle
herhangi bir şeyi değiştirmeden önce şunları okuyun:

- paper-mode varsayılanı ve tek yönlü `enable_real_trading` unlock'ı,
- kimlik bilgisi işleme (DPAPI şifreleme, düz metin saklama yok, key loglama yok),
- immutable/append-only price-action kayıtları,
- allowlisted `scan_market` filtre AST'si (ham SQL enjeksiyon yolları yok),
- price-action hesaplamalarında kapalı mum kuralı,
- Binance HMAC imza sıralama kısıtı,
- daemon'dan bağımsız `emergency_stop.py` gerekliliği.

Değişikliğiniz bunlardan herhangi birine dokunuyorsa, pull request açıklamasında bunu açıkça
belirtin.
