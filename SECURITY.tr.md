# Güvenlik Politikası

Rasattrading MCP canlı borsa kimlik bilgilerini işler ve gerçek parayla emir açıp kapatabilir.
Güvenlik birinci sınıf bir konu olarak ele alınır.

## Desteklenen sürümler

| Sürüm | Destek |
| --- | --- |
| 0.1.x (en güncel) | ✅ |

Yalnızca en güncel sürüm güvenlik düzeltmeleri alır. Bu projeye bağımlıysanız güncel sürümde
kalın.

## Güvenlik açığı bildirme

Şüphelendiğiniz güvenlik açıklarını lütfen açık issue'lar üzerinden değil, özel olarak bildirin.

**Güvenlik açıkları için halka açık bir GitHub issue açmayın.** Bunun yerine GitHub'ın özel
güvenlik danışma (security advisory) akışını kullanın:

1. Deponun **Security** sekmesine gidin.
2. **Report a vulnerability** (veya *Private vulnerability reporting* etkinse **New advisory**)
   seçeneğini seçin.
3. Danışma detaylarını girin: etkilenen bileşen, minimal bir yeniden üretim, etki ve önerilen
   düzeltme.

Rapora yardımcı olacaklar:

- Etkilenen proje ve sürüm.
- Yeniden üretim adımları; mümkünse minimal bir örnek.
- Etki değerlendirmesi (örn. bir saldırgan kimlik bilgilerini okuyabilir mi, emir açabilir mi,
  paper/real trading kilidini atlayabilir mi?).
- Önerilen herhangi bir düzeltme.

Raporlar makul bir süre içinde yanıtlanır. İfşa zamanlaması konusunda sizinle birlikte
çalışacağız; sorunu kamuya açmadan önce bize bir düzeltme ve sürüm şansı tanıyın.

## Kullanıcılar ve katkıda bulunanlar için güvenlik notları

- **Kimlik bilgileri**: Binance API key ve secret'ları diskte Windows DPAPI ile şifrelenir
  (`storage/credentials.py`). Asla loglanmaz ve diskte düz metin saklanmaz. API key veya
  secret'ları issue'lara, pull request'lere veya sohbet loglarına yapıştırmayın.
- **Paper-first**: her hesap `trading_lock=paper` ile başlar. `enable_real_trading` hesap başına
  tek yönlü, geri alınamaz bir unlock'tır; gerçek trading açık hesaplar silinemez.
- **Fail-closed risk**: emirler asla otomatik açılmaz. Alarmlar açık insan onayı gerektiren
  `pending_order` üretir.
- **Yalnızca yerel kontrol düzlemi**: daemon'un HTTP IPC'si localhost'a bağlanır ve bir bearer
  token ile doğrulanır. Token'ı ve `~/.rasattrading/` dizinini gizli tutun.
- **İmzalı istekler**: tüm doğrulanmış Binance istekleri HMAC imzalıdır; imzalar, HTTP
  istemcisinin gönderdiği birebir query-string sıralamasıyla eşleşmelidir.
- **Emergency stop**: `rasattrading-emergency-stop` tasarım gereği daemon'dan bağımsız
  çalışır. Her canlı trading ortamında kullanılabilir ve test edilmiş durumda tutun.

Tüm geliştirme ve güvenlik kuralları için `AGENTS.md`'ye bakın.
