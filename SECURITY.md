# Security Policy

Rasattrading MCP handles live exchange credentials and can place and close real-money orders.
Security is treated as a first-class concern.

## Supported versions

| Version | Supported |
| --- | --- |
| 0.1.x (latest) | ✅ |

Only the latest release receives security fixes. If you depend on this project, stay on the
current release.

## Reporting a vulnerability

Please report suspected vulnerabilities privately, not through public issues.

**Do not open a public GitHub issue for a security vulnerability.** Instead, use GitHub's
private security advisory workflow:

1. Go to the repository's **Security** tab.
2. Select **Report a vulnerability** (or **New advisory** under *Private vulnerability
   reporting* if enabled).
3. Provide the advisory details: affected component, a minimal reproduction, impact, and any
   suggested fix.

What helps the report:

- Project and version affected.
- Steps to reproduce, including a minimal example where possible.
- Impact assessment (e.g., could an attacker read credentials, place orders, or bypass the
  paper/real trading lock?).
- Any proposed remediation.

Reports are acknowledged within a reasonable timeframe. We will work with you on disclosure
timing; please give us a chance to fix and release a patch before making the issue public.

## Security notes for users and contributors

- **Credentials**: Binance API keys and secrets are encrypted at rest with Windows DPAPI
  (`storage/credentials.py`). They are never logged and never stored in plaintext on disk.
  Do not paste API keys or secrets into issues, pull requests, or chat logs.
- **Paper-first**: every account starts with `trading_lock=paper`. `enable_real_trading` is a
  one-way, irreversible unlock per account; accounts with real trading enabled cannot be
  deleted.
- **Fail-closed risk**: orders are never opened automatically. Alarms generate `pending_order`s
  that require explicit human approval.
- **Local-only control plane**: the daemon's HTTP IPC binds to localhost and is authenticated
  with a bearer token. Keep the token and `~/.rasattrading/` private.
- **Signed requests**: all authenticated Binance requests are HMAC-signed; signatures must
  match the exact query-string ordering the HTTP client sends.
- **Emergency stop**: `rasattrading-emergency-stop` works independently of the daemon by
  design. Keep it available and tested in any live-trading environment.

See `AGENTS.md` for the full set of development and security rules.
