# Contributing to Rasattrading MCP

Thanks for considering a contribution. This project trades with real money, so code that
touches execution, credentials, or risk handling is reviewed carefully. Please read this guide
before opening a pull request.

## Development environment

Create an isolated Python 3.11+ environment and install the project with its development
extras:

```
python -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m pytest -q
```

The full test suite currently runs **525 tests** and must pass before any merge. Run it before
you start (to confirm your baseline) and again before you open a pull request. If you add a
regression test for a fix, the suite should grow by at least that amount.

## Branch and pull request workflow

1. Create a branch from `main` for your change:
   ```
   git checkout main
   git checkout -b fix/your-change
   ```
2. Make focused, incremental commits. One logical change per commit.
3. Run the full test suite locally and make sure it passes.
4. Open a pull request to `main`. Describe what the change does and why; for behavior changes,
   reference the test coverage you added.

### Parallel work and worktrees

Parallel agent work is always done in a separate Git worktree so two people never edit the same
physical directory at once. If you are working alongside other contributors, prefer a worktree
created from the current `main`.

## Code style

- Follow the existing code. Read the surrounding module before you edit; the codebase has a
  deliberately restrained, unadorned style.
- The project has a minimal-comment culture. Code should be self-explanatory; add a comment
  only where intent is not obvious from the code. Do not sprinkle explanatory comments over
  what the code already states.
- No test-only or throwaway code in production paths.
- Timestamps are stored as epoch seconds internally. Millisecond values from Binance must be
  normalized to seconds on input. Mixed units are never stored.

## Tests

- `tests/` mirrors the package layout. Put a test for your change next to the module it tests.
- Every behavioral change should carry a regression test.
- The full suite (`python -m pytest -q`) must pass before the pull request is opened.

## Development rules and security constraints

Contributors should preserve these invariants:

- Accounts start in paper mode; enabling real trading is a deliberate, one-way operation.
- Credentials must be encrypted at rest, never logged, and never committed in plaintext.
- Price-action records are immutable and append-only.
- `scan_market` filters must remain an allowlisted AST rather than raw SQL or free-form input.
- Price-action calculations must use closed candles only.
- Binance request signing must preserve the query-string order sent by the HTTP client.
- The emergency-stop command must remain usable independently of the daemon.

If your change touches any of these, call it out explicitly in the pull request description.
