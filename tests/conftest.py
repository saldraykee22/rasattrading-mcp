import asyncio
import sys

# Prevent the Windows Proactor loop teardown KeyboardInterrupt; use a stable
# Selector loop for every test.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
