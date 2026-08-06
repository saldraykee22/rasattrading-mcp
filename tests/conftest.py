import asyncio
import sys

# Windows Proactor loop'un teardown KeyboardInterrupt'ını önler; her test için
# kararlı Selector loop kullanır.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
