import functools
import threading


def call_by_async(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        def _asyncio_thread():
            self.async_loop.run_until_complete(func(self, *args, **kwargs))
        threading.Thread(target=_asyncio_thread, daemon=True).start()
    return wrapper
