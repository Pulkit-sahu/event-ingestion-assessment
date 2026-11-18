import threading
import time
from collections import defaultdict, deque


class RateLimiter:
    """Fixed window-per-key limiter kept in process memory.

    One Flask process is protected. Several processes would need a shared
    store such as Redis; that tradeoff is documented in ARCHITECTURE.md.
    """

    def __init__(self, limit, window_seconds):
        self.limit = limit
        self.window = window_seconds
        self._hits = defaultdict(deque)
        self._lock = threading.Lock()

    def allow(self, key):
        now = time.monotonic()
        with self._lock:
            hits = self._hits[key]
            while hits and now - hits[0] >= self.window:
                hits.popleft()
            if len(hits) >= self.limit:
                if not hits:
                    self._hits.pop(key, None)
                return False, 0
            hits.append(now)
            remaining = self.limit - len(hits)
            return True, remaining
