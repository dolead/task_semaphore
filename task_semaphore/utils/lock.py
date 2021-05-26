import time
from datetime import datetime, timedelta

DEFAULT_MAX_WAIT = 5  # in minutes
DEFAULT_LOCK_LOOP_WAIT_TIME = 2  # in seconds


class AbstractLock:

    def __init__(self, wait_for=DEFAULT_LOCK_LOOP_WAIT_TIME,
                 max_wait=DEFAULT_MAX_WAIT):
        self.wait_for = wait_for
        self.max_lock_wait = timedelta(minutes=max_wait)

    def __enter__(self):
        start = datetime.utcnow()
        while self.is_locked():
            if datetime.utcnow() - start > self.max_lock_wait:
                raise TimeoutError('waited to long for lock')
            time.sleep(self.wait_for)
        self.lock()

    def __exit__(self, type, value, traceback):
        self.unlock()

    def is_locked(self):
        pass

    def lock(self):
        pass

    def unlock(self):
        pass


class RedisLock(AbstractLock):

    def __init__(self, redis_c, lock_key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redis_c = redis_c
        self.lock_key = lock_key

    def is_locked(self):
        return self.redis_c.get(self.lock_key)

    def lock(self):
        self.redis_c.set(self.lock_key, 'IS_LOCKED', 5 * 60)  # lock for 5 min

    def unlock(self):
        return self.redis_c.delete(self.lock_key)
