import pickle
import time
from datetime import datetime, timedelta

from .plainattrs import PlainAttrs


class AbstractLock:

    def __enter__(self):
        start = datetime.now()
        while self.is_locked():
            if datetime.now() - start > self.max_lock_wait:
                raise TimeoutError('waited to long for lock')
            time.sleep(2)
        self.lock()

    def __exit__(self, type, value, traceback):
        self.unlock()

    def is_locked(self):
        pass

    def lock(self):
        pass

    def unlock(self):
        pass


class AbstractStorage(PlainAttrs):

    def __init__(self, scheduler=None):
        self.scheduler = scheduler
        self.max_lock_wait = timedelta(minutes=1)

    def lock_on(self, model):
        return AbstractLock()

    def save(self, model):  # pragma: no cover
        raise NotImplementedError()

    def reload(self, model):  # pragma: no cover
        raise NotImplementedError()


class PickleSerializer:

    def dumps(self, attrs):
        """To string"""
        return pickle.dumps(attrs)

    def loads(self, attrs_s):
        """From string"""
        return pickle.loads(attrs_s) if attrs_s else None


class RedisLock(AbstractLock):

    def __init__(self, redis_c, lock_key):
        self.redis_c = redis_c
        self.lock_key = lock_key

    def is_locked(self):
        return self.redis_c.get(self.lock_key)

    def lock(self):
        self.redis_c.set(self.lock_key, 'IS_LOCKED', 5 * 60)  # lock for 5 min

    def unlock(self):
        return self.redis_c.delete(self.lock_key)


class RedisStorage(AbstractStorage, PickleSerializer):
    """Slot with a redis backend"""

    def __init__(self, redis_c, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redis_c = redis_c

    def lock_on(self, model):
        return RedisLock(self.redis_c, self._db_key(model, 'lock'))

    def save(self, model):
        serialized = self.dumps(model.to_plain())
        return self.redis_c.set(self._db_key(model), serialized)

    def reload(self, model):
        serialized = self.redis_c.get(self._db_key(model))
        attrs = self.loads(serialized) or {}
        model.from_plain(attrs)

    def _db_key(self, model, *args):
        return "task_semaphore.%s" % ".".join(model._storage_key + args)
