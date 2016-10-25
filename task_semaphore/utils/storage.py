import pickle

from .plainattrs import PlainAttrs
from .lock import AbstractLock, RedisLock


class AbstractStorage(PlainAttrs):

    def __init__(self, scheduler=None):
        self.scheduler = scheduler

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
