import pickle
from .plainattrs import PlainAttrs


class AbstractStorage(PlainAttrs):
    def save(self, context, model):
        raise NotImplementedError()

    def reload(self, context, model):
        raise NotImplementedError()


class PickleSerializer:

    def dumps(self, attrs):
        """To string"""
        return pickle.dumps(attrs)

    def loads(self, attrs_s):
        """From string"""
        return pickle.loads(attrs_s)


class RedisStorage(AbstractStorage, PickleSerializer):
    """Slot with a redis backend"""

    redis_c = None

    def __init__(self, redis_c):
        self.redis_c = redis_c

    def save(self, context, model):
        serialized = self.dumps(model.to_plain())
        return self.redis_c.set(self._db_key(context, model), serialized)

    def reload(self, context, model):
        serialized = self.redis_c.get(self._db_key(context, model))
        attrs = self.loads(serialized)
        model.set_attrs(self, attrs)

    def _db_key(self, context, model):
        prefix = ".".join(context)
        return "task_semaphore.%s.%s" % (prefix, model.id_)
