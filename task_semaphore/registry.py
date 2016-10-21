REGISTRY = {}


class TaskSemaphoreMetaRegisterer(type):

    def __init__(self, name, bases, attrs):
        name = self.get_name()
        assert name not in REGISTRY, \
                "Conflicting name for %r and %r" % (self, REGISTRY[name])
        REGISTRY[name] = self
