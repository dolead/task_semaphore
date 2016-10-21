import logging
from datetime import datetime, timedelta

from ..exceptions import TaskTimeoutError, WrongTaskIdError
from ..registry import REGISTRY, TaskSemaphoreMetaRegisterer
from .backend import AbstractBackend

logger = logging.getLogger(__name__)


class AbstractSlot(metaclass=TaskSemaphoreMetaRegisterer):

    def __init__(self, id_, backends=None, timeout_after=60):
        self.id_ = id_
        self.timeout_after = timedelta(minutes=timeout_after)

        # internal value init
        self._current_task_id = None
        self._current_backend_name = None
        self._started_at = None
        self._last_keepalive_at = None

        # we have to keep the order of backends since it matters for polling
        self._backends_names = []
        self._backends = {}
        for backend in backends or []:
            self.add_backend(backend)

    @classmethod
    def get_name(cls):
        """To override if you want your slot to have a different ref than its
        class name"""
        return cls.__name__

    def add_backend(self, backend):
        if isinstance(backend, str):
            assert backend in REGISTRY, \
                    "TaskSemaphore: %r is not a registered backend!" % backend
            BackendCls = REGISTRY[backend]
            backend = BackendCls()
        assert isinstance(backend, AbstractBackend), "TaskSemaphore: " \
                "%r is no AbstractBackend subclass instance" % backend
        backend_name = backend.get_name()
        self._backends_names.append(backend_name)
        self._backends[backend_name] = backend

    def __repr__(self):
        return "<%s id=%r>" % (self.get_name(), self.id_)

    def write(self):  # pragma: no cover
        """Write current state to the database"""
        raise NotImplementedError()

    def read(self):  # pragma: no cover
        """Reads from a database the state of that slot based on its self.id

        Should set self._current_task, self._current_backend_name,
        self._started_at, and self._last_keepalive_at
        """
        raise NotImplementedError()

    def poll(self):
        logger.info('polling for slot %r', self)
        for backend_name in self._backends_names:
            task_id = self._backends[backend_name].poll()
            if task_id:
                return task_id, self._backends[backend_name]
        return None, None

    @property
    def current_task_id(self):
        """Read only, task will be updated after polling backends"""
        return self._current_task_id

    @property
    def current_backend(self):
        """Read only, the current backend if slot is running else None"""
        if not self._current_backend_name:
            return
        return self._backends[self._current_backend_name]

    @property
    def started_at(self):
        return self._started_at

    @property
    def last_keepalive_at(self):
        return self._last_keepalive_at

    def backend_method_wrapper(self, method):
        try:
            return getattr(self.current_backend, method)(self.current_task_id)

        except Exception as error:
            free_slot = False
            try:
                logger.warn('something bad happend while calling %r: %r(%s), '
                            'calling error callback: %r', self.current_backend,
                            method, self.current_task_id, error)
                free_slot = self.current_backend.backend_error_callback(
                        self.current_task_id, error, method)
            except Exception:
                logger.exception('an error occured while calling '
                                 'on error handler, ignoring, freeing slot:')
                free_slot = True
            if free_slot or method == 'start_callback':
                logger.warn('backend_error_callback returned True, '
                            'freeing slot')
                self._free_slot()

    def timeout_if_late(self, unique_task_id):
        """ Based on configured self.timeout_after will decide whether or not
        the task is dead.
        If so, will notify current backend current task is timeouted and will
        mark itself as idle in the database"""
        if self.current_task_id != unique_task_id:
            raise WrongTaskIdError(self, unique_task_id)
        deadline = self._last_keepalive_at + self.timeout_after
        if deadline < datetime.utcnow():
            logger.warn('Deadline was %s (last keep alive on %s) for %s. '
                        'Timeouting', deadline, self._last_keepalive_at, self)
            self.backend_method_wrapper('timeout_callback')
            raise TaskTimeoutError(self)

    def keepalive(self, unique_task_id):
        """ Supposing the running task is the task with the unique_task_id
        will refresh its timeout time
        """
        if self.current_task_id != unique_task_id:
            raise WrongTaskIdError(self, unique_task_id)
        self._last_keepalive_at = datetime.utcnow()
        self.backend_method_wrapper('keepalive_callback')
        self.write()

    def start(self, unique_task_id, backend):
        self._current_task_id = unique_task_id
        self._current_backend_name = backend.get_name()
        self._started_at = datetime.utcnow()
        self._last_keepalive_at = datetime.utcnow()
        self.backend_method_wrapper('start_callback')
        self.write()

    def _free_slot(self):
        self._current_task_id = None
        self._current_backend_name = None
        self._started_at = None
        self._last_keepalive_at = None
        self.write()

    def stop(self, unique_task_id):
        if self.current_task_id != unique_task_id:
            raise WrongTaskIdError(self, unique_task_id)
        self.backend_method_wrapper('stop_callback')
        self._free_slot()


class RedisSlot(AbstractSlot):
    """Slot with a redis backend"""

    def __init__(self, redis_c, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redis_c = redis_c
        self._db_datas = {}
        self._keys = ('current_task_id', 'current_backend_name',
                      'started_at', 'last_keepalive_at')

    def _to_db_key(self, subkey):
        return "task_semaphore.slots.%s.%s" % (self.id_, subkey)

    def _to_db_value(self, subkey):
        value = getattr(self, "_%s" % subkey)
        if hasattr(value, 'isoformat'):
            return value.isoformat()
        elif value:
            return str(value)
        return None

    def write(self):
        return self.redis_c.mset({self._to_db_key(key): self._to_db_value(key)
                                  for key in self._keys})

    def read(self):
        rkeys = [self._to_db_key(key) for key in self._keys]
        raw = dict(zip(self._keys,
                       [val.decode('utf8') if hasattr(val, 'decode') else val
                        for val in self.redis_c.mget(rkeys)]))
        for key, value in raw.items():
            if key in {"started_at", "last_keepalive_at"}:
                value = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f")
            setattr(self, "_%s" % key, value)
