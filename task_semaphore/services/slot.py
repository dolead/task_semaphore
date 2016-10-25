import logging
from datetime import datetime, timedelta

from ..exceptions import TaskTimeoutError, WrongTaskIdError
from ..registry import REGISTRY
from .prio_backend import AbstractPrioBackend
from ..utils.plainattrs import PlainAttrs

logger = logging.getLogger(__name__)


class AbstractSlot(PlainAttrs):

    scheduler = None

    KEYS_TO_SERIALIZE = ('_current_task_id',
                         '_backends_names', '_current_backend_name',
                         '_started_at', '_last_keepalive_at')

    def __init__(self, id_, scheduler, backends=None, timeout_after=60):
        self.id_ = id_
        self.scheduler = scheduler
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
        """Add a single backend. backend can be either the name of a registered
        backend or directly an instance of backend.
        """
        if isinstance(backend, str):
            assert backend in REGISTRY, \
                    "TaskSemaphore: %r is not a registered backend!" % backend
            BackendCls = REGISTRY[backend]
            backend = BackendCls()
        assert isinstance(backend, AbstractPrioBackend), "TaskSemaphore: " \
                "%r is no AbstractBackend subclass instance" % backend
        backend_name = backend.get_name()
        self._backends_names.append(backend_name)
        self._backends[backend_name] = backend

    def __repr__(self):
        return "<%s id=%r>" % (self.get_name(), self.id_)

    def poll(self):
        """ Will poll each associated backend in the order they've been added.
        Will stop after the first task id retrieved this way.

        The task id must be unique across all the backends of a single
        scheduler.
        """
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
        """ `method` being the name of a backend method, will call that method
        and wrap it so it triggers that backend `backend_error_callback` if the
        method raises something.

        If the error handling callback also raises something, it'll be ignored.
        See `AbstractBackend.backend_error_callback`.
        """
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
        logger.debug('bumping keepalive %r(%s)', self, unique_task_id)
        self._last_keepalive_at = datetime.utcnow()
        self.backend_method_wrapper('keepalive_callback')
        self.save()

    def start(self, unique_task_id, backend):
        """Will start the task with `unique_task_id`, meaning, will make so
        this slot isn't free anymore.

        Will set `started_at` and `last_keepalive_at` to now. Will set the
        `current_backend` and `current_task` to the value passed in the args.

        Will call the backend's `start_callback`.
        """
        self._current_task_id = unique_task_id
        self._current_backend_name = backend.get_name()
        self._started_at = datetime.utcnow()
        self._last_keepalive_at = datetime.utcnow()
        logger.warn('starting %r(%s)', self, unique_task_id)
        self.backend_method_wrapper('start_callback')
        self.save()

    def _free_slot(self):
        self._current_task_id = None
        self._current_backend_name = None
        self._started_at = None
        self._last_keepalive_at = None
        self.save()

    def stop(self, unique_task_id):
        """Will stop the task with `unique_task_id`, meaning, will make so
        this slot is free.

        Will set `started_at` and `last_keepalive_at`, `current_backend` and
        `current_task` to None.

        Will call the backend's `stop_callback`.
        """
        if self.current_task_id != unique_task_id:
            raise WrongTaskIdError(self, unique_task_id)
        logger.warn('stopping %r(%s)', self, unique_task_id)
        self.backend_method_wrapper('stop_callback')
        self._free_slot()

    @property
    def storage(self):
        return self.scheduler.storage

    @property
    def _storage_context(self):
        return [self.scheduler.id_, "slot", self.id_]

    def save(self):
        self.storage.save(self._storage_context, self)

    def reload(self):
        self.storage.reload(self._storage_context, self)
