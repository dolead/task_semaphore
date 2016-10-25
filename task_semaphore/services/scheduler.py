import logging

from ..exceptions import TaskTimeoutError, WrongTaskIdError
from .slot import AbstractSlot

logger = logging.getLogger(__name__)


class Scheduler:

    id_ = None
    config = None
    slots = {}
    storage = None

    KEYS_TO_SERIALIZE = ('config', )

    def __init__(self, name, storage):
        self.id_ = name
        self.storage = storage
        self.slots = {}

    def init_from_config(self, config):
        # TODO: config should be auto-loaded from db
        self.config = config
        for slot_config in config:
            slot = self.add_slot(slot_config['slot_id'],
                                 slot_config['backends'],
                                 slot_config.get('slot_kwargs'))
            slot.reload()
        return self

    def schedule(self):
        """ Schedules new tasks for available slots """
        with self.storage.lock_on(self):
            logger.info('starting reviewing slots for scheduling')
            for slot in self.slots.values():
                slot.reload()
                if slot.current_task_id:
                    logger.debug('slot %s is busy', slot)
                    try:
                        slot.timeout_if_late(slot.current_task_id)
                    except TaskTimeoutError:
                        slot.stop(slot.current_task_id)
                    else:  # if not timeouted
                        continue
                task_id, backend = slot.poll()
                if task_id is not None:
                    slot.start(task_id, backend)
                else:
                    logger.debug('nothing to do for slot %r', slot)

    def _transmit_to_slot(self, method, task_id):
        with self.storage.lock_on(self):
            for slot in self.slots.values():
                if slot.current_task_id == task_id:
                    logger.debug('passing %r to %r(%r)', method, slot, task_id)
                    return getattr(slot, method)(task_id)
        raise WrongTaskIdError(self, task_id)

    def keepalive(self, task_id):
        """ Inform the scheduler that the task is still running
        Will reset the timeout """
        self._transmit_to_slot('keepalive', task_id)

    def stop(self, task_id):
        """Inform the scheduler that the task with task_id is finished and that
        its slot should be freed.
        """
        self._transmit_to_slot('stop', task_id)

    def add_slot(self, id_, backends=None, slot_kwargs=None):
        """Add a single slot with an id_ that
        hasn't been yet registered (unique).
        `backends` must be a list of registered backends. See Slot.add_backend.
        `slot_kwargs` are the kwargs you want to pass on to the soon to be
        instantiated backends.
        """
        assert id_ not in self.slots, \
                "TaskSemaphore: slot with id %r already registered!" % id_
        if not slot_kwargs:
            slot_kwargs = {}
        self.slots[id_] = AbstractSlot(id_=id_, scheduler=self, **slot_kwargs)
        for backend in backends:
            self.slots[id_].add_backend(backend)
        return self.slots[id_]

    @property
    def _all_backends(self):
        uniq_backends = {}
        for slot in self.slots.values():
            for backend_id, backend in slot._backends.items():
                uniq_backends[backend.get_name()] = backend
        return uniq_backends

    def inspect(self):
        # TODO: more generic plainify
        return {
            'slots': {slot_id: slot.to_plain()
                      for slot_id, slot in self.slots.items()},
            'backends': {backend_id: backend.inspect()
                         for backend_id, backend in self._all_backends.items()}
        }

    @property
    def _storage_key(self):
        return "scheduler", str(self.id_)
