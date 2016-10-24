import logging

from task_semaphore import AbstractSlot
from ..exceptions import TaskTimeoutError, WrongTaskIdError

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
        for slot in config:
            self.add_slot(slot['slot_id'], slot['backends'])

    def schedule(self):
        """ Schedules new tasks for available slots """
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
        for slot in self.slots.values():
            if slot.current_task_id == task_id:
                return getattr(slot, method)(task_id)
        raise WrongTaskIdError(self, task_id)

    def keepalive(self, task_id):
        """ Inform the scheduler that the task is still running
        Will reset the timeout """
        self._transmit_to_slot('keepalive', task_id)

    def stop(self, task_id):
        self._transmit_to_slot('stop', task_id)

    def add_slot(self, id_, backends=None):
        assert id_ not in self.slots, \
                "TaskSemaphore: slot with id %r already registered!" % id_
        self.slots[id_] = AbstractSlot(id_=id_, scheduler=self)
        for backend in backends:
            self.slots[id_].add_backend(backend)
