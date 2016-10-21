import json
import logging

from ..exceptions import TaskTimeoutError, WrongTaskIdError
from ..registry import REGISTRY

logger = logging.getLogger(__name__)


class Scheduler:

    def __init__(self, slots_extra_params=None):
        self.slots_extra_params = slots_extra_params or {}
        self.slots = {}

    def schedule(self):
        """ Schedules new tasks for available slots """
        logger.info('starting reviewing slots for scheduling')
        for slot in self.slots.values():
            slot.read()
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

    def add_slot(self, id_, cls_name, backends=None, **kwargs):
        assert cls_name in REGISTRY, \
                "TaskSemaphore: %r is not declared !" % cls_name
        assert id_ not in self.slots, \
                "TaskSemaphore: slot with id %r already registered!" % id_
        slot_cls = REGISTRY[cls_name]
        backends_inst = []
        kwargs.update(self.slots_extra_params)
        self.slots[id_] = slot_cls(id_=id_, backends=backends_inst, **kwargs)
        for backend in backends:
            self.slots[id_].add_backend(backend)

    def load(self, config):
        for slot in config:
            self.add_slot(slot['slot_id'], slot['slot_cls'],
                          slot['backends'], **slot.get('slot_kwargs', {}))
        return self

    def dump(self):
        config = []
        for slot in self.slots.values():
            slot_conf = {'slot_cls': slot.get_name(), 'backends': [],
                         'slot_id': slot.id_}
            for backend_name in slot._backends_names:
                slot_conf['backends'].append(
                        slot._backends[backend_name].get_name())
            config.append(slot_conf)
        return config

    def write(self):  # pragma: no cover
        raise NotImplementedError('No database saving method is defined on '
                'the default Scheduler, please use the stock one '
                'or write your own')

    def read(self):  # pragma: no cover
        raise NotImplementedError('No database loading method is defined on '
                'the default Scheduler, please use the stock one '
                'or write your own')


class RedisScheduler(Scheduler):

    def __init__(self, redis_c, redis_key='task_semaphore.scheduler.config',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redis_c = redis_c
        self.redis_key = redis_key

    def write(self):
        self.redis_c.set(self.redis_key, json.dumps(self.dump()))

    def read(self):
        self.load(json.loads(self.redis_c.get(self.redis_key)))
