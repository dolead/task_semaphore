import time
import unittest

import redis

from ..utils.storage import RedisStorage
from .. import Scheduler
from .fixtures import (ExampleScheduleEmptyBackend,
                       ExampleScheduleBackend)


class StorageTstMixin:
    def _storage(self):
        raise NotImplementedError()

    def test_stores_and_reloads(self):
        self._clean()

        config = [{'backends': ['ExampleScheduleBackend',
                                'ExampleScheduleEmptyBackend'],
                   'slot_id': 'sid_1'}]
        sched_before = Scheduler(name='test', storage=self._storage()). \
            init_from_config(config)
        sched_before.schedule()

        sched = Scheduler(name='test', storage=self._storage()). \
            init_from_config(config)
        slot = sched.slots['sid_1']
        backends = [slot._backends[bk_name]
                    for bk_name in slot._backends_names]
        assert isinstance(backends[0], ExampleScheduleBackend)
        assert isinstance(backends[1], ExampleScheduleEmptyBackend)
        assert slot.current_backend is backends[0]
        assert isinstance(slot.current_backend, ExampleScheduleBackend)
        assert slot.current_task_id == 'SELECTED_TASK_ID_1'
        assert slot._last_keepalive_at is not None
        assert slot._started_at is not None

    def _clean(self):
        pass


class RedisStorageTest(unittest.TestCase, StorageTstMixin):
    """Integration test for redis store"""

    @property
    def _redis(self):
        return redis.StrictRedis(host='localhost', port=6379, db=0)

    def _clean(self):
        self._redis.flushdb()

    def _storage(self):
        # FIXME: add this to config
        return RedisStorage(self._redis)