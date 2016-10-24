import time
import unittest

from ..services.slot import AbstractSlot
from .. import AbstractPrioBackend, Scheduler
from .fixtures import (ExampleScheduleEmptyBackend,
                       ExampleScheduleBackend, MockStorage)


class BaseTestCase(unittest.TestCase):

    def get_basic_config(self):
        return [{'backends': ['AbstractPrioBackend'],
                 'slot_id': 'sid_1'}]

    def test_load_config(self):
        sched = Scheduler(name='test', storage=MockStorage()). \
            init_from_config(self.get_basic_config())
        assert len(sched.slots) == 1

        assert 'sid_1' in sched.slots
        slot = sched.slots['sid_1']
        assert len(slot._backends)
        assert 'AbstractPrioBackend' in slot._backends

    def test_load_and_dump_config_inst(self):
        config = self.get_basic_config()
        config[0]['backends'] = [AbstractPrioBackend()]
        sched = Scheduler(name='test', storage=MockStorage()). \
            init_from_config(config)
        assert len(sched.slots) == 1
        assert 'sid_1' in sched.slots
        slot = sched.slots['sid_1']
        assert len(slot._backends)
        assert 'AbstractPrioBackend' in slot._backends

    def test_all_backends_are_polled(self):
        config = [{'backends': ['ExampleScheduleEmptyBackend',
                               'ExampleScheduleBackend'],
                  'slot_id': 'sid_1'}]
        sched = Scheduler(name='test', storage=MockStorage()). \
            init_from_config(config)

        sched.schedule()
        slot = sched.slots['sid_1']
        assert isinstance(slot, AbstractSlot)
        backends = [slot._backends[bk_name]
                    for bk_name in slot._backends_names]
        assert isinstance(backends[0], ExampleScheduleEmptyBackend)
        assert isinstance(backends[1], ExampleScheduleBackend)
        assert slot.current_backend is backends[1]
        assert isinstance(slot.current_backend, ExampleScheduleBackend)
        assert backends[0].polled == 1
        assert backends[0].started == 0
        assert backends[1].polled == 1
        assert backends[1].started == 1
        assert slot.current_task_id == 'SELECTED_TASK_ID_1'

    def test_stop_polling_after_a_backend_responded(self):
        config = [{'backends': ['ExampleScheduleBackend',
                               'ExampleScheduleEmptyBackend'],
                  'slot_id': 'sid_1'}]
        sched = Scheduler(name='test', storage=MockStorage()). \
            init_from_config(config)
        sched.schedule()
        slot = sched.slots['sid_1']
        backends = [slot._backends[bk_name]
                    for bk_name in slot._backends_names]
        assert isinstance(backends[0], ExampleScheduleBackend)
        assert isinstance(backends[1], ExampleScheduleEmptyBackend)
        assert slot.current_backend is backends[0]
        assert isinstance(slot.current_backend, ExampleScheduleBackend)
        assert backends[0].polled == 1
        assert backends[0].started == 1
        assert backends[1].polled == 0
        assert backends[1].started == 0
        assert slot.current_task_id == 'SELECTED_TASK_ID_1'

    def test_multiple_slots(self):
        sched = self._scheduler_with_multiple_slots()
        sched.schedule()
        slot = sched.slots['sid_1']
        backends = [slot._backends[bk_name]
                    for bk_name in slot._backends_names]
        assert isinstance(backends[0], ExampleScheduleBackend)
        assert isinstance(backends[1], ExampleScheduleEmptyBackend)
        assert slot.current_backend is backends[0]
        assert isinstance(slot.current_backend, ExampleScheduleBackend)
        assert backends[0].polled == 1
        assert backends[0].started == 1
        assert backends[1].polled == 0
        assert backends[1].started == 0
        assert slot.current_task_id == 'SELECTED_TASK_ID_1'

    def test_schedule_nothing_to_do(self):
        config = [{'backends': ['ExampleScheduleEmptyBackend'],
                  'slot_id': 'sid_1'}]
        sched = Scheduler(name='test', storage=MockStorage()). \
            init_from_config(config)
        sched.schedule()
        slot = sched.slots['sid_1']
        backends = [slot._backends[bk_name]
                    for bk_name in slot._backends_names]
        assert isinstance(backends[0], ExampleScheduleEmptyBackend)
        assert slot.current_backend is None
        assert backends[0].polled == 1
        assert backends[0].started == 0
        assert slot.current_task_id is None

    def test_stop(self):
        config = [{'backends': ['ExampleScheduleBackend',
                               'ExampleScheduleEmptyBackend'],
                  'slot_id': 'sid_1'}]
        sched = Scheduler(name='test', storage=MockStorage()). \
            init_from_config(config)
        sched.schedule()
        slot = sched.slots['sid_1']
        assert slot.current_task_id == 'SELECTED_TASK_ID_1'
        assert not slot._backends['ExampleScheduleBackend'].stopped

        sched.stop('SELECTED_TASK_ID_1')
        assert slot.current_backend is None
        assert slot.current_task_id is None
        assert slot.started_at is None
        assert slot.last_keepalive_at is None
        assert slot._backends['ExampleScheduleBackend'].started == 1
        assert slot._backends['ExampleScheduleBackend'].stopped == 1

    def test_keepalive(self):
        config = [{'backends': ['ExampleScheduleEmptyBackend',
                                'ExampleScheduleBackend'],
                   'slot_id': 'sid_1'}]
        sched = Scheduler(name='test', storage=MockStorage()). \
            init_from_config(config)
        sched.schedule()
        slot = sched.slots['sid_1']
        assert slot.current_task_id == 'SELECTED_TASK_ID_1'
        assert not slot._backends['ExampleScheduleBackend'].keptalive

        last_keepalive_at = slot.last_keepalive_at
        sched.keepalive('SELECTED_TASK_ID_1')
        sched.keepalive('SELECTED_TASK_ID_1')
        assert slot.last_keepalive_at > last_keepalive_at
        assert slot.current_task_id == 'SELECTED_TASK_ID_1'
        assert slot._backends['ExampleScheduleBackend'].keptalive == 2

    def test_timeout(self):
        config = [{'backends': ['ExampleScheduleBackend',
                                'ExampleScheduleEmptyBackend'],
                  'slot_id': 'sid_1',
                  'slot_kwargs': {'timeout_after': 1 / 120}}]
        sched = Scheduler(name='test', storage=MockStorage()). \
            init_from_config(config)

        sched.schedule()
        slot = sched.slots['sid_1']
        assert slot.current_task_id == 'SELECTED_TASK_ID_1'
        assert slot._backends['ExampleScheduleBackend'].started == 1
        assert slot._backends['ExampleScheduleBackend'].stopped == 0
        assert slot._backends['ExampleScheduleBackend'].timeouted == 0

        # under timeout time so nothing should change
        sched.schedule()
        assert slot.current_task_id == 'SELECTED_TASK_ID_1'
        assert slot._backends['ExampleScheduleBackend'].started == 1
        assert slot._backends['ExampleScheduleBackend'].stopped == 0
        assert slot._backends['ExampleScheduleBackend'].timeouted == 0
        started_at = slot.started_at
        keepalive = slot.last_keepalive_at

        # task should be timeouted and started another task
        time.sleep(1)
        sched.schedule()
        assert slot._backends['ExampleScheduleBackend'].started == 2
        assert slot._backends['ExampleScheduleBackend'].stopped == 1
        assert slot._backends['ExampleScheduleBackend'].timeouted == 1
        assert slot.current_task_id == 'SELECTED_TASK_ID_2'
        assert slot.started_at > started_at
        assert slot.last_keepalive_at > keepalive

    def test_start_error_handling(self):
        config = [{'backends': ['ExampleStartRaisingBackend'],
                  'slot_id': 'sid_1'}]
        sched = Scheduler(name='test', storage=MockStorage()). \
            init_from_config(config)
        sched.schedule()
        slot = sched.slots['sid_1']
        backend = next(backend for backend in slot._backends.values())
        assert backend.polled == 1
        assert backend.started == 1
        assert backend.error_handled == 1
        # start excepted, the slot is freed
        assert slot.current_backend is None
        assert slot.current_task_id is None

    def test_keepalive_error_handling(self):
        config = [{'backends': ['ExampleKeepaliveRaisingBackend'],
                  'slot_id': 'sid_1'}]
        sched = Scheduler(name='test', storage=MockStorage()). \
            init_from_config(config)
        sched.schedule()
        slot = sched.slots['sid_1']
        backend = next(backend for backend in slot._backends.values())
        assert backend.polled == 1
        assert backend.started == 1
        assert backend.error_handled == 0
        assert slot._current_backend_name == 'ExampleKeepaliveRaisingBackend'
        assert slot.current_task_id == 'SELECTED_TASK_ID_1'

        sched.keepalive('SELECTED_TASK_ID_1')

        assert backend.polled == 1
        assert backend.started == 1
        assert backend.error_handled == 1
        assert slot._current_backend_name == 'ExampleKeepaliveRaisingBackend'
        assert slot.current_task_id == 'SELECTED_TASK_ID_1'

    def test_keepalive_no_tolerance_error_handling(self):
        config = [{'backends': ['ExampleRaisingNoToleranceBackend'],
                  'slot_id': 'sid_1'}]
        sched = Scheduler(name='test', storage=MockStorage()). \
            init_from_config(config)
        sched.schedule()
        slot = sched.slots['sid_1']
        backend = next(backend for backend in slot._backends.values())
        assert backend.polled == 1
        assert backend.started == 1
        assert backend.error_handled == 0
        assert slot._current_backend_name == 'ExampleRaisingNoToleranceBackend'
        assert slot.current_task_id == 'SELECTED_TASK_ID_1'

        sched.keepalive('SELECTED_TASK_ID_1')

        assert backend.polled == 1
        assert backend.started == 1
        assert backend.error_handled == 1
        assert slot._current_backend_name is None
        assert slot.current_task_id is None

    def test_start_double_error_handling(self):
        config = [{'backends': ['ExampleDoubleRaisingBackend'],
                  'slot_id': 'sid_1'}]
        sched = Scheduler(name='test', storage=MockStorage()). \
            init_from_config(config)
        sched.schedule()
        slot = sched.slots['sid_1']
        backend = next(backend for backend in slot._backends.values())
        assert backend.polled == 1
        assert backend.started == 1
        assert backend.error_handled == 1
        # start excepted, the slot is freed
        assert slot.current_backend is None
        assert slot.current_task_id is None

    def test_inspect(self):
        sched = self._scheduler_with_multiple_slots()
        sched.schedule()

        backends_status = sched.inspect()
        self.assertEquals(backends_status['backends'],
                          {'ExampleScheduleEmptyBackend': {},
                           'ExampleScheduleBackend': {}})
        self.assertEquals(set(backends_status['slots'].keys()),
                          {'sid_1', 'sid_2'})
        sid_1 = backends_status['slots']['sid_1']
        self.assertEquals(sid_1['_backends_names'],
                          ['ExampleScheduleBackend',
                           'ExampleScheduleEmptyBackend'])
        self.assertEquals(sid_1['_current_backend_name'],
                          'ExampleScheduleBackend')
        self.assertEquals(sid_1['_current_task_id'], 'SELECTED_TASK_ID_1')

    def _scheduler_with_multiple_slots(self):
        config = [{'backends': ['ExampleScheduleBackend',
                                'ExampleScheduleEmptyBackend'],
                   'slot_id': 'sid_1'},
                  {'backends': ['ExampleScheduleEmptyBackend',
                                'ExampleScheduleBackend'],
                   'slot_id': 'sid_2'}
                  ]
        return Scheduler(name='test', storage=MockStorage()). \
            init_from_config(config)
