import time
import unittest

from .. import AbstractBackend, Scheduler
from .fixtures import (ExampleScheduleBackend, ExampleScheduleEmptyBackend,
                       ExampleScheduleSlot)


class BaseTestCase(unittest.TestCase):

    def get_basic_config(self):
        return [{'backends': ['AbstractBackend'],
                 'slot_cls': 'AbstractSlot',
                 'slot_id': 'sid_1'}]

    def test_load_and_dump_config(self):
        sched = Scheduler().load(self.get_basic_config())
        assert len(sched.slots) == 1
        assert 'sid_1' in sched.slots
        slot = sched.slots['sid_1']
        assert len(slot._backends)
        assert 'AbstractBackend' in slot._backends
        assert self.get_basic_config() == sched.dump()

    def test_load_and_dump_config_inst(self):
        config = self.get_basic_config()
        config[0]['backends'] = [AbstractBackend()]
        sched = Scheduler().load(config)
        assert len(sched.slots) == 1
        assert 'sid_1' in sched.slots
        slot = sched.slots['sid_1']
        assert len(slot._backends)
        assert 'AbstractBackend' in slot._backends
        assert self.get_basic_config() == sched.dump()

    def test_all_backends_are_polled(self):
        sched = Scheduler().load(
                [{'backends': ['ExampleScheduleEmptyBackend',
                               'ExampleScheduleBackend'],
                  'slot_cls': 'ExampleScheduleSlot',
                  'slot_id': 'sid_1'}])

        sched.schedule()
        slot = sched.slots['sid_1']
        assert isinstance(slot, ExampleScheduleSlot)
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
        sched = Scheduler().load(
                [{'backends': ['ExampleScheduleBackend',
                               'ExampleScheduleEmptyBackend'],
                  'slot_cls': 'ExampleScheduleSlot',
                  'slot_id': 'sid_1'}])
        sched.schedule()
        slot = sched.slots['sid_1']
        assert isinstance(slot, ExampleScheduleSlot)
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
        sched = Scheduler().load(
                [{'backends': ['ExampleScheduleEmptyBackend'],
                  'slot_cls': 'ExampleScheduleSlot',
                  'slot_id': 'sid_1'}])
        sched.schedule()
        slot = sched.slots['sid_1']
        assert isinstance(slot, ExampleScheduleSlot)
        backends = [slot._backends[bk_name]
                    for bk_name in slot._backends_names]
        assert isinstance(backends[0], ExampleScheduleEmptyBackend)
        assert slot.current_backend is None
        assert backends[0].polled == 1
        assert backends[0].started == 0
        assert slot.current_task_id is None

    def test_stop(self):
        sched = Scheduler().load(
                [{'backends': ['ExampleScheduleBackend',
                               'ExampleScheduleEmptyBackend'],
                  'slot_cls': 'ExampleScheduleSlot',
                  'slot_id': 'sid_1'}])
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
        sched = Scheduler().load(
                [{'backends': ['ExampleScheduleBackend',
                               'ExampleScheduleEmptyBackend'],
                  'slot_cls': 'ExampleScheduleSlot',
                  'slot_id': 'sid_1'}])
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
        sched = Scheduler().load(
                [{'backends': ['ExampleScheduleBackend',
                               'ExampleScheduleEmptyBackend'],
                  'slot_cls': 'ExampleScheduleSlot',
                  'slot_id': 'sid_1',
                  'slot_kwargs': {'timeout_after': 1 / 120}}])  # 1/2 second

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
        sched = Scheduler().load(
                [{'backends': ['ExampleStartRaisingBackend'],
                  'slot_cls': 'ExampleScheduleSlot',
                  'slot_id': 'sid_1'}])
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
        sched = Scheduler().load(
                [{'backends': ['ExampleKeepaliveRaisingBackend'],
                  'slot_cls': 'ExampleScheduleSlot',
                  'slot_id': 'sid_1'}])
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
        sched = Scheduler().load(
                [{'backends': ['ExampleRaisingNoToleranceBackend'],
                  'slot_cls': 'ExampleScheduleSlot',
                  'slot_id': 'sid_1'}])
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
        sched = Scheduler().load(
                [{'backends': ['ExampleDoubleRaisingBackend'],
                  'slot_cls': 'ExampleScheduleSlot',
                  'slot_id': 'sid_1'}])
        sched.schedule()
        slot = sched.slots['sid_1']
        backend = next(backend for backend in slot._backends.values())
        assert backend.polled == 1
        assert backend.started == 1
        assert backend.error_handled == 1
        # start excepted, the slot is freed
        assert slot.current_backend is None
        assert slot.current_task_id is None
