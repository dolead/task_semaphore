from .. import AbstractPrioBackend
from ..utils.storage import AbstractStorage


class MockStorage(AbstractStorage):
    def save(self, model):
        pass

    def reload(self, model):
        pass


class ExampleBackend(AbstractPrioBackend):
    def __init__(self):
        self.polled, self.started, self.stopped = 0, 0, 0
        self.timeouted, self.keptalive, self.error_handled = 0, 0, 0

    def poll(self):
        self.polled += 1

    def start_callback(self, unique_task_id):
        self.started += 1

    def stop_callback(self, unique_task_id):
        self.stopped += 1

    def timeout_callback(self, unique_task_id):
        self.timeouted += 1

    def keepalive_callback(self, unique_task_id):
        self.keptalive += 1

    def backend_error_callback(self, unique_task_id, error, method):
        self.error_handled += 1


class ExampleScheduleEmptyBackend(ExampleBackend):
    pass


class ExampleScheduleBackend(ExampleBackend):
    def poll(self):
        self.polled += 1
        return 'SELECTED_TASK_ID_%d' % self.polled


class ExampleStartRaisingBackend(ExampleScheduleBackend):
    def start_callback(self, unique_task_id):
        super().start_callback(unique_task_id)
        raise ZeroDivisionError()


class ExampleKeepaliveRaisingBackend(ExampleScheduleBackend):
    def keepalive_callback(self, unique_task_id):
        super().keepalive_callback(unique_task_id)
        raise ZeroDivisionError()


class ExampleRaisingNoToleranceBackend(ExampleKeepaliveRaisingBackend):

    def backend_error_callback(self, unique_task_id, error, method):
        super().backend_error_callback(unique_task_id, error, method)
        return True


class ExampleDoubleRaisingBackend(ExampleStartRaisingBackend):

    def backend_error_callback(self, unique_task_id, error, method):
        super().backend_error_callback(unique_task_id, error, method)
        raise ZeroDivisionError()
