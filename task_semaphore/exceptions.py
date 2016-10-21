class TaskSemaphoreError(Exception):
    """Base exceptions used for global catching"""
    pass


class WrongTaskIdError(TaskSemaphoreError):

    def __init__(self, task_handler, task_id):
        super().__init__("%r is unknown to %r" % (task_id, task_handler))


class TaskTimeoutError(TaskSemaphoreError, TimeoutError):

    def __init__(self, slot):
        super().__init__("%r on %r timeouted for %r" % (
                slot.current_task_id, slot.current_backend, slot))
