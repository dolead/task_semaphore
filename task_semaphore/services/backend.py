import logging
from ..registry import TaskSemaphoreMetaRegisterer

logger = logging.getLogger(__name__)


class AbstractBackend(metaclass=TaskSemaphoreMetaRegisterer):

    @classmethod
    def get_name(cls):
        """To override if you want your slot to have a different ref than its
        class name"""
        return cls.__name__

    def __repr__(self):
        return "<%s>" % self.get_name()

    def poll(self):  # pragma: no cover
        """Return one unique task id that should be unique accross all the
        same type of task to avoid collisions between slots.
        If not task is available for queueing, return anything false."""
        raise NotImplementedError()

    def start_callback(self, unique_task_id):  # pragma: no cover
        """This method will be called once the slot has been attributed to a
        task, that's where you should put your code to actually launch the task
        """
        raise NotImplementedError()

    def stop_callback(self, unique_task_id):  # pragma: no cover
        """This method will be called after a slot is freed by normal stopping
        or timeout, it's where you should write code if you want to add side
        effects to the slots freeing.
        """
        return NotImplemented

    def timeout_callback(self, unique_task_id):  # pragma: no cover
        """ If a slot timeouted, that callback will be triggered. """
        return NotImplemented

    def keepalive_callback(self, unique_task_id):  # pragma: no cover
        """ Triggered when a keepalive is received by the slot. """
        return NotImplemented

    def backend_error_callback(self, unique_task_id, error, method_name):
        """All callback above will be wrapped and this method will be called if
        any of them raise an error while being called. Any error occuring here
        will be ignored.

        If backend_error_callback returns something True (1, non empty string,
        et c) the slot will be freed.
        If the method raising the error is the start_callback,
        the slot will be freed.
        If backend_error_callback raise a error, the slot will be freed.
        """
        return False
