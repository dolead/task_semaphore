from .utils import with_exclusive_access


class AbstractScheduler:
    workers = {}

    def __init__(self):
        self._load_default_config()

    def add_worker(self, worker):
        worker.set_scheduler(self)
        self.workers[worker.id] = worker

    def schedule(self):
        """ Schedules new tasks for available workers """
        pass

    @with_exclusive_access
    def task_started(self, task_id, metadata=None):
        """ External signal to inform the scheduler that the task has started """
        pass

    @with_exclusive_access
    def task_finished(self, task_id, metadata=None):
        """ External signal to inform the scheduler that the task has finished """
        pass

    def _load_default_config(self):
        raise NotImplementedError()

    def _refresh_worker_status(self):
        pass
