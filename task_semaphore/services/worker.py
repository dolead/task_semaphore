class Worker:

    # ID is very important, for persistence
    id = None
    # all configured backends
    backends = []
    # link to parent (scheduler)
    scheduler = []
    # related to current task
    current_backend = None
    current_task = None

    def __init__(self, id=None, backends=None):
        self.id = id
        self.backends = backends

    def set_scheduler(self, scheduler):
        self.scheduler = scheduler

    def priorize_task(self):
        pass

    def choose_one(self):
        pass

    def start(self):
        pass

    def finish(self):
        pass
