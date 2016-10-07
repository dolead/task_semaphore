class Backend:
    worker = None

    def list(self, limit=10):
        pass

    def choose_task(self):
        """ Picks the task with the highest prio, to be started """
        raise NotImplementedError()

    def start(self):
        raise NotImplementedError()

    def finish(self):
        raise NotImplementedError()
