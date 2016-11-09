[![Build Status](https://travis-ci.org/dolead/task_semaphore.svg?branch=master)](https://travis-ci.org/dolead/task_semaphore)
[![Code Climate](https://codeclimate.com/github/dolead/task_semaphore/badges/gpa.svg)](https://codeclimate.com/github/dolead/task_semaphore)

# Dolead Task Semaphores

The point of the Dolead Task semaphores is to control the number of tasks running in parralelle when it's difficult to control them directly (eg, a celery workflow that spawns a number of celery tasks you can't really control).

## How does it work

The main entry point is the Scheduler. That's the main object that will load the configuration, register the different slots you specified and route external signals to them (through methods like `stop`, `keepalive`. and `schedule`). The last one should be called regularly so task can be scheduled around. The scheduler can have any number of slots.

The number of tasks you can execute at the same time is controlled by the number of slots you decide to instantiate. The slots will register the state their in the storage they're linked to. The base implementation uses Redis but you can implement your own! Pull requests are welcome!

A slot can have any number of backends from which they'll retrieved a task to start. They do so by calling on the `poll` method of the backend, and if that methods returns `None`, the backend is considered as having nothing to do and the slot will poll the next one for a task.
The backend is the only object you'll have to override to use this module. Here's an example :

```python
import time
from task_semaphore import Scheduler, AbstractBackend, AbstractSlot


class MyBackend(AbstractBackend):

    def poll(self):
        # fetch the database for a task of which you return a unique id
        unique_id = my_db.get_my_task()
        return str(unique_id)

    def start_callback(self, unique_id):
        # makes so that that task won't be reached a second time
        my_db.update_my_task(unique_id)
        # the actual code that launch the task (must not be blocking)
        return launch_my_task(unique_id)


scheduler = Scheduler({'redis_c': redis_c}).load([{'backends': ['MyBackend'],
                                                   'slot_cls': 'RedisSlot',
                                                   'slot_id': 1}])

if __name__ == '__main__':
    while True:
        scheduler.schedule()
        time.sleep(60)
```

## Implementation details

The `Scheduler` object has three methods you might want to call on: *
* `schedule()`: Will browse all slots, test if the running one timeouted and start tasks for the idles.
* `keepalive(task_id)`: Send a signal that the slot working on `task_id` should be kept alive and not timeouted yet.
* `stop(task_id)`: Send a signal that the slot occupied by the task with `task_id` should be freed and that another task should be started.

Several method can be overriden in Backends, they all take a task id (as returned by `poll` for unique argument:
* `start_callback`: as shown above this one *must* be overridden since it's the one running your code. It must not be blocking.
* `stop_callback`: fired when a slots stops working on a task, either because the task is finished or because the task timeouted.
* `timeout_callback`: fired when a task timeout right before `stop_callback()` is called.
* `keepalive_callback`: fired when the scheduler receive a keepalive signal for the task running on this backend.
