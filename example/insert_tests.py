
import logging
from random import shuffle
import time
import uuid

import webapp2

from furious.async import Async
from furious import context
from furious.context.context import _insert_tasks
from furious.context.context import _insert_tasks_all_async
#from furious.context.context import _insert_tasks_batched_async
from furious.context.context import _insert_tasks_async_on_fail
from furious.context.context import (
    _insert_tasks_batched_async_with_all_async_on_fail)


class InsertTestsHandler(webapp2.RequestHandler):
    """Demonstrate the creation and insertion of a single furious task."""
    def get(self):
        n = int(self.request.get('n', 1))
        fail = bool(self.request.get('fail', 0))

        Async(all_tests, [n, fail]).start()

        self.response.out.write('kicked off test, see logs')
        self.response.out.write('<br />%s %s' % (n, fail))


def all_tests(n, fail):
    if fail:
        task_name = 'insert-test-%s' % uuid.uuid4().hex
        task_names = ['%s-%s' % (task_name, i) for i in xrange(n)]
        logging.info('inserting tasks to take tasksnames')
        for task_name in task_names:
            Async(example_function, [1], task_args={'name': task_name})

    def run_test(f):
        with context.new(insert_tasks=f) as ctx:
            for i in xrange(n):
                task_args = {}
                if fail:
                    task_args.update(name=('%s-%s' % (task_name, i)))
                ctx.add(example_function, [1], task_args=task_args)
            start = time.time()
        end = time.time()
        logging.info('%s time to insert %s tasks: %s\n'
                     % (f.__name__, n, str(end - start)))

    fs = [_insert_tasks,
          _insert_tasks_all_async,
          #_insert_tasks_batched_async,
          _insert_tasks_async_on_fail,
          _insert_tasks_batched_async_with_all_async_on_fail]
    shuffle(fs)
    for f in fs:
        run_test(f)


def example_function(*args, **kwargs):
    """This function is called by furious tasks to demonstrate usage."""
    logging.info('example_function executed with args: %r, kwargs: %r',
                 args, kwargs)

    return args

