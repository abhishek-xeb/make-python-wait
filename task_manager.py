from threading import Thread, Lock, Event
import traceback
import os
import logging
from logging.handlers import RotatingFileHandler


class TaskManager(object):
    def __init__(self, log_path=None):
        self.counter = dict()
        self.counter['counter'] = 0
        self.counter['jobs'] = list()
        self.lock = Lock()
        self.event = Event()
        self.log_path = log_path
        self.logger = logging.getLogger()
        self.task_manger_log_setup()

    def task_manger_log_setup(self):

        if not self.log_path:
            self.log_path = '/tmp/task_manager/'
        file_path = os.path.join(self.log_path, 'task_manger.txt')
        try:
            file_exist = os.path.isfile(file_path)
            if not file_exist:
                os.makedirs(self.log_path)
        except Exception as e:
            print(f"Exception creating task_manger log file {e}")

        # logger setup
        log_format = "[%(asctime)s][%(module)s %(funcName)s():%(lineno)s] %(levelname)-s %(message)s"
        formatter = logging.Formatter(log_format)
        self.logger.setLevel(logging.DEBUG)

        handler = RotatingFileHandler(file_path, maxBytes=500000, backupCount=10)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.info('Task Manager log created')

    # caller will use this method to wait until given timeout for all jobs to be processed
    def wait_on_jobs(self, timeout=5):
        msg = f'TasManager Waiting for {timeout}'
        self.logger.info(msg)
        self.event.wait(timeout)
        msg = f'Unfinished jobs {self.counter}'
        self.logger.info(msg)

    # Use this method to enqueue jobs which needs to be processed before main_thread/caller exit's
    def enqueue(self, func, param, job_id):
        # Clear event whenever new job is added.
        # This will make sure that wait_on_jobs doesn't return before time, when last set of jobs were already processed
        with self.lock:
            self.event.clear()
        msg = f'Cleared event status for job {func.__name__}_{job_id}'
        self.logger.info(msg)

        # Schedule the job
        t = TaskExecutor(func, param, job_id, self.counter, self.lock, self.event, self.logger)
        t.daemon = True
        t.start()


class TaskExecutor(Thread):
    def __init__(self, func, param, job_id, counter, lock, event, logger):
        Thread.__init__(self)
        self.func = func
        self.param = param
        self.job_id = job_id
        self.counter = counter
        self.lock = lock
        self.event = event
        self.logger = logger

    def run(self):
        self.increment_counter()

        try:
            self.func(self.param, self.job_id)
        except Exception as _:
            msg = f'Traceback: [{self.func.__name__}_{self.job_id}]: {traceback.format_exc()}'
            self.logger.error(msg)

        self.decrement_counter_and_notify()

    # Increment the job count which is being processed
    def increment_counter(self):
        with self.lock:
            self.counter['counter'] += 1
            self.counter['jobs'].append(f'{self.func.__name__}_{self.job_id}')

    # Decrement the job count which is done processing
    def decrement_counter_and_notify(self):
        with self.lock:
            self.counter['counter'] -= 1
            self.counter['jobs'].remove(f'{self.func.__name__}_{self.job_id}')

            # Notify only when job count is 0
            if self.counter['counter'] == 0:
                self.event.set()
