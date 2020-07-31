import concurrent
import logging
import multiprocessing
import queue
import threading
import time

from .process import NonBlockingProcess


class FutureNonBlockingProcess:
    LOGGER = logging.getLogger(__name__)

    def __init__(self):
        self._future = concurrent.futures.Future()
        FutureNonBlockingProcess.LOGGER.debug("Created future %s", self._future)

    def set_result(self, obj):
        FutureNonBlockingProcess.LOGGER.debug("Setting future result %s", obj)
        self._future.set_result(obj)

    def __getattr__(self, item):
        FutureNonBlockingProcess.LOGGER.debug("Getting attribute %s", item)
        if item in ("_future", "__dict__"):
            return self.__getattribute__(item)
        return getattr(self._future.result(), item)

    def __setattr__(self, prop, value):
        FutureNonBlockingProcess.LOGGER.debug("Setting attribute %s to %s", prop, value)
        if prop == "_future":
            self.__dict__[prop] = value
            return
        return setattr(self._future.result(), prop, value)


class Pool:
    # TODO move finished work to done queue so the results can be grabbed
    @staticmethod
    def _manage_thread_pool(pool, jobs, shutdown):
        logger = logging.getLogger(__name__)
        logger.debug("Thread manager running")
        while not shutdown.is_set():
            logger.debug("Attempting to get job")
            try:
                job = jobs.get(timeout=1)
            except queue.Empty:
                logger.debug("No jobs available")
                continue

            scheduled = False
            logger.debug("Looking for available pool slot")
            while not scheduled:
                for i in range(len(pool)):
                    if pool[i] is None or pool[i].process.poll() is not None:
                        logger.info("Found available pool slot %i. Scheduling job", i)
                        pool[i] = NonBlockingProcess(*job[1], **job[2])
                        job[0].set_result(pool[i])
                        scheduled = True
                        break
                if not scheduled:
                    time.sleep(0.01)

    def __init__(self, workers=None):
        self._logger = logging.getLogger(__name__)
        self.worker_count = workers or multiprocessing.cpu_count()
        self._logger.info("Creating with %i workers", self.worker_count)

        self.pool = [None] * self.worker_count
        self.job_queue = queue.Queue()
        self._shutdown = threading.Event()
        self._logger.debug("Creating manager thread")
        self._manager = threading.Thread(
            target=self._manage_thread_pool,
            args=(self.pool, self.job_queue, self._shutdown),
        )
        self._logger.info("Starting manager thread")
        self._manager.start()

    def queue(self, *args, **kwargs):
        future = FutureNonBlockingProcess()
        self._logger.info("Enqueueing job (%s, %s)", args, kwargs)
        self.job_queue.put((future, args, kwargs))
        return future

    def shutdown(self):
        self._logger.debug("Setting pool to shutdown")
        self._shutdown.set()
        self._logger.debug("Waiting on manager thread")
        self._manager.join()
        for p in self.pool:
            try:
                self._logger.debug("Waiting on %s", p)
                p.wait()
            except:
                pass
        self._logger.debug("Shutdown complete")


if __name__ == "__main__":
    # from non_blocking_process.pool import Pool
    pool = Pool()
    for _ in range(int(pool.worker_count * 2.5)):
        pool.queue(["sleep", "60"])
    pool.shutdown()
