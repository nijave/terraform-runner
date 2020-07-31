import logging
import io
import queue
import select
import subprocess
import threading
import time


class NonBlockingProcess:
    """
    A process whose output can be read without blocking
    """

    OUTPUT_STREAMS = ("stdout", "stderr")

    @staticmethod
    def reader(process, attribute, output):
        """
        Reads from a process stream and queues the results
        """
        buf = bytearray(io.DEFAULT_BUFFER_SIZE)
        stream = getattr(process, attribute)
        logger = logging.getLogger(f"{__name__}.reader.{attribute}")
        while True:
            stream_available = bool(select.select([stream.fileno()], [], [], 1)[0])
            size = stream.readinto(buf)
            logger.debug("Read %i bytes", size)
            if size:
                output.put(buf[:size].decode("utf-8"))
            logger.debug("Size: %i; Available: %s", size, stream_available)
            if not size and process.poll() is not None:
                break
        logger.debug("Done reading %s", stream)

    @staticmethod
    def _read_queue(q):
        """
        Reads everything out of a queue that was present
        at the time of calling (more items may be added in the
        meantime)
        """
        sentinel = object()
        q.put(sentinel)
        for val in iter(q.get, sentinel):
            yield val

    @staticmethod
    def _check_stream_valid(stream):
        if stream not in NonBlockingProcess.OUTPUT_STREAMS:
            raise ValueError(
                f"Stream must be one of {NonBlockingProcess.OUTPUT_STREAMS}"
            )

    def __init__(self, exc, **kwargs):
        self._logger = logging.getLogger(__name__)
        self._logger.info("Starting process %s", exc)
        self._lock = threading.RLock()
        self._args = (exc, kwargs)
        self.process = subprocess.Popen(
            exc, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs
        )
        for stream in self.__class__.OUTPUT_STREAMS:
            setattr(self, stream, io.StringIO())
            setattr(self, f"_{stream}_q", queue.Queue())
            setattr(
                self,
                f"_{stream}_reader",
                threading.Thread(
                    target=self.__class__.reader,
                    args=(self.process, stream, getattr(self, f"_{stream}_q")),
                    name=f"{__name__}.reader.{stream}",
                ),
            )
            getattr(self, f"_{stream}_reader").daemon = True
            getattr(self, f"_{stream}_reader").start()

    def wait(self):
        """
        Waits for the process to finish and writes
        any output to internal stdout/stderr streams
        """
        self._logger.info("Waiting for process %s", self._args)
        while self.process.poll() is None:
            time.sleep(0.01)
        self._logger.info("Process finished. Updating streams")
        with self._lock:
            for stream in self.__class__.OUTPUT_STREAMS:
                self._logger.info(f"Waiting for {stream}")
                getattr(self, f"_{stream}_reader").join(timeout=1)
                self.read(stream=stream)
                getattr(self, stream).seek(0)

    def seek(self, position, stream="stdout"):
        """
        Changes to `position` in stream
        """
        with self._lock:
            self._check_stream_valid(stream)
            getattr(self, stream).seek(position)

    def tell(self, stream="stdout"):
        """
        Returns the current position of the stream
        """
        with self._lock:
            self._check_stream_valid(stream)
            return getattr(self, stream).tell()

    def read(self, size=-1, stream="stdout"):
        """
        Reads contents of stream by first checking for
        any enqueued values. If there is no new content,
        an empty string is returned
        """
        self._check_stream_valid(stream)
        with self._lock:
            contents = self.__class__._read_queue(getattr(self, f"_{stream}_q"))
            stream = getattr(self, stream)
            last_position = stream.tell()
            stream.seek(0, io.SEEK_END)
            for c in contents:
                stream.write(c)
            stream.seek(last_position)
            return stream.read(size)

    def readall(self, stream="stdout"):
        """
        Reads entire contents of stream.
        Position in stream remains unchanged
        """
        stream_name = stream
        self._check_stream_valid(stream_name)
        stream = getattr(self, stream_name)
        with self._lock:
            last_position = stream.tell()
            stream.seek(0)
            contents = self.read(stream=stream_name)
            stream.seek(last_position)
            return contents

    @property
    def returncode(self):
        return self.process.poll()

    @property
    def result(self):
        """
        Runs process to completion and returns results as
        (returncode, stdout, stderr)
        """
        self.wait()
        return (self.returncode, self.readall("stdout"), self.readall("stderr"))


if __name__ == "__main__":
    argv = [
        "bash",
        "-c",
        'for i in {1..100}; do echo "$THREAD_ID-$i"; echo "err$i" >/dev/stderr; sleep 0.1; done; exit 1',
    ]
    nb1 = NonBlockingProcess(argv, env={"THREAD_ID": "thread1"})
    # nb2 = NonBlockingProcess(argv, env={"THREAD_ID": "thread2"})
    # while any(nb.returncode is None for nb in (nb1, nb2)):
    #     contents = nb1.read() + nb2.read()
    #     if contents:
    #         print(contents)
    #     else:
    #         time.sleep(0.001)

    print(nb1.result)

    # nb.wait()
    # output = nb.readall()
