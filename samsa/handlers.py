import atexit

from collections import namedtuple
from Queue import Queue
from threading import Event, Thread


class ResponseFuture(object):
    """A samsa response which may have a value at some point."""

    def __init__(self):
        self.error = False
        self._ready = Event()

    def set_response(self, response):
        """Set response data and trigger get method."""
        self.response = response
        self._ready.set()

    def set_error(self, error):
        """Set error and trigger get method."""
        self.error = error
        self._ready.set()

    def get(self):
        """Block until data is ready and return.

        Raises exception if there was an error.

        """
        self._ready.wait()
        if self.error:
            raise self.error
        return self.response


class RequestHandler(object):
    """Interface for request handlers."""

    def __init__(self, connection):
        self.connection = connection

    def start(self):
        """Start the request processor."""
        raise NotImplementedError

    def stop(self):
        """Stop the request processor."""
        raise NotImplementedError

    def request(self):
        """Construct a new requst

        :returns: :class:`samsa.client.ResponseFuture`

        """
        raise NotImplementedError


class ThreadedRequestHandler(RequestHandler):
    """Uses a worker thread to dispatch requests."""

    Task = namedtuple('Task', ['request', 'future'])

    def __init__(self, connection):
        super(ThreadedRequestHandler, self).__init__(connection)
        self._requests = Queue()
        self.ending = Event()
        atexit.register(self.stop)

    def request(self, request, has_response=True):
        future = None
        if has_response:
            future = ResponseFuture()

        task = self.Task(request, future)
        self._requests.put(task)
        return future

    def start(self):
        self.t = self._start_thread()

    def stop(self):
        self._requests.join()
        self.ending.set()

    def _start_thread(self):
        def worker():
            while not self.ending.is_set():
                task = self._requests.get()
                try:
                    self.connection.request(task.request)
                    if task.future:
                        self.connection.response(task.future)
                except Exception, e:
                    if task.future:
                        task.future.set_error(e)
                finally:
                    self._requests.task_done()

        t = Thread(target=worker)
        t.daemon = True
        t.start()
        return t
