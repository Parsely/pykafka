__license__ = """
Copyright 2012 DISQUS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import atexit

from collections import namedtuple
from Queue import Queue
import threading


class ResponseFuture(object):
    """A samsa response which may have a value at some point."""

    def __init__(self, handler):
        self.handler = handler
        self.error = False
        self._ready = handler.Event()

    def set_response(self, response):
        """Set response data and trigger get method."""
        self.response = response
        self._ready.set()

    def set_error(self, error):
        """Set error and trigger get method."""
        self.error = error
        self._ready.set()

    def get(self, timeout=None):
        """Block until data is ready and return.

        Raises exception if there was an error.

        """
        self._ready.wait(timeout)
        if self.error:
            raise self.error
        return self.response


class Handler(object):

    def spawn(self, target, *args, **kwargs):
        raise NotImplementedError


# Could be possible to use the Kazoo handlers. Our RequestHandler is already
# implementation agnostic.
class ThreadingHandler(Handler):

    Queue = Queue
    Event = threading.Event

    def spawn(self, target, *args, **kwargs):
        t = threading.Thread(target=target, *args, **kwargs)
        t.daemon = True
        t.start()
        return t


class RequestHandler(object):
    """Uses a worker thread to dispatch requests."""

    Task = namedtuple('Task', ['request', 'future'])

    def __init__(self, handler, connection):
        self.handler = handler
        self.connection = connection
        self._requests = handler.Queue()
        self.ending = handler.Event()
        atexit.register(self.stop)

    def request(self, request, has_response=True):
        """Construct a new requst

        :returns: :class:`samsa.handlers.ResponseFuture`

        """
        future = None
        if has_response:
            future = ResponseFuture(self.handler)

        task = self.Task(request, future)
        self._requests.put(task)
        return future

    def start(self):
        """Start the request processor."""
        self.t = self._start_thread()

    def stop(self):
        """Stop the request processor."""
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

        return self.handler.spawn(worker)
