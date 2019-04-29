import logging
from queue import Queue
from threading import Thread
from urllib.request import Request, urlopen
from abc import abstractmethod, ABCMeta

from nameko.extensions import SharedExtension
from py_zipkin.transport import BaseTransportHandler
from nameko_zipkin.constants import *

logger = logging.getLogger('nameko-zipkin')


class IHandler(metaclass=ABCMeta):
    def start(self):
        pass

    def stop(self):
        pass

    @abstractmethod
    def handle(self, encoded_span):
        pass


class HttpHandler(IHandler):
    def __init__(self, url):
        self.url = url
        self.queue = Queue()
        self.thread = None

    def start(self):
        self.thread = Thread(target=self._poll)
        self.thread.start()

    def stop(self):
        self.queue.put(StopIteration)
        self.thread.join()

    def handle(self, encoded_span):
        body = b'\x0c\x00\x00\x00\x01' + encoded_span
        request = Request(self.url, body, {'Content-Type': 'application/x-thrift'}, method='POST')
        try:
            urlopen(request)
        except:
            logger.error('Exception handling span', exc_info=True)

    def _poll(self):
        while True:
            span = self.queue.get()
            if span == StopIteration:
                break
            self.handle(span)


class Transport(SharedExtension):
    def __init__(self):
        self._handler = None

    def setup(self):
        config = self.container.config[ZIPKIN_CONFIG_SECTION]
        handler_cls = globals()[config[HANDLER_KEY]]
        handler_params = config[HANDLER_PARAMS_KEY]
        self._handler = handler_cls(**handler_params)

    def start(self):
        self._handler.start()

    def stop(self):
        self._handler.stop()

    def handle(self, encoded_span):
        self._handler.handle(encoded_span)
#@TODO create custom transport
class HttpTransport(BaseTransportHandler):

    def get_max_payload_bytes(self):
        return None

    def send(self, encoded_span):
        # The collector expects a thrift-encoded list of spans.
        response = requests.post(
            'http://localhost:9411/api/v1/spans',
            data=encoded_span,
            headers={'Content-Type': 'application/x-thrift'},
        )
        return response