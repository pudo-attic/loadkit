import time
import logging
import multiprocessing
try:
    from queue import Queue
except ImportError:
    from Queue import Queue
from threading import Thread

from loadkit.node import Node, resolve_dependencies

GENERATE = 'generate'
PROCESS = 'process'

log = logging.getLogger(__name__)


class Pipeline(object):
    """ A pipeline is defined by a set of operators which are
    executed in a given sequence based on their mutual
    dependencies. The whole pipeline consists of three main 
    phases: one, in which packages are generated, the second,
    in which packages are transformed, and the third, in which
    final tasks are performed. """

    def __init__(self, collection, name, config=None):
        self.config = dict()
        self.collection = collection
        self.name = name

        if config is not None:
            self.config.update(config)

        self.threads = config.get('config', {}).get('threads')
        if self.threads is None:
            self.threads = multiprocessing.cpu_count() * 2

        self._nodes = None

        self._queue = None

    @property
    def queue(self):
        if self._queue is None:
            self._queue = Queue(maxsize=self.threads * 100)
            for i in range(self.threads):
                thread = Thread(target=self._process_packages)
                thread.daemon = True
                thread.start()
        return self._queue

    @property
    def nodes(self):
        if self._nodes is None:
            self._nodes = {GENERATE: [], PROCESS: []}

            for phase in self._nodes.keys():
                for name, config in self.config.get(phase, {}).items():
                    base = self.config.get('config', {}).copy()
                    base.update(config)
                    node = Node(self, name, base)
                    self._nodes[phase].append(node)
        return self._nodes

    def generate(self):
        for node in self.nodes[GENERATE]:
            node.generate()

    def process(self):
        for package in self.collection:
            self.queue.put(package)

        try:
            while True:
                if self.queue.empty():
                    break
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass

    def process_sync(self):
        for package in self.collection:
            self.process_package(package)

    def process_package(self, package):
        try:
            for node in resolve_dependencies(self.nodes[PROCESS]):
                node.process(package)
        except Exception, e:
            log.exception(e)

    def _process_packages(self):
        while True:
            try:
                package = self.queue.get(True)
                self.process_package(package)
            finally:
                self.queue.task_done()

    def finalize(self):
        for phase, nodes in self.nodes.items():
            for node in nodes:
                node.finalize()

    def run(self):
        self.generate()
        self.process()
        self.finalize()

    def __repr__(self):
        return "<Pipeline(%r)>" % self.name
