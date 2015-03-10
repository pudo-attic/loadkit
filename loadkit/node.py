import logging

from loadkit.util import ConfigException
from loadkit.operators import load_operator

log = logging.getLogger(__name__)


def resolve_dependencies(nodes):
    """ Figure out which order the nodes in the graph can be executed
    in to satisfy all requirements. """
    done = set()
    while True:
        if len(done) == len(nodes):
            break
        for node in nodes:
            if node.name not in done:
                match = done.intersection(node.requires)
                if len(match) == len(node.requires):
                    done.add(node.name)
                    yield node
                    break
        else:
            raise ConfigException('Invalid requirements in pipeline!')


class Node(object):

    def __init__(self, pipeline, name, config):
        self.name = name
        self.config = config
        self.pipeline = pipeline
        self._operator = None
        self._requires = None

    @property
    def operator(self):
        if self._operator is None:
            op_name = self.config.get('operator')
            operator = load_operator(op_name)
            self._operator = operator(self.pipeline, self.name, self.config)
        return self._operator

    def generate(self):
        log.debug("Running extract: %s (%s)" % (self.name, self.operator.type))
        self.operator.generate()

    def process(self, package):
        log.debug("Running transform: %s (%s) on %r" %
                  (self.name, self.operator.type, package))
        self.operator.process(package)

    def finalize(self):
        self.operator.finalize()

    @property
    def requires(self):
        if self._requires is None:
            reqs = self.config.get('requires', [])
            if reqs is None:
                reqs = []
            if not isinstance(reqs, (list, tuple, set)):
                reqs = [unicode(reqs)]
            self._requires = set(reqs)
        return self._requires

    def __repr__(self):
        return '<Node(%r, %r)>' % (self.name, self.operator.type)
