import logging

log = logging.getLogger(__name__)


class Operator(object):
    """ A simple operator, working on a particular package or 
    generating packages through an ingest process. """

    def __init__(self, pipeline, name, config):
        self.pipeline = pipeline
        self.name = name
        self.config = config

    def generate(self):
        pass

    def process(self, package):
        pass

    def finalize(self):
        pass

    @property
    def type(self):
        return self.__class__.__name__

    def __repr__(self):
        return '<%s(%r)>' % (self.type, self.name)


class SourceOperator(Operator):
    """ An operator which has an input resource which must exist
    in order for it to run. The resource name is given as a class
    constant, and when transforming, the resource is passed into
    a ``analyze`` function which must be subclassed. """

    DEFAULT_SOURCE = None

    def analyze(self, source):
        raise NotImplemented()

    def process(self, package):
        source_path = self.config.get('source', self.DEFAULT_SOURCE)
        if source_path is not None:
            source = package.get_resource(source_path)
        else:
            source = package.source

        if source is None:
            log.warn("No source configured for operator %r", self.type)
            return

        if not source.exists():
            log.debug("Missing source for operator %r: %r", self.type, source)
            return

        return self.analyze(source)


class TransformOperator(SourceOperator):
    """ Similar to the ``SourceOperator``, this operator transforms
    a given resource into another resource. Both resource names are
    given as constants to subclasses, and passed into the
    ``transform()`` method, which must be sub-classed. """

    DEFAULT_TARGET = None

    def transform(self, source, target):
        raise NotImplemented()

    def analyze(self, source):
        target_path = self.config.get('target', self.DEFAULT_TARGET)
        if target_path is None:
            log.error("No target for operator %r", self.type)
            return

        target = source.package.get_resource(target_path)

        if target.exists() and not self.config.get('overwrite', False):
            log.debug("Skipping operator %r: %r exists", self.type, target)
            return

        return self.transform(source, target)
