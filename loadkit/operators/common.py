import logging

log = logging.getLogger(__name__)


class Operator(object):

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
