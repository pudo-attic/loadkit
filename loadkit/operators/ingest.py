import logging

from loadkit import Operator

log = logging.getLogger(__name__)


class IngestOperator(Operator):

    def generate(self):
        source = self.config.get('source')
        if source is None or not len(source.strip()):
            log.error('Invalid source for %s: %r', self.name, source)
        else:
            log.info('Ingesting content from %s...', source)
            meta = self.config.get('meta', {})
            self.pipeline.collection.ingest(source, meta=meta)
