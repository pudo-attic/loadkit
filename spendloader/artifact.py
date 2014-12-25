import json
import logging
import tempfile
from contextlib import contextmanager


log = logging.getLogger(__name__)


class Artifact(object):
    """ The artifact holds a temporary, cleaned representation of the
    package resource (as a newline-separated set of JSON
    documents). """

    RESOURCE = '%s.json'
    
    def __init__(self, package, name):
        self.package = package
        self.name = name
        self.key = package.resource(self.RESOURCE % name)

    @contextmanager
    def store(self):
        """ Create a context manager to store records in the cleaned
        table. """
        output = tempfile.NamedTemporaryFile(suffix='.json')
        try:
            yield lambda obj: output.write(json.dumps(obj) + '\n')

            output.seek(0)
            log.info("Uploading generated artifact to S3 (%r)...", self.key)
            self.key.set_contents_from_file(output)
        finally:
            output.close()

    def records(self):
        """ Get each record that has been stored in the table. """
        output = tempfile.NamedTemporaryFile(suffix='.json')
        try:
            log.info("Loading artifact from S3 (%r)...", self.key)
            self.key.get_contents_to_file(output)
            output.seek(0)

            for line in output.file:
                yield json.loads(line)
        
        finally:
            output.close()

    def __repr__(self):
        return '<Artifact(%r)>' % self.name
