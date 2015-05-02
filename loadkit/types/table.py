import logging
import json
import tempfile
import shutil
from contextlib import contextmanager

from archivekit import Resource
from loadkit.util import json_default, json_hook

log = logging.getLogger(__name__)


class Table(Resource):
    """ The table holds a temporary, cleaned representation of the
    package resource (as a newline-separated set of JSON
    documents). """

    GROUP = 'tables'

    @contextmanager
    def store(self):
        """ Create a context manager to store records in the cleaned
        table. """
        output = tempfile.NamedTemporaryFile(suffix='.json')
        try:

            def write(o):
                line = json.dumps(o, default=json_default)
                return output.write(line + '\n')

            yield write

            output.seek(0)
            log.info("Uploading generated table (%s)...", self._obj)
            self.save_file(output.name, destructive=True)
        finally:
            try:
                output.close()
            except:
                pass

    def records(self):
        """ Get each record that has been stored in the table. """
        output = tempfile.NamedTemporaryFile(suffix='.json')
        try:
            log.info("Loading table from (%s)...", self._obj)
            shutil.copyfileobj(self.fh(), output)
            output.seek(0)

            for line in output.file:
                yield json.loads(line, object_hook=json_hook)

        finally:
            try:
                output.close()
            except:
                pass

    def __repr__(self):
        return '<Table(%r)>' % self.name
