import json
import logging
import tempfile
from contextlib import contextmanager

TABLE_RESOURCE = 'table.json'

log = logging.getLogger(__name__)


def get_table(package):
    return package.resource(TABLE_RESOURCE)


@contextmanager
def store_records(package):
    key = get_table(package)
    output = tempfile.NamedTemporaryFile(suffix='.json')
    try:
        yield lambda obj: output.write(json.dumps(obj) + '\n')

        output.seek(0)
        log.info("Uploading generated table to S3 (%r)...", key)
        key.set_contents_from_file(output)
    finally:
        output.close()


def get_records(package):
    key = get_table(package)
    output = tempfile.NamedTemporaryFile(suffix='.json')
    try:
        key.get_contents_to_file(output)
        output.seek(0)

        for line in output.file:
            yield json.loads(line)
    
    finally:
        output.close()
