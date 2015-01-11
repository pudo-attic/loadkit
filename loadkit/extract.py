from urllib import urlopen
from shutil import copyfileobj
import tempfile

from loadkit.core import Source
from loadkit.util import make_secure_filename


def _make_source(package, name, fh, metadata):
    source = Source(package, name)
    source.key.set_contents_from_file(fh)
    source.meta.update(metadata)
    source.meta.save()
    fh.close()
    return source


def from_file(package, source_file):
    fh = open(source_file)
    return from_fileobj(package, fh, source_name=source_file)


def from_fileobj(package, fileobj, source_name=None):
    name = make_secure_filename(source_name or 'source')
    meta = {'source_file': source_name}
    return _make_source(package, name, fileobj, meta)


def from_url(package, source_url):
    name = make_secure_filename(source_url)
    temp = tempfile.NamedTemporaryFile()
    fh = urlopen(source_url)
    copyfileobj(fh, temp)
    fh.close()

    temp.seek(0)
    meta = {
        'source_url': source_url,
        'mime_type': fh.headers.get('Content-Type')
    }
    return _make_source(package, name, temp, meta)
