from urllib import urlopen
from shutil import copyfileobj
from itertools import count
import tempfile

from loadkit.core import Source
from loadkit.util import make_secure_filename


def _make_source(package, slug, fh, metadata):
    for i in count(1):
        name = '-%s' % i if i > 1 else ''
        name = '%s%s.%s' % (slug, name, metadata.get('extension'))
        if package.has(Source, name):
            continue
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
    source_name, slug, ext = make_secure_filename(source_name)
    meta = {
        'extension': ext,
        'name': source_name
    }
    return _make_source(package, slug, fileobj, meta)


def from_url(package, source_url):
    source_name, slug, ext = make_secure_filename(source_url)
    temp = tempfile.NamedTemporaryFile()
    fh = urlopen(source_url)
    copyfileobj(fh, temp)
    fh.close()

    temp.seek(0)
    meta = {
        'extension': ext,
        'source_url': source_url,
        'name': source_name,
        'mime_type': fh.headers.get('Content-Type')
    }
    return _make_source(package, slug, temp, meta)
