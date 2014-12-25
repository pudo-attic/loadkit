from urllib import urlopen
from shutil import copyfileobj
import tempfile

from loadkit.core import Resource
from loadkit.util import make_secure_filename


def _make_resource(package, name, fh, metadata):
    package.manifest.update(metadata)
    package.save()
    resource = Resource(package, name)
    resource.key.set_contents_from_file(fh)
    fh.close()
    return resource


def from_file(package, source_file):
    name = make_secure_filename(source_file)
    meta = {'source_file': source_file}
    fh = open(source_file)
    return _make_resource(package, name, fh, meta)


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
    return _make_resource(package, name, temp, meta)
