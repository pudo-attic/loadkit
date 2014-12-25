import os
import json
from uuid import uuid4

from boto.s3.connection import S3Connection, S3ResponseError
from boto.s3.connection import Location

LOCATION = Location.EU

conn = S3Connection(os.environ.get('AWS_KEY_ID'),
                    os.environ.get('AWS_SECRET'))


bucket_name = 'data.mapthemoney.org'

try:
    bucket = conn.get_bucket(bucket_name)
except S3ResponseError, se:
    #if se.status == 404:
    bucket = conn.create_bucket(bucket_name)


class Manifest(dict):

    def __init__(self, key):
        self.key = key
        self.reload()

    def reload(self):
        if self.key.exists():
            self.update(json.load(self.key))

    def save(self):
        content = json.dumps(self)
        self.key.set_contents_from_string(content)

    def __repr__(self):
        return '<Manifest(%r)>' % self.key


class Package(object):
    """ An package is a resource in the remote bucket. It consists of a
    source file, a manifest metadata file and one or many processed
    version. """

    PREFIX = 'packages'
    MANIFEST = 'manifest.json'

    def __init__(self, bucket, id=None):
        self.bucket = bucket
        self.id = id or uuid4().hex

    def _get_key(self, name):
        key_name = os.path.join(self.PREFIX, self.id, name)
        key = self.bucket.get_key(key_name)
        if key is None:
            key = self.bucket.new_key(key_name)
        return key

    @property
    def source(self):
        if not hasattr(self, '_source_key'):
            self._source_key = self._get_key('source.data')
        return self._source_key

    @property
    def manifest(self):
        if not hasattr(self, '_manifest'):
            key = self._get_key(self.MANIFEST)
            self._manifest = Manifest(key)
        return self._manifest

    def save(self):
        self.manifest.save()

    def __repr__(self):
        return '<Package(%r)>' % self.id


class PackageIndex(object):
    """ The list of all packages with an existing manifest which exist in
    the given bucket. """

    def __init__(self, bucket):
        self.bucket = bucket

    def create(self, manifest=None):
        """ Create a package and save a manifest. If ``manifest`` is
        given, the values are saved to the manifest. """
        package = Package(self.bucket)
        if manifest is not None:
            package.manifest.update(manifest)
        package.save()
        return package

    def get(self, id):
        """ Get a ``Package`` identified by the ``id``. """
        return Package(self.bucket, id=id)

    def __iter__(self):
        for key in self.bucket.get_all_keys(prefix=Package.PREFIX):
            _, id, part = key.name.split('/')
            if part == Package.MANIFEST:
                yield self.get(id)


index = PackageIndex(bucket)
for package in index:
    print package, package.manifest.items()

# package = Package(bucket)

PATH = '/Users/fl/Downloads/interpol.csv'
with open(PATH, 'rb') as fh:
    package = index.create({'file_name': PATH})
    package.source.set_contents_from_file(fh)
