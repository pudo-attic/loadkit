import os
import json
from uuid import uuid4


class Manifest(dict):
    """ A manifest has metadata on a package. """

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
        self._resources = {}

    def _get_key(self, name):
        key_name = os.path.join(self.PREFIX, self.id, name)
        key = self.bucket.get_key(key_name)
        if key is None:
            key = self.bucket.new_key(key_name)
        return key

    def resource(self, name):
        if name not in self._resources:
            self._resources[name] = self._get_key(name)
        return self._resources[name]

    @property
    def source(self):
        return self.resource('source_data')

    @property
    def manifest(self):
        if not hasattr(self, '_manifest'):
            key = self.resource(self.MANIFEST)
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

