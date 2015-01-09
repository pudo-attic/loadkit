import os
from uuid import uuid4

from loadkit.core.resource import Resource
from loadkit.core.artifact import Artifact
from loadkit.core.manifest import Manifest


class Package(object):
    """ An package is a resource in the remote bucket. It consists of a
    source file, a manifest metadata file and one or many processed
    version. """

    PREFIX = 'packages'
    MANIFEST = 'manifest.json'

    def __init__(self, bucket, id=None):
        self.bucket = bucket
        self.id = id or uuid4().hex
        self._keys = {}

    def get_key(self, name):
        if name not in self._keys:
            key_name = os.path.join(self.PREFIX, self.id, name)
            key = self.bucket.get_key(key_name)
            if key is None:
                key = self.bucket.new_key(key_name)
            self._keys[name] = key
        return self._keys[name]

    @property
    def resources(self):
        prefix = os.path.join(self.PREFIX, self.id)
        for key in self.bucket.get_all_keys(prefix=prefix):
            path = key.name.replace(prefix, '').strip('/')
            if path == self.MANIFEST:
                continue
            yield Resource(self, path)
    
    @property
    def artifacts(self):
        for resource in self.resources:
            artifact = Artifact.from_path(self, resource.path)
            if artifact is not None:
                yield artifact

    @property
    def manifest(self):
        if not hasattr(self, '_manifest'):
            key = self.get_key(self.MANIFEST)
            self._manifest = Manifest(key)
        return self._manifest

    def save(self):
        self.manifest.save()

    def __eq__(self, other):
        return self.id == other.id

    def __repr__(self):
        return '<Package(%r)>' % self.id
