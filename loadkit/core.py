import os
import json
import tempfile
import logging
from contextlib import contextmanager
from uuid import uuid4

log = logging.getLogger(__name__)


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

    @property
    def source(self):
        return self.resource('source.raw')

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

    def create(self, manifest=None, source_file=None, source_url=None,
               mime_type=None):
        """ Create a package and save a manifest. If ``manifest`` is
        given, the values are saved to the manifest. """
        package = Package(self.bucket)
        if manifest is not None:
            package.manifest.update(manifest)
        package.manifest['source_file'] = source_file
        package.manifest['source_url'] = source_url
        package.manifest['mime_type'] = mime_type
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


class Resource(object):
    """ Any file within the prefix of the given package, including
    source data and artifacts. """

    def __init__(self, package, path):
        self.package = package
        self.path = path
        self.key = package._get_key(path)

        # Welcome to the world of open data:
        self.key.make_public()

    @property
    def url(self):
        return self.key.generate_url(expires_in=0, query_auth=False)

    def __repr__(self):
        return '<Resource(%r)>' % self.path


class Artifact(Resource):
    """ The artifact holds a temporary, cleaned representation of the
    package resource (as a newline-separated set of JSON
    documents). """

    RESOURCE = 'artifacts/%s.json'
    
    def __init__(self, package, name):
        self.name = name
        path = self.RESOURCE % name
        super(Artifact, self).__init__(package, path)

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
