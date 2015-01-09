

class Resource(object):
    """ Any file within the prefix of the given package, including
    source data and artifacts. """

    def __init__(self, package, path):
        self.package = package
        self.path = path
        self.key = package.get_key(path)
        
    @property
    def url(self):
        # Welcome to the world of open data:
        self.key.make_public()
        return self.key.generate_url(expires_in=0, query_auth=False)

    def __repr__(self):
        return '<Resource(%r)>' % self.path

