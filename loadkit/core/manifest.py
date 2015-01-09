import json

from loadkit.util import json_default, json_hook


class Manifest(dict):
    """ A manifest has metadata on a package. """

    def __init__(self, key):
        self.key = key
        self.reload()

    def reload(self):
        if self.key.exists():
            self.update(json.load(self.key, object_hook=json_hook))

    def save(self):
        content = json.dumps(self, default=json_default)
        self.key.set_contents_from_string(content)

    def __repr__(self):
        return '<Manifest(%r)>' % self.key

