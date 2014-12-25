import os
from slugify import slugify
from datetime import datetime, date


def make_secure_filename(source):
    source = os.path.basename(source)
    fn, ext = os.path.splitext(source)
    ext = ext or '.raw'
    return slugify(fn) + ext


def guess_extension(manifest):
    source_file = manifest.get('source_file', '')
    _, ext = os.path.splitext(source_file)
    if not len(ext):
        source_url = manifest.get('source_url', '')
        _, ext = os.path.splitext(source_url)
    return ext.replace('.', '').lower().strip()
            

def json_default(obj):
    if isinstance(obj, datetime):
        obj = obj.isoformat()
    if isinstance(obj, date):
        return 'loadKitDate(%s)' % obj.isoformat()
    return obj


def json_hook(obj):
    for k, v in obj.items():
        try:
            if isinstance(v, unicode):
                obj[k] = datetime.strptime(v, "loadKitDate(%Y-%m-%d)").date()
        except ValueError:
            pass
        try:
            if isinstance(v, unicode):
                obj[k] = datetime.strptime(v, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass
    return obj
