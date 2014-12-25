import os
from slugify import slugify


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
            
