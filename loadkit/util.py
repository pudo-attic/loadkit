import os


def guess_extension(manifest):
    source_file = manifest.get('source_file', '')
    _, ext = os.path.splitext(source_file)
    if not len(ext):
        source_url = manifest.get('source_url', '')
        _, ext = os.path.splitext(source_url)
    return ext.replace('.', '').lower().strip()
            
