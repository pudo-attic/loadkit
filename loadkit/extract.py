
def from_file(package, source_file):
    return package.ingest(source_file)


def from_fileobj(package, fileobj, source_name=None):
    meta = {'source_file': source_name}
    return package.ingest(from_fileobj, meta=meta)


def from_url(package, source_url):
    return package.ingest(source_url)
