import os
from urllib import urlopen

from messytables import any_tableset, type_guess
from messytables import types_processor, headers_guess
from messytables import headers_processor, offset_processor


def get_fileobj(package):
    # This is a work-around because messytables hangs on boto file
    # handles, so we're doing it via plain old HTTP.
    package.source.make_public()
    url = package.source.generate_url(expires_in=0, query_auth=False)
    return urlopen(url)


def package_rows(package):
    """ Generate an iterator over all the rows in this package's
    source data. """

    # Try to gather information about the source file type.
    if not package.manifest.get('extension'):
        source_file = package.manifest.get('source_file', '')
        _, ext = os.path.splitext(source_file)
        if not len(ext):
            source_url = package.manifest.get('source_url', '')
            _, ext = os.path.splitext(source_url)
        ext = ext.replace('.', '').lower().strip()
        package.manifest['extension'] = ext
    
    table_set = any_tableset(get_fileobj(package),
                             extension=package.manifest.get('extension'),
                             mimetype=package.manifest.get('mime_type'))
    tables = list(table_set.tables)
    if not len(tables):
        return

    row_set = tables[0]
    offset, headers = headers_guess(row_set.sample)
    row_set.register_processor(headers_processor(headers))
    row_set.register_processor(offset_processor(offset + 1))
    types = type_guess(row_set.sample, strict=True)
    row_set.register_processor(types_processor(types))
    
    for row in row_set:
        yield row


def convert_package(package):
    """ Store a parsed version of the package resource. """
    for row in package_rows(package):
        print row
