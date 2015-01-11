import random
import logging
from decimal import Decimal
from datetime import datetime
from urllib import urlopen

from slugify import slugify
from messytables import any_tableset, type_guess
from messytables import types_processor, headers_guess
from messytables import headers_processor, offset_processor

from loadkit.core import Artifact
from loadkit.util import guess_extension

log = logging.getLogger(__name__)


def resource_row_set(package, resource):
    """ Generate an iterator over all the rows in this resource's
    source data. """
    # Try to gather information about the source file type.
    if not resource.meta.get('extension'):
        resource.meta['extension'] = guess_extension(resource.meta.get('name'))
    
    # This is a work-around because messytables hangs on boto file
    # handles, so we're doing it via plain old HTTP.
    table_set = any_tableset(urlopen(resource.url),
                             extension=resource.meta.get('extension'),
                             mimetype=resource.meta.get('mime_type'))
    tables = list(table_set.tables)
    if not len(tables):
        log.error("No tables were found in the source file.")
        return
    return tables[0]


def column_alias(cell, names):
    """ Generate a normalized version of the column name. """
    column = slugify(cell.column or '', separator='_')
    column = column.strip('_')
    column = 'column' if not len(column) else column
    name, i = column, 2
    # de-dupe: column, column_2, column_3, ...
    while name in names:
        name = '%s_%s' % (name, i)
        i += 1
    return name


def generate_field_spec(row):
    """ Generate a set of metadata for each field/column in
    the data. This is loosely based on jsontableschema. """
    names = set()
    fields = []
    for cell in row:
        name = column_alias(cell, names)
        field = {
            'name': name,
            'title': cell.column,
            'type': unicode(cell.type).lower(),
            'has_nulls': False,
            'has_empty': False,
            'samples': []
        }
        if hasattr(cell.type, 'format'):
            field['type'] = 'date'
            field['format'] = cell.type.format
        fields.append(field)
    return fields


def random_sample(value, field, row, num=10):
    """ Collect a random sample of the values in a particular
    field based on the reservoir sampling technique. """
    # TODO: Could become a more general DQ piece.
    if value is None:
        field['has_nulls'] = True
        return
    if value in field['samples']:
        return
    if isinstance(value, basestring) and not len(value.strip()):
        field['has_empty'] = True
        return
    if len(field['samples']) < num:
        field['samples'].append(value)
        return
    j = random.randint(0, row)
    if j < (num - 1):
        field['samples'][j] = value


def parse_table(row_set, save_func):
    num_rows = 0
    fields = {}

    offset, headers = headers_guess(row_set.sample)
    row_set.register_processor(headers_processor(headers))
    row_set.register_processor(offset_processor(offset + 1))
    types = type_guess(row_set.sample, strict=True)
    row_set.register_processor(types_processor(types))

    for i, row in enumerate(row_set):
        if not len(fields):
            fields = generate_field_spec(row)

        data = {}
        for cell, field in zip(row, fields):
            value = cell.value
            if isinstance(value, datetime):
                value = value.date()
            if isinstance(value, Decimal):
                # Baby jesus forgive me.
                value = float(value)
            if isinstance(value, basestring) and not len(value.strip()):
                value = None
            data[field['name']] = value
            random_sample(value, field, i)

        check_empty = set(data.values())
        if None in check_empty and len(check_empty) == 1:
            continue

        save_func(data)
        num_rows = i

    fields = {f.get('name'): f for f in fields}
    return num_rows, fields


def to_table(resource, name):
    """ Store a parsed version of the package resource. """
    package = resource.package
    artifact = Artifact(package, name)
    artifact.meta.update(resource.meta)

    with artifact.store() as save:
        row_set = resource_row_set(package, resource)
        num_rows, fields = parse_table(row_set, save)

    log.info("Converted %s rows with %s columns.", num_rows, len(fields))
    artifact.meta['fields'] = fields
    artifact.meta['num_records'] = num_rows
    artifact.meta.save()
    return artifact
