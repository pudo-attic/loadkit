import os
from decimal import Decimal
from datetime import datetime, date


def guess_extension(name):
    _, ext = os.path.splitext(name or '')
    return ext.replace('.', '').lower().strip()


def json_default(obj):
    if isinstance(obj, datetime):
        obj = obj.isoformat()
    if isinstance(obj, Decimal):
        obj = float(obj)
    if isinstance(obj, date):
        return 'loadKitDate(%s)' % obj.isoformat()
    return obj


def json_hook(obj):
    for k, v in obj.items():
        if isinstance(v, basestring):
            try:
                obj[k] = datetime.strptime(v, "loadKitDate(%Y-%m-%d)").date()
            except ValueError:
                pass
            try:
                obj[k] = datetime.strptime(v, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                pass
    return obj
