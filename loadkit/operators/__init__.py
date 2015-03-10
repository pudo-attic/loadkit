import sys
import os
import logging
import importlib

from pkg_resources import iter_entry_points

from loadkit.util import ConfigException

NAMESPACE = 'loadkit.operators'
OPERATORS = {}

log = logging.getLogger(__name__)


def load():
    if not len(OPERATORS):
        for ep in iter_entry_points(NAMESPACE):
            OPERATORS[ep.name] = ep.load()
        log.info('Available operators: %s', ', '.join(OPERATORS.keys()))
    return OPERATORS


def load_operator(name):
    operators = load()
    if name in operators:
        return operators.get(name)
    try:
        if ':' in name:
            if os.getcwd() not in sys.path:
                sys.path.append(os.getcwd())
            module, cls = name.split(':', 1)
            return getattr(importlib.import_module(module), cls)
    except ImportError:
        pass
    raise ConfigException('Invalid operator: %r' % name)
