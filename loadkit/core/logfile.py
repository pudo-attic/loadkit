import logging

from loadkit.core.resource import Resource

log = logging.getLogger(__name__)


class LogFile(Resource):
    """ A log file is a snippet of Python logging, preserved in the
    bucket. """

    GROUP = 'logs'
    
    def __repr__(self):
        return '<LogFile(%r)>' % self.name
