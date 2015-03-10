from archivekit import Resource


class LogFile(Resource):
    """ A log file is a snippet of Python logging, preserved in the
    bucket. """

    GROUP = 'logs'

    def __repr__(self):
        return '<LogFile(%r)>' % self.name
