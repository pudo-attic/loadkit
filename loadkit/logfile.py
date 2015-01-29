import time
import logging
import tempfile
import shutil

from barn import Resource

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


class LogFile(Resource):
    """ A log file is a snippet of Python logging, preserved in the
    bucket. """

    GROUP = 'logs'
    
    def __repr__(self):
        return '<LogFile(%r)>' % self.name


class LogFileHandler(logging.FileHandler):
    """ Log to a temporary local file, then dump to a bucket. """

    def __init__(self, package, prefix):
        self.package = package
        self.prefix = prefix
        self.tmp = tempfile.NamedTemporaryFile()
        # to be reopened by the super class
        self.tmp.close()
        super(LogFileHandler, self).__init__(self.tmp.name)

    def archive(self):
        self.close()
        name = '%s/%s.log' % (self.prefix, int(time.time() * 1000))
        logfile = LogFile(self.package, name)
        self.tmp.seek(0)
        logfile.save_fileobj(self.tmp)


def capture(package, prefix, modules=[], level=logging.DEBUG):
    """ Capture log messages for the given modules and archive
    them to a ``LogFile`` resource. """
    handler = LogFileHandler(package, prefix)
    formatter = logging.Formatter(FORMAT)
    handler.setFormatter(formatter)
    modules = set(modules + ['loadkit'])

    for logger in modules:
        if not hasattr(logger, 'addHandler'):
            logger = logging.getLogger(logger)
        logger.setLevel(level=level)
        logger.addHandler(handler)

    return handler


def load(package, prefix, offset=0, limit=1000):
    """ Load lines from the log file with pagination support. """
    logs = package.all(LogFile, unicode(prefix))
    logs = sorted(logs, key=lambda l: l.name)
    seen = 0
    tmp = tempfile.NamedTemporaryFile(suffix='.log')
    for log in logs:
        shutil.copyfileobj(log.fh(), tmp)
        tmp.seek(0)
        for line in tmp:
            seen += 1
            if seen < offset:
                continue
            if seen > limit:
                tmp.close()
                return
            yield line
        tmp.seek(0)
    tmp.close()
