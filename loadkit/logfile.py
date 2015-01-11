import time
import logging
import tempfile

from loadkit.core import LogFile

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


class LogFileHandler(logging.FileHandler):
    """ Log to a temporary local file, then dump to a bucket. """

    def __init__(self, package, prefix):
        self.package = package
        self.prefix = prefix
        self.tmp = tempfile.NamedTemporaryFile()
        super(LogFileHandler, self).__init__(self.tmp.name)

    def archive(self):
        self.close()
        name = '%s/%s.log' % (self.prefix, int(time.time() * 1000))
        logfile = LogFile(self.package, name)
        self.tmp.seek(0)
        logfile.key.set_contents_from_file(self.tmp)


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
    logs = package.logfiles(unicode(prefix))
    logs = sorted(logs, key=lambda l: l.name)
    seen = 0
    tmp = tempfile.NamedTemporaryFile(suffix='.log')
    for log in logs:
        log.key.get_contents_to_file(tmp)
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
