import time
import logging
import tempfile
import shutil

from loadkit.types.logfile import LogFile

SEP = '-||-'
FORMAT = '%%(asctime)s %s %%(name)s %s %%(levelname)s %s %%(message)s' % \
    (SEP, SEP, SEP)


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
        with open(self.tmp.name, 'rb') as fh:
            logfile.save_fileobj(fh)


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
    logs = sorted(logs, key=lambda l: l.name, reverse=True)
    seen = 0
    record = None
    tmp = tempfile.NamedTemporaryFile(suffix='.log')
    for log in logs:
        shutil.copyfileobj(log.fh(), tmp)
        tmp.seek(0)
        for line in reversed(list(tmp)):
            seen += 1
            if seen < offset:
                continue
            if seen > limit:
                tmp.close()
                return
            try:
                d, mo, l, m = line.split(' %s ' % SEP, 4)
                if record is not None:
                    yield record
                record = {'time': d, 'module': mo, 'level': l, 'message': m}
            except ValueError:
                if record is not None:
                    record['message'] += '\n' + line
        tmp.seek(0)
    tmp.close()
    if record is not None:
        yield record
