from sqlalchemy.schema import MetaData, Column
from sqlalchemy.schema import Table


class DatabaseLoader(object):
    
    def __init__(self, engine, artifact):
        self.schema = None
        self.metadata = MetaData(schema=self.schema)
        self.engine = engine
        self.package = artifact.package
        self.artifact = artifact

    @property
    def table(self):
        pass

    def load(self):

        for record in self.artifact.records():
            print 'REC', record


def table(engine, artifact):
    l = DatabaseLoader(engine, artifact)
    l.load()
    return l.table
