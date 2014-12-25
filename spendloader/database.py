
from sqlalchemy.schema import MetaData, Column
from sqlalchemy.schema import Table as SQLATable

from spendloader.table import Table


class DatabaseLoader(object):
    
    def __init__(self, engine, package):
        self.schema = None
        self.metadata = MetaData(schema=self.schema)
        self.engine = engine
        self.package = package

    def schema(self, artifact):
        pass

    def load(self, artifact):
        print self.schema(artifact)

        for record in artifact.records():
            print 'REC', record
