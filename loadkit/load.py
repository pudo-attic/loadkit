from sqlalchemy.schema import MetaData, Column
from sqlalchemy.schema import Table


class DatabaseLoader(object):
    
    def __init__(self, engine, package):
        self.schema = None
        self.metadata = MetaData(schema=self.schema)
        self.engine = engine
        self.package = package

    def table(self, artifact):
        pass

    def load(self, artifact):
        print self.table(artifact)

        for record in artifact.records():
            print 'REC', record
