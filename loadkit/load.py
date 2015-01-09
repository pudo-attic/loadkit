from sqlalchemy.schema import MetaData, Column
from sqlalchemy.schema import Table
from sqlalchemy import Integer, String, Date

TYPES = {
    'string': String,
    'integer': Integer,
    'date': Date
}


class DatabaseLoader(object):
    
    def __init__(self, engine, artifact):
        self.schema = None
        self.metadata = MetaData(schema=self.schema)
        self.engine = engine
        self.package = artifact.package
        self.artifact = artifact

    @property
    def fields(self):
        fields = self.package.manifest.get('fields', [])
        assert len(fields), 'Has this package not been transformed?'
        return fields

    @property
    def table_name(self):
        # TODO: make configurable.
        return 'loadkit_%s_%s' % (self.package.id, self.artifact.name)

    @property
    def table(self):
        if not hasattr(self, '_table'):
            if self.engine.has_table(self.table_name, schema=self.schema):
                self._table = Table(self.table_name, self.metadata, autoload=True)

            else:
                table = Table(self.table_name, self.metadata)
                col = Column('_id', Integer, primary_key=True)
                table.append_column(col)

                for field in self.fields:
                    data_type = TYPES.get(field.get('type', String))
                    col = Column(field.get('name'), data_type)
                    table.append_column(col)

                table.create(self.engine)
                self._table = table
        return self._table

    def load(self, chunk_size=1000):
        """ Bulk load all the data in an artifact to a matching database
        table. """
        table = self.table
        chunk = []
        conn = self.engine.connect()
        tx = conn.begin()
        try:
            for record in self.artifact.records():
                chunk.append(record)
                if len(chunk) >= chunk_size:
                    stmt = table.insert(chunk)
                    conn.execute(stmt)
                    chunk = []

            if len(chunk):
                stmt = table.insert(chunk)
                conn.execute(stmt)
            tx.commit()
        except:
            tx.rollback()
            raise


def table(engine, artifact):
    l = DatabaseLoader(engine, artifact)
    l.load()
    return l.table
