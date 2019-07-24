from algernon.aws import Opossum
from mysql import connector

from toll_booth.obj.scalars.inputs import InputVertex


class SqlDriver:
    def __init__(self, sql_host, sql_port, db_name, username, password):
        self._sql_host = sql_host
        self._sql_port = sql_port
        self._db_name = db_name
        self._username = username
        self._password = password
        self._cursor = None
        self._connection = None

    @classmethod
    def generate(cls, sql_host, sql_port, db_name):
        credentials = Opossum.get_secrets('rds')
        return cls(sql_host, sql_port, db_name, credentials['username'], credentials['password'])

    def __enter__(self):
        self._connection = connector.connect(
            host=self._sql_host,
            port=self._sql_port,
            database=self._db_name,
            username=self._username,
            password=self._password
        )
        self._cursor = self._connection.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            self._cursor.close()
            self._connection.close()
            self._cursor = None
            self._connection = None
            return True
        raise Exception(exc_val)

    def add_vertex(self, scalar: InputVertex):
        if self._cursor is None:
            raise RuntimeError(f'must access the SqlDriver from within a context manager')
        vertex_args = (
            scalar.internal_id, scalar.identifier_stem, scalar.vertex_type, scalar.id_value, scalar.numeric_id_value)
        vertex_command = f'INSERT INTO Vertex VALUES (%s, %s, %s, %s, %s)'
        property_command = f'INSERT INTO VertexProperty VALUES (%s, %s, %s, %s, %s, %s)'
        property_args = []
        for vertex_property in scalar.vertex_properties:
            property_args.append((
                scalar.internal_id,
                vertex_property.property_name,
                vertex_property.property_value.data_type,
                vertex_property.property_value.property_value,
                vertex_property.property_value.__class__.__name__
            ))
        self._cursor.execute(vertex_command, params=vertex_args)
        self._cursor.executemany(property_command, property_args)
