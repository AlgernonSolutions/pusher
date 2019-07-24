from toll_booth.obj.scalars.inputs import InputVertex
from toll_booth.obj.sql.sql_driver import SqlDriver


def rds_handler(pushed_objects, **kwargs):
    pass


if __name__ == '__main__':
    host = 'algernon-1.cluster-cnd32dx4xing.us-east-1.rds.amazonaws.com'
    port = 3306
    db_name = 'algernon'
    driver = SqlDriver.generate(host, port, db_name)
    test_vertex = InputVertex('')
    print()
