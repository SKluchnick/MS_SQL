import pyodbc


class DBConnectorSecond:
    _select_query = 'select *  from {table} where {coll} = {val}'
    _shema = {}

    def __init__(self, dbname):
        with  pyodbc.connect(dbname) as connection:
            self.connection = connection
            self.cursor = self.connection.cursor()

    def fetch_table(self):
        tables = self.main_execute("SELECT * FROM INFORMATION_SCHEMA.TABLES")
        for i in range(len(tables)):
            self._shema.update({tables[i][2]: tables[i][0]})
        return self._shema

    def main_execute(self, query):
        self.cursor.execute(query)
        # self.connection.commit()
        return self.cursor.fetchall()

    def select(self, table, coll, val):
        new_shema = self.fetch_table()
        if table not in new_shema:
            raise Exception ("This table is absent")
        return self.main_execute(self._select_query.format(table=table, coll=coll, val=val))


db = DBConnectorSecond(
    r'Driver={SQL Server};Server=DESKTOP-JP8IK62\SQLEXPRESS;Database=computer;Trusted_Connection=yes;')
res = db.select('PCf', 'model', 1232)
print(res)
