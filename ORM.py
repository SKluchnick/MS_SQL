import pyodbc


class DBConnectorFirst:
    _select_query = 'select *  from {table} where {coll} = {val}'

    def __init__(self, dbname):
        with  pyodbc.connect(dbname) as connection:
            self.connection = connection
            self.cursor = self.connection.cursor()

    def main_execute(self, query):
        self.cursor.execute(query)
        # self.connection.commit()
        return self.cursor.fetchall()

    def select(self, table, coll, val):
        return self.main_execute(self._select_query.format(table=table, coll=coll, val=val))


db = DBConnectorFirst(r'Driver={SQL Server};Server=;Database=;Trusted_Connection=yes;')


