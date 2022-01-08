import pyodbc

class DBConnector:
    _select_query = 'select *  from {table} where {coll} = {val}'
    def __init__(self, dbname):
        with  pyodbc.connect(dbname) as connection:
            self.connection = connection
            self.cursor = self.connection.cursor()
            table = self.main_execute("SELECT * FROM INFORMATION_SCHEMA.TABLES")
            rows = self.main_execute("SELECT * FROM INFORMATION_SCHEMA.COLUMNS")


    def main_execute(self, query):
        self.cursor.execute(query)
        # self.connection.commit()
        return self.cursor.fetchall()

    def select(self,table,coll,val):
        return self.main_execute(self._select_query.format(table=table,coll=coll,val=val))



db = DBConnector(r'Driver={SQL Server};Server=;Database=;Trusted_Connection=yes;')
res = db.main_execute("select * from m_phones")
print(res)
res2 = db.select('colors','color_id','1')
print(res2)

