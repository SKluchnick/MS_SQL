import pyodbc
from datetime import datetime

connection_to_db = pyodbc.connect(r'Driver={SQL Server};Server=DESKTOP-JP8IK62\SQLEXPRESS;Database=P_STORE;Trusted_Connection=yes;')
cursor = connection_to_db.cursor()
cursor.execute('select * from phones')
while 1:
    row = cursor.fetchone()
    if not row:
        break
    print(row.phone_id)
connection_to_db.close()