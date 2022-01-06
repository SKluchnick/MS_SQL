import pyodbc

connection_to_db = pyodbc.connect(
    r'Driver={SQL Server};Server=DESKTOP-JP8IK62\SQLEXPRESS;Database=P_STORE;Trusted_Connection=yes;')
cursor = connection_to_db.cursor()

cursor.execute("insert into new_phone_price(phone_id,color_id,merchant_id,price)\
 values (10002,19,55,9999)")

cursor.execute("select * from new_phone_price where phone_id = 10002 and color_id = 19 and merchant_id = 55 and price = 9999")

cursor.execute("insert into new_phone_price(phone_id,merchant_id,color_id,price) \
               select p.phone_id,55 as merchant_id,19 as color_id,8888 as price \
               from phones p left join phone_price pp on p.phone_id = pp.phone_id where pp.phone_id is null and p.brand_id = 6")

cursor.execute("select * from new_phone_price where price = 8888 and merchant_id = 55")

cursor.execute("update npp set npp.price = npp.price +110 from new_phone_price npp join phones p on npp.phone_id=p.phone_id left join phone_price pp on p.phone_id = pp.phone_id where pp.phone_id is null and p.brand_id = 6")


cursor.execute("select from new_phone_price npp join phones p on npp.phone_id=p.phone_id left join phone_price pp on p.phone_id = pp.phone_id where pp.phone_id is null and p.brand_id = 6")


cursor.execute("update pp set pp.price = pp.price +10 \
from new_phone_price pp \
where exists (select * from stock s where s.amount = 1 and pp.phone_id = s.phone_id \
and pp.color_id = s.color_id and pp.merchant_id = s.merchant_id) \
")

cursor.execute("delete from  npp \
from new_phone_price npp  \
join (select top 2 *from phone_price where merchant_id=2 and color_id=23 order by price)a \
on npp.phone_id = a.phone_id and npp.merchant_id = a.merchant_id and npp.color_id = a.color_id")

while 1:
    row = cursor.fetchone()
    if not row:
        break
    for i in row:
        print(i)
connection_to_db.close()