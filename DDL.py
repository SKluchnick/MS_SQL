import pyodbc

connection_to_db = pyodbc.connect(
    r'Driver={SQL Server};Server=;Database=;Trusted_Connection=yes;')
cursor = connection_to_db.cursor()

# cursor.execute("create table  exchange_rate (cur_name nchar(3),rate numeric(4,2) )")
#
# cursor.commit()

# cursor.execute("insert into exchange_rate (cur_name,rate) values ('USD',25)")
# cursor.commit()

# cursor.execute("select * from exchange_rate")

# cursor.execute("create view phone_price_usd as select p.phone_id,p.color_id,p.merchant_id, round(p.price/er.rate,0)as price from phone_price p,exchange_rate er")
# cursor.commit()

# cursor.execute("select * from phone_price_usd")

# cursor.execute("insert into exchange_rate (cur_name,rate) values ('EUR',27)")
# cursor.commit()

# cursor.execute("select * from exchange_rate")

# cursor.execute("alter view phone_price_usd as select p.phone_id,p.color_id,p.merchant_id, round(p.price/er.rate,0)as price from phone_price p,exchange_rate er WHERE cur_name='USD'")
# cursor.commit()

# cursor.execute("select * from phone_price_usd")

# cursor.execute("create view phone_price_eur as select p.phone_id,p.color_id,p.merchant_id, round(p.price/er.rate,0)as price from phone_price p,exchange_rate er WHERE cur_name='EUR'")
# cursor.commit()

# cursor.execute("select * from phone_price_eur")

# cursor.execute("alter table exchange_rate add date_from DATE, date_to DATE")
# cursor.commit()

#
# cursor.execute("update exchange_rate set date_from = DATEFROMPARTS(2020,1,1),date_to = DATEFROMPARTS(2100,12,31)")
# cursor.commit()

# cursor.execute("insert into exchange_rate (cur_name,rate,date_from,date_to) values('USD' ,24,DATEFROMPARTS(2019,1,1), DATEFROMPARTS(2019,12,31))")
# cursor.commit()

# cursor.execute("alter view phone_price_usd as select p.phone_id,p.color_id,p.merchant_id, round(p.price/er.rate,0)as price,er.date_from,er.date_to,er.cur_name from phone_price p,exchange_rate er WHERE er.cur_name='USD' and getdate() between date_from and date_to")
# cursor.commit()

# cursor.execute("drop table exchange_rate")
# cursor.commit()

# cursor.execute("drop  view phone_price_usd")
# cursor.commit()

# cursor.execute("drop  view phone_price_eur")
# cursor.commit()

# cursor.execute("select * from exchange_rate")

# while 1:
#     row = cursor.fetchone()
#     if not row:
#         break
#     for i in row:
#         print(i)
# connection_to_db.close()