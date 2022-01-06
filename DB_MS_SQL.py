import pyodbc

connection_to_db = pyodbc.connect(
    r'Driver={SQL Server};Server=DESKTOP-JP8IK62\SQLEXPRESS;Database=P_STORE;Trusted_Connection=yes;')
cursor = connection_to_db.cursor()

cursor.execute('select min(weight) as min, max(weight) as max,avg(weight) as avg,sum(weight)/count(weight) as averOfWeight'
               ', sum(weight)/count(*)as averOfWeighTwo from m_phones')
cursor.execute('select brand_name, phone_name, battery_capacity/weight as phoneBatteryCapacity from m_phones')

cursor.execute('select top 15 percent * from m_phones order by battery_capacity desc')

cursor.execute('select * from m_phones order by battery_capacity desc offset 2 rows fetch next 4 rows only')

cursor.execute('select brand_name,battery_type, avg(price) as avgPriceBatteryType, count(phone_name) as quantityPhones \
               from m_phones where battery_capacity > 3000\
               group by brand_name, battery_type having count(phone_name) >= 2 order by brand_name ')

cursor.execute('select avg(mp) from (select color_id,max(price) as mp from phone_price group by color_id) a')

cursor.execute('select * from phone_price p1 where p1.price \
               any (select p2.price from phone_price p2 where p2.merchant_id =1) and p1.merchant_id =1 order by p1.price')

cursor.execute('select * from phone_price p1 where p1.price any (select p2.price from phone_price p2 \
               where p2.merchant_id =1) and p1.merchant_id =1 order by p1.price')

cursor.execute('select * from phones left join phone_comment on phones.phone_id = phone_comment.phone_id \
                where exists (select phone_comment.enthusiastic \
               from phone_comment where phones.phone_id = phone_comment.phone_id and  phone_comment.enthusiastic > 100')

cursor.execute('select phone_price.phone_id,phone_price.color_id from phone_price where phone_price.merchant_id = 1 \
                intersect select phone_price.phone_id,phone_price.color_id from phone_price where phone_price.merchant_id = 8')

cursor.execute('select cl.client_name as clientName \
                ,p.date_purch as datePurch \
                ,b.Brand_name as brandName \
                ,ph.phone_name as phoneName \
                ,c.color_name as colorName \
                ,p.price as price \
                from purchases p \
                left join clients cl on p.client_id = cl.client_id \
                left join phones ph on p.phone_id = ph.phone_id \
                left join brands b on ph.brand_id = b.brand_id \
                left join colors c on p.color_id = c.color_id \
                where p.client_id in (select purchases.client_id \
                from purchases \
                group by purchases.client_id \
                having avg(purchases.price) > 3000 and count(purchases.date_purch)>= 2)  ')



cursor.execute('select brand_name, phone_name, memory, price, \
                count(phone_name)over(partition by memory) as quantityPhonesWithMemory,\
                avg(price)over(partition by memory) as quantityPhonesWithPrice \
                from m_phones where brand_name in (?)',('apple'))

while 1:
    row = cursor.fetchone()
    if not row:
        break
    for i in row:
        print(i)
connection_to_db.close()
