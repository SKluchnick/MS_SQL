import pyodbc

connection_to_db = pyodbc.connect(
    r'Driver={SQL Server};Server=;Database=;Trusted_Connection=yes;')
cursor = connection_to_db.cursor()

cursor.execute("select*\
                ,sum(t1.out) over(partition by year(t1.date), month(t1.date))/sum(t1.out) over(partition by year(t1.date))\
                from Outcome_o t1")

cursor.execute("select*\
                ,sum(t1.out) over(order by(t1.date) rows between  unbounded preceding and current row)\
                from Outcome_o t1\
                ")

cursor.execute("select*\
                ,sum(t1.out) over(partition by year(t1.date) order by(t1.date) rows between  unbounded preceding and current row)\
                from Outcome_o t1")

cursor.execute("select*\
                ,sum(t1.out) over(order by(t1.date) rows between  1 preceding and 1 following)\
                from Outcome_o t1")

cursor.execute("select*\
                ,sum(t1.out) over(order by(t1.date) rows between unbounded preceding and  1 preceding)\
                from Outcome_o t1")


cursor.execute(" \
                select a.merchant_name,a.Brand_name,a.color_name,a.qu_sell \
                ,LAST_VALUE(a.qu_sell)over(partition by a.merchant_name,a.Brand_name order by a.Brand_name,a.qu_sell rows between \
                current row and unbounded following) as [total_a.qu_sell_12]\
                from\
                (select m.merchant_name,b.Brand_name,c.color_name,count(*) as qu_sell from purchases p \
                left join phones f on p.phone_id = f.phone_id\
                left join brands b on f.brand_id = b.brand_id\
                left join merchants m on p.merchant_id = m.merchant_id\
                left join colors c on p.color_id = c.color_id\
                group by m.merchant_name,b.Brand_name,c.color_name)a\
                order by a.merchant_name,a.Brand_name,a.qu_sell")



cursor.execute('select brand_name, phone_name, memory, price, \
                count(phone_name)over(partition by memory) as quantityPhonesWithMemory,\
                avg(price)over(partition by memory) as quantityPhonesWithPrice \
                from m_phones where brand_name in (?)',('apple'))

cursor.execute("select pp.price,m.merchant_name,b.Brand_name,p.phone_name\
                ,row_number() over( order by merchant_name, brand_name , phone_name) as [row_number_table]\
                ,row_number() over(partition by merchant_name order by merchant_name, brand_name , phone_name) as [row_number_merchant_name]\
                ,row_number() over(partition by merchant_name, brand_name order by merchant_name, brand_name , phone_name) as [row_number_merchant_name_brand_name]\
                ,row_number() over(partition by  brand_name order by merchant_name, brand_name , phone_name) as [row_number_merchant_name_brand_name]\
                from phone_price pp \
                left join merchants m on pp.merchant_id = m.merchant_id\
                left join phones p on pp.phone_id = p.phone_id\
                left join brands b on p.brand_id = b.brand_id\
                order by m.merchant_name, b.brand_name, p.phone_name")


cursor.execute("select maker, model, type from\
                (\
                select maker, model, type, \
                rank() over(partition  by type order by by model) num\
                from Product\
                ) X\
                WHERE num <= 3")


cursor.execute("select brand_name, phone_name,memory,price,\
                row_number() over(order by memory, price) as rowNumberMemoryPrice,\
                row_number() over(order by price desc) as rowNumberPriceDesc\
                from m_phones\
                where brand_name in ('samsung') and price is not null\
                order by memory, price")



cursor.execute("select point, date, out,\
                (select SUM(out) \
                from Outcome_o \
                where point = o.point and date <= o.date) run_tot \
                from Outcome_o o\
                where point = 2\
                order BY point, date;")

cursor.execute("Select max_sum, type, date, point \
                from (\
                select max(inc) over() AS max_sum, *\
                from (\
                select inc, 'inc' type, date, point FROM Income \
                union all \
                select inc, 'inc' type, date, point FROM Income_o \
                union all \
                select out, 'out' type, date, point FROM Outcome_o \
                union all \
                select out, 'out' type, date, point FROM Outcome \
                ) X \
                ) Y\
                where inc = max_sum;")


while 1:
    row = cursor.fetchone()
    if not row:
        break
    for i in row:
        print(i)
connection_to_db.close()
