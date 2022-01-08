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

cursor.execute("select * \
                ,(case when b.five < 0.8 then 'a'\
                when b.five < 0.95 then 'b'\
                else 'c'\
                end) as tab\
                from(select * \
                ,sum(a.four) over(order by a.four desc  rows between unbounded preceding and current row) five\
                from(select distinct t3.Brand_name\
                ,sum(t1.price) over(partition by t3.Brand_name) one\
                ,count(t1.price) over(partition by t3.Brand_name) two\
                ,sum(t1.price) over() three\
                ,sum(t1.price) over(partition by t3.Brand_name)/sum(t1.price) over() four\
                from purchases t1\
                left join phones t2 on t1.phone_id = t2.phone_id\
                left join brands t3 on t2.brand_id = t3.brand_id\
                where year(t1.date_purch)=2019 and month(t1.date_purch) between 1 and 3)a\
                )b")

cursor.execute("select ab.phone_name\
                ,ab.[total phone]\
                ,ab.[sum(price)]\
                ,ab.share\
                ,ab.[нарастающий итог] \
                ,case\
                when ab.[нарастающий итог] <= 0.8 then 'A' \
                when ab.[нарастающий итог] >= 0.8 and ab.[нарастающий итог] <=0.95 then 'B' \
                else 'C' end as top_rate\
                from(\
                select a.phone_name\
                ,a.[total phone]\
                ,b.[sum(price)] \
                ,a.[total phone]/b.[sum(price)]   as share\
                ,sum(a.[total phone]/b.[sum(price)]) over(order by a.[total phone]/b.[sum(price)] desc) as [нарастающий итог]\
                from\
                (\
                select p.phone_name,sum(s.price) as [total phone]\
                from purchases s\
                left join phones p on s.phone_id = p.phone_id\
                left join brands b on p.brand_id = b.brand_id\
                where YEAR(s.date_purch) =2019 and month(s.date_purch) between 1 and 3\
                group by p.phone_name )a\
                ,(select sum(price) as [sum(price)] from purchases where YEAR(date_purch) =2019 and month(date_purch) between 1 and 3)b\
                --order by share desc\
                )ab\
                order by top_rate\
                ")







while 1:
    row = cursor.fetchone()
    if not row:
        break
    for i in row:
        print(i)
connection_to_db.close()
