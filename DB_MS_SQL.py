import pyodbc

connection_to_db = pyodbc.connect(
    r'Driver={SQL Server};Server=DESKTOP-JP8IK62\SQLEXPRESS;Database=P_STORE;Trusted_Connection=yes;')
cursor = connection_to_db.cursor()
#
# cursor.execute('select min(weight) as min, max(weight) as max,avg(weight) as avg,sum(weight)/count(weight) as averOfWeight'
#                ', sum(weight)/count(*)as averOfWeighTwo from m_phones')
# cursor.execute('select brand_name, phone_name, battery_capacity/weight as phoneBatteryCapacity from m_phones')
#
# cursor.execute('select top 15 percent * from m_phones order by battery_capacity desc')
#
# cursor.execute('select * from m_phones order by battery_capacity desc offset 2 rows fetch next 4 rows only')
#
# cursor.execute('select brand_name,battery_type, avg(price) as avgPriceBatteryType, count(phone_name) as quantityPhones \
#                from m_phones where battery_capacity > 3000\
#                group by brand_name, battery_type having count(phone_name) >= 2 order by brand_name ')
#
# cursor.execute('select avg(mp) from (select color_id,max(price) as mp from phone_price group by color_id) a')
#
# cursor.execute('select * from phone_price p1 where p1.price \
#                any (select p2.price from phone_price p2 where p2.merchant_id =1) and p1.merchant_id =1 order by p1.price')
#
# cursor.execute('select * from phone_price p1 where p1.price any (select p2.price from phone_price p2 \
#                where p2.merchant_id =1) and p1.merchant_id =1 order by p1.price')
#
# cursor.execute('select * from phones left join phone_comment on phones.phone_id = phone_comment.phone_id \
#                 where exists (select phone_comment.enthusiastic \
#                from phone_comment where phones.phone_id = phone_comment.phone_id and  phone_comment.enthusiastic > 100')
#
# cursor.execute('select phone_price.phone_id,phone_price.color_id from phone_price where phone_price.merchant_id = 1 \
#                 intersect select phone_price.phone_id,phone_price.color_id from phone_price where phone_price.merchant_id = 8')
#
# cursor.execute('select cl.client_name as clientName \
#                 ,p.date_purch as datePurch \
#                 ,b.Brand_name as brandName \
#                 ,ph.phone_name as phoneName \
#                 ,c.color_name as colorName \
#                 ,p.price as price \
#                 from purchases p \
#                 left join clients cl on p.client_id = cl.client_id \
#                 left join phones ph on p.phone_id = ph.phone_id \
#                 left join brands b on ph.brand_id = b.brand_id \
#                 left join colors c on p.color_id = c.color_id \
#                 where p.client_id in (select purchases.client_id \
#                 from purchases \
#                 group by purchases.client_id \
#                 having avg(purchases.price) > 3000 and count(purchases.date_purch)>= 2)  ')
#
#
#
# cursor.execute('select brand_name, phone_name, memory, price, \
#                 count(phone_name)over(partition by memory) as quantityPhonesWithMemory,\
#                 avg(price)over(partition by memory) as quantityPhonesWithPrice \
#                 from m_phones where brand_name in (?)',('apple'))


# cursor.execute('select brand_name, brand_id , color_name, color_id from brands,colors where reg_country in (?)',('Украина'))

#
# cursor.execute("select iif(grouping(brand_name)=1,'итого',brand_name) ,iif(grouping(battery_type)=1,'итого',battery_type) as battery_type\
#                 ,iif(grouping(ram)=1,'итого',str(ram)) as ram \
#                 ,count(*) from M_PHONES \
#                 where brand_name in('huawei','xiaomi') and ram in (4,8) \
#                 group by brand_name, battery_type,ram with rollup \
#                 order by brand_name, battery_type,ram ")

# cursor.execute("select brand_name, phone_name,memory,price,\
#                 row_number() over(order by memory, price) as rowNumberMemoryPrice,\
#                 row_number() over(order by price desc) as rowNumberPriceDesc\
#                 from m_phones\
#                 where brand_name in ('samsung') and price is not null\
#                 order by memory, price")


# cursor.execute("select pp.price,m.merchant_name,b.Brand_name,p.phone_name\
#                 ,row_number() over( order by merchant_name, brand_name , phone_name) as [row_number_table]\
#                 ,row_number() over(partition by merchant_name order by merchant_name, brand_name , phone_name) as [row_number_merchant_name]\
#                 ,row_number() over(partition by merchant_name, brand_name order by merchant_name, brand_name , phone_name) as [row_number_merchant_name_brand_name]\
#                 ,row_number() over(partition by  brand_name order by merchant_name, brand_name , phone_name) as [row_number_merchant_name_brand_name]\
#                 from phone_price pp \
#                 left join merchants m on pp.merchant_id = m.merchant_id\
#                 left join phones p on pp.phone_id = p.phone_id\
#                 left join brands b on p.brand_id = b.brand_id\
#                 order by m.merchant_name, b.brand_name, p.phone_name")

# cursor.execute("select os  ,CHARINDEX('.',os) as [CHARINDEX '.']\
#                 ,CHARINDEX('v',os) as [CHARINDEX 'v']\
#                 ,1*SUBSTRING(os,CHARINDEX('v',os)+1,CHARINDEX('.',os)-CHARINDEX('v',os)-1) as [CHARINDEX'v'_CHARINDEX'.']\
#                 ,1*SUBSTRING(os,CHARINDEX('.',os)+1,1) as [CHARINDEX'.'_+1]\
#                 from phones where os like 'android v%'\
#                 order by 1*SUBSTRING(os,CHARINDEX('v',os)+1,CHARINDEX('.',os)-CHARINDEX('v',os)-1) ,1*SUBSTRING(os,CHARINDEX('.',os)+1,1) ")


# cursor.execute("select client_name\
#                 ,TRIM(LOWER(SUBSTRING(client_name,1,5))) as [TRIM(LOWER(SUBSTRING(client_name,1,5)))]\
#                 ,REVERSE(TRIM(LOWER(SUBSTRING(client_name,1,5)))) as [REVERSE (TRIM(LOWER(SUBSTRING(client_name,1,5))))]\
#                 from clients\
#                 where TRIM(LOWER(SUBSTRING(client_name,1,5))) = REVERSE(TRIM(LOWER(SUBSTRING(client_name,1,5))))")


# cursor.execute("select os \
#                 ,REPLACE(os,'v','версия') as [REPLACE]\
#                 from phones \
#                 where os like 'android v%'")

#
# cursor.execute("select isnull (battery_capacity,0) as battery_capacity,isnull(battery_type,'Нет данных') as battery_type from phones p")


# cursor.execute("select case \
#                 when a.battery_capacity = 0 then 'Нет данныx'\
#                 when a.battery_capacity < 1000 then '0-1000'\
#                 when a.battery_capacity < 5000 then '1000-5000'\
#                 when a.battery_capacity < 10000 then '5000-10000'\
#                 when a.battery_capacity >= 10000 then '>10000'\
#                 else '____'\
#                 end as battery_capacity\
#                 ,battery_type\
#                 from\
#                 (select isnull (battery_capacity,0) as battery_capacity,isnull(battery_type,'Нет данных') as battery_type from phones p )a\
#                 order by battery_capacity  ")
#
# cursor.execute("select b.battery_capacity \
#                 ,count(b.battery_capacity) as [count battery capacity] \
#                 ,sum(IIF(TRIM(b.battery_type)='Li-Ion',1,0)) as [sum Li-Ion] \
#                 ,sum(IIF(TRIM(b.battery_type)='Li-Pol',1,0)) as [sum Li-Pol] \
#                 from ( \
#                 select case \
#                 when a.battery_capacity = 0 then 'Нет данныx' \
#                 when a.battery_capacity < 1000 then '0-1000' \
#                 when a.battery_capacity < 5000 then '1000-5000' \
#                 when a.battery_capacity < 10000 then '5000-10000' \
#                 when a.battery_capacity >= 10000 then '>10000' \
#                 else '____' \
#                 end as battery_capacity \
#                 ,battery_type \
#                 from \
#                 (select isnull (battery_capacity,0) as battery_capacity,isnull(battery_type,'Нет данных') as battery_type from phones p )a \
#                 ) b  group by b.battery_capacity")
#
#
# cursor.execute("select conn_type01, conn_type, conn_type2, conn_type3,conn_type0 \
#                 ,COALESCE(conn_type01, conn_type, conn_type2, conn_type3,'нет данных')\
#                 from phones")
#
# cursor.execute("select phone_name,main_camera from phones\
#                 where NULLIF(main_camera,0) is not null")


# cursor.execute("select \
#                     b.brand_name \
#                     ,count(*) as q_sell \
#                     ,sum(s.price) as tot_grn \
#                 from purchases s \
#                     left join phones p on s.phone_id=p.phone_id \
#                     left join brands b on p.brand_id=b.brand_id \
#                 where s.phone_id=916472 and s.color_id=17 \
#                     and year(s.date_purch)=2019 \
#                     --and month(date_purch) between 10 and 12 \
#                     and datepart(qq,date_purch)=4 \
#                     and datepart(mm,date_purch)=12 \
#                     and datepart(dd,date_purch)=31 \
#                 group by b.brand_name \ ")
#
# cursor.execute("select c.birth_date,count(*),datepart(qq,c.birth_date)from clients c\
#                     where year(c.birth_date)>= 1990 and  year(c.birth_date) <=1995\
#                     group by c.birth_date")
#
#
# cursor.execute("select * from purchases\
#                 where date_purch >= DATETIMEFROMPARTS(2019,8,20,15,00,00,000)\
#                 and date_purch <= DATETIMEFROMPARTS(2019,8,21,09,00,00,000)\
#                 order by date_purch")
#
# cursor.execute("select purchases.*, clients.*\
#                 ,ABS(DATEDIFF(yy,purchases.date_purch,clients.birth_date))\
#                 from purchases\
#                 join clients on purchases.client_id = clients.client_id\
#                 where MONTH(purchases.date_purch) = MONTH(clients.birth_date) and DAY(purchases.date_purch) = DAY(clients.birth_date)")
#
# cursor.execute("select DATEADD(mm,DATEDIFF(mm,1,GETDATE()),0)\
#                 , DATEADD(mm,1+DATEDIFF(mm,1,GETDATE()),0)\
#                 ,datediff(dd,DATEADD(mm,DATEDIFF(mm,1,GETDATE()),0),DATEADD(mm,1+DATEDIFF(mm,1,GETDATE()),0))")
#
# cursor.execute("select p.phone_name from phones p\
#                 where ISNUMERIC(SUBSTRING(p.phone_name,patindex('_%',p.phone_name ),1))=1")
#
# cursor.execute("select p.price from phone_price p\
#                 where abs(p.price-6000) < 500\
#                 order by p.price")
#
# cursor.execute("SELECT case\
#                 when rnd >= 0 and rnd <= 0.33333 then '0'\
#                 when rnd > 0.33333 and rnd <= 0.66666 then '100'\
#                 when rnd >0.66666 and rnd <= 1 then '200'\
#                 else '___'\
#                 end as k\
#                 from (select RAND() as rnd) a")
#
# cursor.execute("select price \
#                 ,TRIM (FORMAT(price,'C','ua-ua')) as [FORMAT(price,'C','ua-ua')]\
#                 ,TRIM (FORMAT(price / 25,'C','us-us')) as [FORMAT(price / 25,'C','us-us')]\
#                 ,TRIM (STR(price,4,1)) as [STR(price,4,1)]\
#                 from phone_price\
#                 ")
#
# cursor.execute("\
#                 select GETDATE()\
#                 ,CONVERT(smalldatetime,GETDATE())\
#                 ,CONVERT(date,GETDATE())\
#                 ,CONVERT(time,GETDATE())\
#                 ,CONVERT(datetime2,GETDATE())\
#                 ,CONVERT(datetimeoffset,GETDATE())")
#
# cursor.execute("select b.brand_name,count(*) as q_sell,sum(s.price) as total_grn\
#                 ,nullif (sum(case when month(s.date_purch) between 1 and 3 then s.price else 0 end),0) as first_quarter\
#                 ,nullif (sum(case when month(s.date_purch) between 4 and 6 then s.price else 0 end),0) as second_quarter\
#                 ,nullif (sum(case when month(s.date_purch) between 7 and 9 then s.price else 0 end),0) as third_quarter\
#                 ,nullif (sum(case when month(s.date_purch) between 10 and 12 then s.price else 0 end),0) as fourth_quarter\
#                 ,round (100 * nullif (sum(case when month(s.date_purch) between 4 and 6 then s.price else 0 end),0)\
#                 /nullif (sum(case when month(s.date_purch) between 1 and 3 then s.price else 0 end),0)-1,2) as increase_decrease_2_to_1\
#                 ,round (100 * nullif (sum(case when month(s.date_purch) between 7 and 9 then s.price else 0 end),0)\
#                 /nullif (sum(case when month(s.date_purch) between 4 and 6 then s.price else 0 end),0)-1,2) as increase_decrease_3_to_2\
#                 ,round (100 * nullif (sum(case when month(s.date_purch) between 10 and 12 then s.price else 0 end),0)\
#                 /nullif (sum(case when month(s.date_purch) between 7 and 9 then s.price else 0 end),0)-1,2) as increase_decrease_4_to_3\
#                 from purchases s left join phones p on s.phone_id=p.phone_id\
#                 left join brands b on p.brand_id=b.brand_id\
#                 where year(s.date_purch)=2019\
#                 group by b.brand_name")
#
# cursor.execute("select * \
#                 ,(case when b.five < 0.8 then 'a'\
#                 when b.five < 0.95 then 'b'\
#                 else 'c'\
#                 end) as tab\
#                 from(select * \
#                 ,sum(a.four) over(order by a.four desc  rows between unbounded preceding and current row) five\
#                 from(select distinct t3.Brand_name\
#                 ,sum(t1.price) over(partition by t3.Brand_name) one\
#                 ,count(t1.price) over(partition by t3.Brand_name) two\
#                 ,sum(t1.price) over() three\
#                 ,sum(t1.price) over(partition by t3.Brand_name)/sum(t1.price) over() four\
#                 from purchases t1\
#                 left join phones t2 on t1.phone_id = t2.phone_id\
#                 left join brands t3 on t2.brand_id = t3.brand_id\
#                 where year(t1.date_purch)=2019 and month(t1.date_purch) between 1 and 3)a\
#                 )b")
#
# cursor.execute("select\
#                     a.merchant_name\
#                     ,count(a.phone_id) q_t\
#                     ,sum(a.tot_pr) as tot_pr\
#                     ,sum(a.q_s) as q_s\
#                     ,sum(a.tot_pr)/sum(a.q_s) as avg_ch\
#                 from (\
#                         select\
#                             m.merchant_name\
#                             ,s.phone_id\
#                             ,sum(s.price) as tot_pr\
#                             ,count(*) as q_s\
#                         from purchases s\
#                         left join merchants m on s.merchant_id=m.merchant_id\
#                         where year(s.date_purch)=2019 and month(s.date_purch) between 1 and 6\
#                         group by\
#                             m.merchant_name\
#                             ,s.phone_id\
#                     ) a\
#                 group by a.merchant_name\
#                 order by q_t desc\
#                 ")

while 1:
    row = cursor.fetchone()
    if not row:
        break
    for i in row:
        print(i)
connection_to_db.close()
