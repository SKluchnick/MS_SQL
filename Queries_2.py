import pyodbc

connection_to_db = pyodbc.connect(
    r'Driver={SQL Server};Server=;Database=;Trusted_Connection=yes;')
cursor = connection_to_db.cursor()

cursor.execute("select p.maker,a.* from Product p\
                join \
                (select * \
                ,(select max(price) from Laptop l2 \
                join Product p1 on l2.model = p1.model\
                where p1.maker = (select p2.maker from Product p2 where p2.model = l1.model)) max\
                ,(select min(price) from Laptop l2 \
                join Product p1 on l2.model = p1.model\
                where p1.maker = (select p2.maker from Product p2 where p2.model = l1.model)) min\
                ,(select avg(price) from Laptop l2 \
                join Product p1 on l2.model = p1.model\
                where p1.maker = (select p2.maker from Product p2 where p2.model = l1.model)) mavg\
                from Laptop l1)a\
                on a.model = p.model")


cursor.execute("select *\
                from laptop L1\
                cross apply\
                (select max(price) max_price, min(price) min_price  from Laptop L2\
                join  Product P1 on L2.model=P1.model \
                where maker = (select maker from Product P2 where P2.model= L1.model)) X;")

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