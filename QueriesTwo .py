import pyodbc

connection_to_db = pyodbc.connect(
    r'Driver={SQL Server};Server=DESKTOP-JP8IK62\SQLEXPRESS;Database=inc_out;Trusted_Connection=yes;')
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
