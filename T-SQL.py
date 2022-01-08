import pyodbc

connection_to_db = pyodbc.connect(
    r'Driver={SQL Server};Server=;Database=;Trusted_Connection=yes;')
cursor = connection_to_db.cursor()

cursor.execute("create table result_table(phone_id int, numOfSell int )")
cursor.commit()


cursor.execute("declare @current_year int \
                set  @current_year = 2019 \
                declare @qu_ph int \
                declare @cur_ph_id int \
                declare @qu_sell int \
                declare @current_month int \
                declare @cur_ph_id_m int \
                declare @count int \
                set @cur_ph_id = 1600000 \
                while isnull(@cur_ph_id,0)!=0 \
                begin \
                select @cur_ph_id = min(distinct tm.phone_id) from purchases tm where year(tm.date_purch) = @current_year and tm.phone_id > @cur_ph_id \
                set @count = 0 \
                set @cur_ph_id_m = 0 \
                set @current_month = 1 \
                while  @current_month <= 12 \
                begin  \
                select @qu_sell = count(t1.price) from purchases t1 where year(t1.date_purch) = @current_year and t1.phone_id = @cur_ph_id \
                and month(t1.date_purch) = @current_month \
                set @current_month += 1 \
                if @qu_sell <= @cur_ph_id_m \
                begin \
                break \
                end \
                else \
                begin \
                set  @cur_ph_id_m =  @qu_sell \
                set @count +=1 \
                end \
                end \
                if @count >= 3 \
                insert into result_table(phone_id,numOfSell ) values (@cur_ph_id,@count) \
                end \
")
cursor.commit()

cursor.execute("select phone_id,numOfSell from result_table ")

while 1:
    row = cursor.fetchone()
    if not row:
        break
    for i in row:
        print(i)
connection_to_db.close()

cursor.execute("drop table result_table")
cursor.commit()

