import pyodbc

connection_to_db = pyodbc.connect(
    r'Driver={SQL Server};Server=;Database=;Trusted_Connection=yes;')
cursor = connection_to_db.cursor()

cursor.execute("create function print_memory(@count_value numeric) \
                returns nchar(15) begin if @count_value is null return '_' if @count_value in(512,256,128,16,8,1,0)  \
                return 'гигабайт' if @count_value in(64,32,4,2) return 'гигабайта'return 'гб'end")

cursor.commit()

cursor.execute("drop function print_memory")
cursor.commit()

cursor.execute("create function select_client_id_10(@client_tlf nchar(12)) returns table \
                return select client_name,client_id,client_tlf,birth_date \
                from clients where client_tlf = @client_tlf")
cursor.commit()

cursor.execute("select * from dbo.select_client_id_10('380710748067')")

cursor.execute("drop function select_client_id_10")
cursor.commit()

cursor.execute("create function my_period_number_of_wk(@start date,@finish date) returns @week_number table (p_week smallint)\
                begin \
                declare @current_date date \
                set @current_date = @start \
                while DATEDIFF(WK,@current_date,@finish)>=0 \
                begin \
                insert into @week_number(p_week) values (DATEPART(wk,@current_date)) \
                set @current_date=dateadd(wk,1,@current_date) \
                end \
                return \
                end")
cursor.commit()

cursor.execute("select * from dbo.my_period_number_of_wk('2019-01-01 00:00:00','2019-12-31 00:00:00')")

cursor.execute("drop function my_period_number_of_wk")
cursor.commit()

while 1:
    row = cursor.fetchone()
    if not row:
        break
    for i in row:
        print(i)
connection_to_db.close()



