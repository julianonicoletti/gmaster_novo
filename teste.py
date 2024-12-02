import pymysql

connection = pymysql.connect(
    host='localhost',
    user='root',
    password='1234',
    database='sakila',
    port=3306
)

print("Conexão bem-sucedida!")
connection.close()