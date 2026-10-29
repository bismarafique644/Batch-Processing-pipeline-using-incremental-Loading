import pymysql
import os

connection = pymysql.connect(
    host="127.0.0.1",
    port=3306,
    user="ecommerce_app",
    password=os.environ["MYSQL_PASSWORD"],
    database="ecommerce_api_db"
)

print("MySQL connected successfully!")

connection.close()