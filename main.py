import psycopg2
import pygrametl


pgconn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="mydb",
    user="myuser",
    password="mypassword"
)

connection = pygrametl.ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute('set search_path to pygrametlexa')
connection