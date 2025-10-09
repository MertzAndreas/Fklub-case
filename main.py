from datetime import datetime
import psycopg2
import pygrametl
from pygrametl.datasources import SQLSource 

pgconn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="stregsystem",
    user="myuser",
    password="mypassword"
)

connection = pygrametl.ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute('set search_path to stregsystem')

sale_query = "SELECT * FROM stregsystem_sale"
sale_source = SQLSource(connection=connection, query=sale_query)

def dateTransform(row):
    """Adds date parts to the row dictionary."""
    date: datetime = row['timestamp']
    row['day'] = date.day
    row['month'] = date.month
    row['year'] = date.year
    row['week'] = date.isocalendar()[1]
    row['weekyear'] = date.isocalendar()[0]

for row in sale_source:
    dateTransform(row) 
    print(row)       

connection.commit()
connection.close()