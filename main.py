import psycopg2
import pygrametl
from pygrametl.datasources import SQLSource
from pygrametl.tables import CachedDimension, FactTable
from datetime import datetime
from warehouse_tables import create_warehouse_tables


fklub = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="stregsystem",
    user="myuser",
    password="mypassword"
)
warehouse = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="warehouse",
    user="myuser",
    password="mypassword"
)

connection_f = pygrametl.ConnectionWrapper(fklub)
connection_w = pygrametl.ConnectionWrapper(warehouse)
connection_f.execute('SET search_path TO stregsystem')

create_warehouse_tables(connection_w)

time_dimension = CachedDimension(
    name='time',
    key='time_id',
    attributes=['day', 'month', 'year', 'week', 'weekyear'],
    targetconnection=connection_w
)

product_dimension = CachedDimension(
    name='product',
    key='product_id',
    attributes=['product_name', 'type', 'category', 'price', 'from', 'to'],
    targetconnection=connection_w
)

member_dimension = CachedDimension(
    name='member',
    key='member_id',
    attributes=['active', 'account_created', 'gender', 'balance'],
    targetconnection=connection_w
)

fact_table = FactTable(
    name='sale',
    keyrefs=['time_id', 'product_id', 'member_id'],
    measures=['sale'],
    targetconnection=connection_w
)


name_mapping = 'time', 'product', 'member', 'sale'
sale_query = "SELECT * FROM stregsystem_sale"
sale_source = SQLSource(connection=connection_f, query=sale_query)


def dateTransform(row):
    date: datetime = row['timestamp']
    newRow = dict()
    newRow['day'] = date.day
    newRow['month'] = date.month
    newRow['year'] = date.year
    newRow['week'] = date.isocalendar()[1]
    newRow['weekyear'] = date.isocalendar()[0]
    time_dimension.insert(newRow)



for row in sale_source:
    dateTransform(row)


connection_f.commit()
connection_f.close()
connection_w.commit()
connection_w.close()
