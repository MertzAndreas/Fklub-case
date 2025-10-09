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


# DELETE WAREHOUSE TABLES IF THEY EXIST
connection_w.execute("DROP TABLE IF EXISTS sale")
connection_w.execute("DROP TABLE IF EXISTS time")
connection_w.execute("DROP TABLE IF EXISTS product")
connection_w.execute("DROP TABLE IF EXISTS member")
connection_w.commit()

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






def createDateShit():
    time_query = "SELECT * FROM stregsystem_sale"
    time_source = SQLSource(connection=connection_f, query=time_query)
    def dateTransform(row):
        date: datetime = row['timestamp']
        newRow = dict()
        newRow['day'] = date.day
        newRow['month'] = date.month
        newRow['year'] = date.year
        newRow['week'] = date.isocalendar()[1]
        newRow['weekyear'] = date.isocalendar()[0]
        time_dimension.ensure(newRow)
    for row in time_source:
        dateTransform(row)

def createMemberShit():
    member_query = "SELECT * FROM stregsystem_member"
    member_source = SQLSource(connection=connection_f, query=member_query)
    def memberTransform(row):
        newRow = dict()
        newRow['active'] = row['active']
        newRow['account_created'] = row['year']
        newRow['gender'] = row['gender']
        newRow['balance'] = row['balance']
        member_dimension.ensure(newRow)
    for row in member_source:
        memberTransform(row)

createMemberShit()
createDateShit()

connection_f.commit()
connection_f.close()
connection_w.commit()
connection_w.close()
