import psycopg2
import pygrametl
from pygrametl.datasources import SQLSource
from pygrametl.tables import CachedDimension, FactTable, SCDimension
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

# ENSURE THE TABLES EXIST
create_warehouse_tables(connection_w)

# DEFINE DIMENSIONS AND FACTS
time_dimension = CachedDimension(
    name='time',
    key='time_id',
    attributes=['day', 'month', 'year', 'week', 'weekyear'],
    targetconnection=connection_w
)

product_dimension = SCDimension(
    name='product',
    key='product_id',
    attributes=['product_name', 'type', 'category', 'price', 'version', 'from_date', 'to_date'],
    lookupatts=['product_name'],  
    fromatt='from_date',
    toatt='to_date',
    fromfinder=pygrametl.datereader("from_date"),
    versionatt='version',
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

#createMemberShit()
#createDateShit()


def createProductShit():
    product_query = """
        SELECT 
            p.id AS product_id,
            p.name AS product_name,
            c.name AS category,
            p.price,
            oldprice.changed_on AS changed_on
        FROM stregsystem.stregsystem_product AS p
        LEFT JOIN stregsystem.stregsystem_product_categories AS pc 
            ON p.id = pc.product_id AND pc.category_id != 10
        LEFT JOIN stregsystem.stregsystem_category AS c 
            ON pc.category_id = c.id
        LEFT JOIN stregsystem.stregsystem_oldprice AS oldprice
            ON p.id = oldprice.product_id
        WHERE c.id != 5 OR c.id IS NULL
    """

    member_source = SQLSource(connection=connection_f, query=product_query)

    def productTypeToCategory(type: str | None) -> str:
        if type == "Alkoholdie varer": 
            raise ValueError("Alkoholdie varer should not be in the dataset")
        if type == "Event":
            exit(1)
        if type is None:
            return "Ukategoriseret"
        cases = {
            "Sodavand" : "Sodavand",
            "Vitamin Vand" : "Andet",
            "Øl" : "Øl", 
            "Special Øl": "Øl", 
            "Kaffe": "Koffein", 
            "Hård spiritus": "Spiritus", 
            "Spiritus": "Spiritus",  
            "Spiselige varer": "Mad", 
            "Energidrik": "Koffein", 
        }
        return cases[type]


    def productTransform(row):
        newRow = dict()
        type = row['category']
        newRow['type'] = type
        newRow['category'] = productTypeToCategory(type)
        newRow['product_name'] = row['product_name']
        newRow['price'] = row['price'] 
        newRow['from_date'] = row['changed_on'] or datetime.now().date()
        newRow['to_date'] = None
        product_dimension.scdensure(newRow)


    for row in member_source:
        print(row)
        productTransform(row)

createProductShit()

connection_f.commit()
connection_f.close()
connection_w.commit()
connection_w.close()
