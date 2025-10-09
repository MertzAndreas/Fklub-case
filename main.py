import psycopg2
import pygrametl
from pygrametl.datasources import SQLSource
from pygrametl.tables import CachedDimension, FactTable, SCDimension
from datetime import datetime
from utils import clean_html, infer_product_type, normalize_liters, product_type_to_category, transform_date
from warehouse_tables import create_warehouse_tables

fklub = {
    'host': "localhost",
    'port': 5432,
    'dbname': "stregsystem",
    'user': "myuser",
    'password': "mypassword"
}

warehouse = {
    'host': "localhost",
    'port': 5432,
    'dbname': "warehouse",
    'user': "myuser",
    'password': "mypassword"
}

src_conn = pygrametl.ConnectionWrapper(psycopg2.connect(**fklub))
dst_conn = pygrametl.ConnectionWrapper(psycopg2.connect(**warehouse))
src_conn.execute('SET search_path TO stregsystem')

# Reset warehouse tables
for table in ['sale', 'time', 'product', 'member']:
    dst_conn.execute(f"DROP TABLE IF EXISTS {table}")
dst_conn.commit()
create_warehouse_tables(dst_conn)

time_dimension = CachedDimension(
    name='time',
    key='time_id',
    attributes=['day', 'month', 'year', 'week', 'weekyear'],
    targetconnection=dst_conn
)

product_dimension = SCDimension(
    name='product',
    key='product_sk',
    attributes=['product_id','product_name', 'type', 'category', 'price', 'version', 'from_date', 'to_date'],
    lookupatts=['product_id'],  
    fromatt='from_date',
    toatt='to_date',
    fromfinder=pygrametl.datereader("from_date"),
    versionatt='version',
    targetconnection=dst_conn
)

member_dimension = CachedDimension(
    name='member',
    key='member_id',
    attributes=['active', 'account_created', 'gender', 'balance'],
    targetconnection=dst_conn
)

fact_table = FactTable(
    name='sale',
    keyrefs=['time_id', 'product_sk', 'member_id'],
    measures=['sale'],
    targetconnection=dst_conn
)

UNKNOWN_MEMBER_ID = 99999999

def load_time_dimension():
    time_query = "SELECT * FROM stregsystem_sale"
    time_source = SQLSource(connection=src_conn, query=time_query)
    for row in time_source:
        date: datetime = row['timestamp']
        date_row = transform_date(date)
        time_dimension.ensure(date_row)


def load_member_dimension():
    member_query = "SELECT * FROM stregsystem_member"
    member_source = SQLSource(connection=src_conn, query=member_query)
    for row in member_source:
        newRow = dict()
        newRow['active'] = row['active']
        newRow['account_created'] = row['year']
        newRow['gender'] = row['gender']
        newRow['balance'] = row['balance'] / 100
        member_dimension.ensure(newRow)

    member_dimension.insert({'member_id': UNKNOWN_MEMBER_ID, 'active': False, 'account_created': 1970, 'gender': 'U', 'balance': 0.0})

def load_product_dimension():
    product_query = """
    WITH one_category AS (
        SELECT DISTINCT ON (pc.product_id)
            pc.product_id,
            pc.category_id
        FROM stregsystem.stregsystem_product_categories pc
        ORDER BY pc.product_id, pc.category_id
    )

    SELECT
        p.id AS product_id,
        p.name AS product_name,
        c.name AS category,
        COALESCE(oldprice.price, p.price) AS price,
        oldprice.changed_on AS changed_on
    FROM stregsystem.stregsystem_product AS p
    LEFT JOIN one_category oc ON p.id = oc.product_id
    LEFT JOIN stregsystem.stregsystem_category AS c ON oc.category_id = c.id
    LEFT JOIN stregsystem.stregsystem_oldprice AS oldprice ON p.id = oldprice.product_id
    """

    product_source = SQLSource(connection=src_conn, query=product_query)
    for row in product_source:
        newRow = dict()
        newRow['product_id'] = row['product_id']
        name = clean_html(row['product_name'])
        name = normalize_liters(name)

        type = row['category']
        if type == None:
            type = infer_product_type(name)

        newRow['type'] = type
        newRow['category'] = product_type_to_category(type)
        newRow['product_name'] = name
        newRow['price'] = row['price'] / 100
        newRow['from_date'] = row['changed_on'] or datetime.now().date()
        product_dimension.scdensure(newRow)


def load_sales_fact():
    product_query = """
        SELECT 
            member_id, 
            product_id, 
            timestamp 
        FROM stregsystem_sale
        WHERE member_id > 0
    """
    product_source = SQLSource(connection=src_conn, query=product_query)
    for row in product_source:
        newRow = dict()
        product_sk = product_dimension.lookupasof({'product_id': row['product_id']}, row['timestamp'].date(), (True, True))
        product_row = product_dimension.getbykey(product_sk)
        date_row = transform_date(row['timestamp'].date())
        timestamp_id = time_dimension.lookup(date_row)

        member = member_dimension.getbykey(row['member_id'])
        member_id = member['member_id'] 
        if member_id is None:
            member_id = UNKNOWN_MEMBER_ID
            
        newRow['member_id'] = member_id
        newRow['time_id'] = timestamp_id
        newRow['product_sk'] = product_sk
        newRow['sale'] = product_row['price']
        fact_table.insert(newRow)



def run_etl():
    load_member_dimension()
    load_time_dimension()
    load_product_dimension()
    load_sales_fact()
    dst_conn.commit()

if __name__ == "__main__":
    run_etl()
    src_conn.close()
    dst_conn.close()