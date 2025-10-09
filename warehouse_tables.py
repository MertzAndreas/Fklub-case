
from pygrametl import ConnectionWrapper


def create_warehouse_tables(connection_w : ConnectionWrapper):
    """Create the tables in the data warehouse if they do not exist."""

    tables_sql = [
        """
        CREATE TABLE IF NOT EXISTS time (
            time_id SERIAL PRIMARY KEY,
            day INTEGER,
            month INTEGER,
            year INTEGER,
            week INTEGER,
            weekyear INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS product (
            product_id SERIAL PRIMARY KEY,
            product_name TEXT,
            type TEXT,
            category TEXT,
            price NUMERIC,
            "from" DATE,
            "to" DATE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS member (
            member_id SERIAL PRIMARY KEY,
            active BOOLEAN,
            account_created INTEGER,
            gender TEXT,
            balance NUMERIC
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS sale (
            sale_id SERIAL PRIMARY KEY,
            time_id INTEGER REFERENCES time(time_id),
            product_id INTEGER REFERENCES product(product_id),
            member_id INTEGER REFERENCES member(member_id),
            sale NUMERIC
        )
        """
    ]
    for sql in tables_sql:
        connection_w.execute(sql)
    connection_w.commit()
