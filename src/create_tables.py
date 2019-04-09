import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Function creates a database.

    Using psycopg2 connection to the database is established, sparkify database is established
    with UTF8 encoding.

    Returns
    ------- 
    cur
        Database cursor.
    conn
        Database connection

    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Existing tables, created by create_tables() functions, are dropped from the database.

    Parameters
    ----------
    cur
        Database cursor.
    conn
        Database connection.
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Tables defined in the sql_queries.py are created in the database.

    Parameters
    ----------
    cur
        Database cursor.
    conn
        Database connection.
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function that creates the database, drops existing tables and creates new tables.
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()