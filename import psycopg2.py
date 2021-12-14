import psycopg2
import sys
import pandas as pd

# Here you want to change your database, username & password according to your own values
param_dic = {
    "host"      : "localhost",
    "database"  : "movement",
    "user"      : "simon",
    "password"  : "Babbage65?"
}

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    return conn


def single_insert(conn, insert_req):
    """ Execute a single INSERT request """
    cursor = conn.cursor()
    try:
        cursor.execute(insert_req)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()


# Connecting to the database
conn = connect(param_dic)

# Inserting each row
for i in df.index:

    query = """
    INSERT into emissions(column1, column2, column3) values('%s',%s,%s);
    """ % (dataframe['column1'], dataframe['column2'], dataframe['column3'])
    single_insert(conn, query)

# Close the connection
conn.close()