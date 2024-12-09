import pymysql

def create_connection():

    """ Load cleaned and transformed data into warehouse for backup"""

    connection = pymysql.connect(
        host="localhost",
        user="root",
        password="root",
        database="world_data"
    )

    return connection
