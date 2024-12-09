
from dotenv import load_dotenv
from model.database_connection import create_connection

load_dotenv()

def mysql_load_data(transformed_data):

    """ Load cleaned and transformed data into warehouse for backup"""

    connection = create_connection()

    # Get the number of rows
    rows = transformed_data.count()

    # Convert the Spark DataFrame to a list of tuples
    data_tuples = [tuple(row.asDict().values()) for row in transformed_data.collect()]

    # Iterate over the list of tuples and insert the data into the database
    with connection.cursor() as cursor:
        query = "INSERT INTO reporting_layer_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for data in data_tuples:
            cursor.execute(query, data)

    # Commit the transaction
    connection.commit()
    connection.close()
