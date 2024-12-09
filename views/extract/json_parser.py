import json
import pandas as pd
from pyspark.sql import SparkSession
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\sanket.upadhyay\AppData\Local\Programs\Python\Python39\python.exe'  #loaded python version for execution with pyspark

def load_freshwater_json(json_file_path,spark):

    # Load JSON data into a dictionary
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Prepare a list to hold the records
    records = []

    # Iterate through each country in the JSON data
    for country_name, country_data in data.items():
        country_code = country_data.get("Country Code")

        # Iterate through each year and its value
        for year in range(1960, 2024):
            value = country_data.get(str(year), None)  # Get value or None if not present
            # Convert empty strings to None
            if value == "":
                value = None
            records.append({
                "Country Name": country_name,
                "Country Code": country_code,
                "Year": year,
                "Freshwater Consumption": float(value) if value is not None else 0.0  # Convert to float or None
            })

    # Create a Pandas DataFrame from the records
    df = pd.DataFrame(records)

    # Convert the Pandas DataFrame to a Spark DataFrame without the index
    freshwater_df = spark.createDataFrame(df)

    # Show the Spark DataFrame
    freshwater_df.show(150, truncate=False)
    return freshwater_df
