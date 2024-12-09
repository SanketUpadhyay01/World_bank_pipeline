from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from views import transform_data, load_data
from constants.constants import (XML_FILE_PATH, JSON_FILE_PATH,
                                 METADATA_FILE_PATH, POPULATION_FILE_PATH)
from views import transform_data, load_data
from views.extract import (database_loader,json_parser,
                           xml_parser,csv_parser)
import streamlit as st
import time

def run_etl():
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("ETL pipeline") \
        .getOrCreate()

    # Extract data from sources
    with st.status("Extraction in progress...") as status:
        time.sleep(1)  # Simulate time taken for extraction
        gdp_df = xml_parser.load_gdp_xml(XML_FILE_PATH, spark)
        freshwater_df = json_parser.load_freshwater_json(JSON_FILE_PATH, spark)
        metadata_df = csv_parser.clean_metadata_csv(METADATA_FILE_PATH, spark)
        population_df = csv_parser.load_population_csv(POPULATION_FILE_PATH, spark)
        inflation_df = database_loader.load_sql_data(spark)
        st.success("Extraction process completed!")
        status.update(label="Extraction process complete!", state="complete")

    # Transform data into final version
    with st.status("Transformation in progress...") as status:
        time.sleep(1)  # Simulate time taken for transformation
        transformed_data = transform_data.create_final_dataframe(metadata_df, gdp_df,
                                                         population_df, inflation_df, freshwater_df)

        st.success("Transformation process completed!")
        status.update(label="Transformation process complete!", state="complete")

    # Load data into data warehouse
    with st.status("Loading in progress...") as status:
        time.sleep(1)  
        load_data.mysql_load_data(transformed_data)
        st.success("Loading process completed!")
        status.update(label="Loading process complete!", state="complete")

    # Stop Spark session
    spark.stop()
