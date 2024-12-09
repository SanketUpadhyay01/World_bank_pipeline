
import os
import streamlit as st
import pandas as pd
import json
import xml.etree.ElementTree as ET
import pymysql  
from sparketl import run_etl  
from sqlalchemy import create_engine

# Create data directory if it doesn't exist
if not os.path.exists('data'):
    os.makedirs('data')

# Sidebar for file uploads
st.sidebar.header("Upload Data")

# Upload Population Data
population_file = st.sidebar.file_uploader("Upload Population CSV", type=["csv"])
if population_file is not None:
    population_df = pd.read_csv(population_file)
    population_df.to_csv('data/population.csv', index=False)
    st.sidebar.success("Population data uploaded successfully!")

# Upload Metadata Data
metadata_file = st.sidebar.file_uploader("Upload Metadata CSV", type=["csv"])
if metadata_file is not None:
    metadata_df = pd.read_csv(metadata_file)
    metadata_df.to_csv('data/metadata.csv', index=False)
    st.sidebar.success("Metadata data uploaded successfully!")

# Upload Freshwater Data
freshwater_file = st.sidebar.file_uploader("Upload Freshwater JSON", type=["json"])
if freshwater_file is not None:
    freshwater_data = json.load(freshwater_file)
    with open('data/freshwater.json', 'w') as json_file:
        json.dump(freshwater_data, json_file)
    st.sidebar.success("Freshwater data uploaded successfully!")

# Upload GDP Data
gdp_file = st.sidebar.file_uploader("Upload GDP XML", type=["xml"])
if gdp_file is not None:
    tree = ET.parse(gdp_file)
    root = tree.getroot()
    tree.write('data/gdp.xml')
    st.sidebar.success("GDP data uploaded successfully!")

# Main page content
st.title("ETL Pipeline Application")
st.write("This application allows you to upload data of different formats and run an ETL pipeline.")

# Button to run ETL process
if st.sidebar.button("Process Data"):
    # Collapse the sidebar
    st.sidebar.empty()  

    st.title("Progress status")

    with st.spinner("Running ETL process..."):
        run_etl()  
        st.success("ETL process completed!")


def fetch_final_data():
        st.title("Final Data-table")
        # Database connection parameters
        db_host = 'localhost'
        db_user = 'root'
        db_password = 'root'
        db_name = 'world_data'
        table_name = 'reporting_layer_data'

        engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}')

        # Query to fetch data
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, engine)

        return df


# Display the final table
if st.sidebar.button("Show Final Data"):
    final_data_df = fetch_final_data()
    st.dataframe(final_data_df)

    # Button to download the DataFrame as CSV
    csv = final_data_df.to_csv(index=False)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name='reporting_final_data.csv',
        mime='text/csv'
    )
