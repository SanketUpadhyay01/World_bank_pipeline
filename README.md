# **ETL Pipeline for Country Data Analysis**

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline that processes data from multiple sources, including JSON, CSV, XML files, and a MySQL database.
The pipeline is designed to aggregate and transform data related to world countries, focusing on metrics such as GDP, freshwater consumption, and population over the last 20 years. 
The final transformed data is stored in a MySQL database for reporting purposes and can also be downloaded as a CSV file.

## Features
1. **Data Extraction:** Supports data extraction from various formats (JSON, CSV, XML) and a MySQL database.
2. **Staging Area:** Data is initially loaded into a staging area for preliminary processing.
3. **Data Transformation:** Utilizes PySpark to transform and clean the data into a single DataFrame.
4. **Data Loading:** The final transformed data is loaded into a MySQL database as a data warehouse.
5. **CSV Download:** Users can download the transformed data as a CSV file.
6. **Streamlit Frontend:** A user-friendly interface for uploading data files and initiating the ETL process.
7. **Progress Tracking:** Displays the progress of each stage of the ETL pipeline.

## Requirements
* Python 3.9
* PySpark
* MySQL Connector
* Streamlit
* Pandas
* Other necessary libraries (see requirements.txt)

## Installation
Clone the repository:
```
git clone https://github.com/SanketUpadhyay01/World_bank_pipeline
cd World_bank_pipeline
```

Install the required packages:
```
pip install -r requirements.txt
```

Set up your MySQL database by running scripts from the utils folder.

Update the database connection settings in the .env file.

## Running the Application
Start the Streamlit application:
```
streamlit run app.py
```
Open your web browser and navigate to http://localhost:8501.

Use the interface to upload your data files (JSON, CSV, XML) and initiate the ETL process.

Monitor the progress of each stage of the pipeline.

Once the process is complete, you can download the transformed data as a CSV file.

### Data Sources
Find the data sources in the data folder :
* JSON files containing freshwater data
* CSV files with population metrics and metadata
* XML files with GDP data
* MySQL database with inflation data
