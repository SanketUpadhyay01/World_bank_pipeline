
from pyspark.sql.functions import col,expr,when
from dotenv import load_dotenv
import os

load_dotenv()

def load_sql_data(spark):

    """Load SQL data from MySQL using JDBC and convert into spark dataframe"""

    # read sql table into df
    inflation_df = spark.read \
        .format("jdbc") \
        .option("url", os.getenv("DB_JDBC_URL")) \
        .option("dbtable", os.getenv("DB_TABLE_NAME")) \
        .option("user", os.getenv("DB_USER")) \
        .option("password", os.getenv("DB_PASSWORD")) \
        .load()

    inflation_df.drop("Indicator Name","Indicator Code")

    #reaname headings
    new_column_names = {col: col.split('_')[1] for col in inflation_df.columns if col.startswith("year_")}
    renamed_df = inflation_df.selectExpr([f"`{col}` as `{new_column_names.get(col, col)}`" for col in inflation_df.columns])

    # extract years from df
    years = renamed_df.columns[4:]

    # convert years to numeric type
    new_inflation_df = []
    for year in years:
        new_inflation_df = renamed_df.withColumn(year, col(year).cast("float"))

    # melt df to convert years to row values
    melted_dfs = []
    for year in years:
        melted_df = new_inflation_df.select(
            expr("`Country Name`"),
            expr("`Country Code`"),
            expr(f"'{year}'").alias("Year"),
            expr(f"`{year}`").alias("Total Inflation")
        )
        melted_dfs.append(melted_df)

    # combine all melted df
    final_inflation_df = melted_dfs[0]
    for melted_df in melted_dfs[1:]:
        final_inflation_df = final_inflation_df.union(melted_df)

    # Drop rows with null values in the 'total_inflation_count' column
    final_inflation_df = final_inflation_df.select(
        *[when(col(c) == "", None).otherwise(col(c)).alias(c) for c in final_inflation_df.columns]
    )
    final_inflation_df = final_inflation_df.dropna(subset=["Total Inflation"])

    # Sort the final DataFrame by 'Country Name' and 'year'
    final_inflation_df = final_inflation_df.sort("Country Name", "Year")

    return final_inflation_df
