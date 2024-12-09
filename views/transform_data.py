from pyspark.sql.functions import col

def create_final_dataframe(metadata_df, gdp_df, population_df,inflation_df, freshwater_df):

    """ Create a final output by joining the different dataframes"""

    # Perform the joins
    combined_df = (metadata_df
        .join(gdp_df, on=['Country Code', 'Country Name'], how='inner')
        .join(population_df, on=['Country Code', 'Country Name', 'Year'], how='inner')
        .join(inflation_df, on=['Country Code', 'Country Name', 'Year'], how='inner')
        .join(freshwater_df, on=['Country Code', 'Country Name', 'Year'], how='inner')
        )

    # Select the desired columns
    final_df = combined_df.select(
        col('Country Name').alias('Country_Name'),
        col('Country Code').alias('Country_Code'),
        col('Region'),
        col('IncomeGroup'),
        col('Year'),
        col('Total Population').alias('Total_Population'),
        col('Freshwater Consumption').alias('Freshwater_Consumption'),
        col('GDP Data').alias('GDP_Data'),
        col('Total Inflation').alias('Total_Inflation')
    )

    final_df.drop("_c68")

    final_df = final_df.sort("Country Name", "Year")

    return final_df
