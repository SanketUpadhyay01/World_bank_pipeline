
from pyspark.sql.functions import expr

def clean_metadata_csv(metadata_file_path, spark):

    """ Load Metadata from CSV file and clean it """

    # load CSV file
    metadata_df = spark.read.csv(metadata_file_path, header=True, inferSchema=True)

    # drop the column "special notes"
    metadata_df = metadata_df.drop("SpecialNotes", "_c5")
    # rename the column "table name" to "Country"
    metadata_df = metadata_df.withColumnRenamed("TableName", "Country Name")

    # reorder the columns to make "Country" the first column
    columns = metadata_df.columns
    first_column = columns[0]
    other_columns = columns[1:]
    new_column_order = ["Country Name",first_column] + [col for col in other_columns if col != "Country Name"]
    metadata_df = metadata_df.select(new_column_order)

    # drop rows with any null values
    metadata_df = metadata_df.dropna()

    print(metadata_df)

    return metadata_df


def load_population_csv(csv_file_path, spark):

    """Load Population data from CSV into dataframe"""

    population_df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
    population_df.drop("_c68","Indicator Name","Indicator Code")
    years = population_df.columns[4:]

    # create dataframe structure to hold melted df
    melted_dfs = []
    for year in years:
        melted_df = population_df.select(
            expr(f"`Country Name`"),
            expr(f"`Country Code`"),
            expr(f"'{year}'").alias("Year"),
            expr(f"`{year}`").alias("Total Population")
        )
        melted_dfs.append(melted_df)

    final_population_df = melted_dfs[0]

    for melted_df in melted_dfs[1:]:
        final_population_df = final_population_df.union(melted_df)

    # drop null column values
    final_population_df = final_population_df.dropna(subset=["Total Population"])
    # sort by country name and year
    final_population_df = final_population_df.sort("Country Name", "year")

    return final_population_df