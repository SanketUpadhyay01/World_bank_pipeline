
from pyspark.sql.functions import expr

def load_gdp_xml(xml_file_path,spark):

    """Load XML file data into dataframe and clean it"""

    # Load XML file
    xml_data = spark.read.option("rowTag", "record").format("xml").load(xml_file_path)

    xml_transformed_df = xml_data.select(
        expr("field[0]._VALUE").alias("Country Name"),
        expr("field[0]._key").alias("Country Code"),
        xml_data.field[2]._VALUE.alias("Year"),
        xml_data.field[3]._VALUE.alias("GDP Data")

    )

    return xml_transformed_df