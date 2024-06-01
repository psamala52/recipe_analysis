import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils import iso_to_minutes_udf, duplicate_null_drop


def preprocess_data(input_path, output_path):
    spark = SparkSession.builder.appName("RecipePreprocessing").getOrCreate()
    logging.info("Spark session created.")

    # Read source data for recipes datasets
    try:
        df1 = spark.read.json(input_path + '//recipes-000.json')
        df2 = spark.read.json(input_path + '//recipes-001.json')
        df3 = spark.read.json(input_path + '//recipes-002.json')
        logging.info(f"Datasets read from given path {input_path}.")
    except Exception as e:
        logging.info(f"Datasets reading failed from the given path {input_path}.")
        print(f"Error reading file : {e}")

    if not df1 or not df2 or not df3:
        logging.info("Datasets not able read from given path.")
        raise FileNotFoundError("File could be read. Please check the file paths.")
    else:
        print("Original Combined Dataset")
        df = df1.union(df2).union(df3)
        logging.info("Datasets Combined.")

        # Dropping off the unwanted columns
        unwanted_columns = ["datePublished", "description", "image", "url", "recipeYield"]
        df = df.drop(*unwanted_columns)
        df.show()
        # Add columns with durations in seconds
        df = df.withColumn("cookTime(min)", iso_to_minutes_udf(col("cookTime"))) \
            .withColumn("prepTime(min)", iso_to_minutes_udf(col("prepTime")))
        logging.info("Added Minimum cookTime and Minimum prepTime Columns")

        # Removing repeated columns
        not_required = ["cookTime", "prepTime"]
        df = df.drop(*not_required)
        df.show()
        logging.info("Removed cookTime and prepTime Column")

        df = duplicate_null_drop(df)
        logging.info("Removed duplicate and null records from data")

        # Checking for records with no prepTime and cookTime
        empty_time_entries = df.filter((col("prepTime").isNull() | (col("prepTime") == "")) &
                                       (col("cookTime").isNull() | (col("cookTime") == "")))

        empty_time_entries.show(truncate=False)


        # Persist processed data
        df.write.mode("overwrite").parquet(output_path)
        logging.info("Processed data written to output path.")

    spark.stop()