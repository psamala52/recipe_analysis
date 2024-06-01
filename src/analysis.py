import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when


def analyze_data(input_path, output_path):
    spark = SparkSession.builder.appName("RecipeAnalysis").getOrCreate()
    logging.info("Spark session created for analyze data.")

    # Read processed data
    df = spark.read.parquet(input_path)
    logging.info("Processed data read from processed path.")

    # Extract recipes with beef
    beef_recipes = df.filter(col("ingredients").contains("beef"))
    beef_recipes.show()
    logging.info("Extracted recipes with beef from dataset.")

    # Creating another column as total_time to sum prepTime and cookTime
    df = df.withColumn("total_time", col("prepTime(min)") + col("cookTime(min)"))
    df.show()
    logging.info("Created Total time column.")

    # Define conditions for assigning difficulty levels
    conditions = [
        (col("total_time") < 30),  # Easy
        (col("total_time") >= 30) & (col("total_time") <= 60),  # Medium
        (col("total_time") > 60)  # Hard
    ]

    # Define corresponding difficulty levels
    difficulty_levels = ["easy", "medium", "hard"]

    df = df.withColumn("difficulty_level",
                       when(conditions[0], difficulty_levels[0])
                       .when(conditions[1], difficulty_levels[1])
                       .otherwise(difficulty_levels[2]))

    # Calculating the average of total cooking time as per difficulty
    avg_time_df = df.groupBy("difficulty_level").avg("total_time").orderBy("avg(total_time)")
    avg_time_df = avg_time_df.withColumnRenamed("difficulty_level", "Difficulty") \
        .withColumnRenamed("avg(total_time)", "Avg_total_cooking_time")
    avg_time_df.show()

    # Saving for avg cooking time per difficulty level output as a csv file
    avg_time_df.write.mode('overwrite').csv(output_path, header=True)

