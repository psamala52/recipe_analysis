import unittest, os
from pyspark.sql import SparkSession, Row
from src.preprocess import preprocess_data
from src.analysis import analyze_data

class PreprocessTest(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test").getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_preprocess_data(self):
        # Implement test logic for preprocess
        # Create sample JSON data
        data1 = [
            Row(ingredients="beef, tomato, salt", prepTime="PT20M", cookTime="PT30M", 
                datePublished="2020-01-01", description="desc", image="image1", url="url1", recipeYield="2"),
        ]
        data2 = [
            Row(ingredients="chicken, tomato, salt", prepTime="PT15M", cookTime="PT25M", 
                datePublished="2020-02-01", description="desc", image="image2", url="url2", recipeYield="4"),
        ]
        data3 = [
            Row(ingredients="beef, onion, garlic", prepTime="PT10M", cookTime="PT20M", 
                datePublished="2020-03-01", description="desc", image="image3", url="url3", recipeYield="3"),
        ]
        df1 = self.spark.createDataFrame(data1)
        df2 = self.spark.createDataFrame(data2)
        df3 = self.spark.createDataFrame(data3)
        
        # Write sample data to JSON
        input_path = "test_input"
        os.makedirs(input_path, exist_ok=True)
        df1.write.mode("overwrite").json(os.path.join(input_path, "recipes-000.json"))
        df2.write.mode("overwrite").json(os.path.join(input_path, "recipes-001.json"))
        df3.write.mode("overwrite").json(os.path.join(input_path, "recipes-002.json"))
        
        # Define output path
        output_path = "test_output/"
        
        # Call the preprocess_data function
        preprocess_data(input_path, output_path)
        
        # Read the processed data
        processed_df = self.spark.read.parquet(output_path)
        
        # Define expected results
        expected_data = [
            Row(ingredients="beef, tomato, salt", prepTime_min=20, cookTime_min=30),
            Row(ingredients="beef, onion, garlic", prepTime_min=10, cookTime_min=20),
        ]
        expected_df = self.spark.createDataFrame(expected_data)
        
        # Verify the results
        self.assertEqual(processed_df.select("ingredients", "prepTime(min)", "cookTime(min)").collect(), 
                        expected_df.collect())

class AnalysisTest(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test").getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_analyze_data(self):
        # Implement test logic for analysis
        # Create a test DataFrame
        data = [
            Row(ingredients="beef, tomato, salt", prepTime="10", cookTime="20"),
            Row(ingredients="chicken, tomato, salt", prepTime="15", cookTime="35"),
            Row(ingredients="beef, onion, garlic", prepTime="5", cookTime="10"),
            Row(ingredients="fish, lemon, salt", prepTime="20", cookTime="30"),
            Row(ingredients="beef, pepper, salt", prepTime="40", cookTime="30")
        ]
        df = self.spark.createDataFrame(data)

        # Write DataFrame to parquet (simulate input data)
        input_path = "test_input.parquet"
        df.write.mode("overwrite").parquet(input_path)

        # Define output path
        output_path = "test_output/"

        # Call the function to test
        analyze_data(input_path, output_path)

        # Read the output data
        result_df = self.spark.read.csv(os.path.join(output_path, "avg_cooking_time_per_difficulty.csv"), header=True)

        # Define expected results
        expected_data = [
            Row(Difficulty="easy", Avg_total_cooking_time="15.0"),
            Row(Difficulty="medium", Avg_total_cooking_time="40.0"),
            Row(Difficulty="hard", Avg_total_cooking_time="70.0")
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        # Verify the results
        self.assertEqual(result_df.collect(), expected_df.collect())

if __name__ == "__main__":
    unittest.main()
