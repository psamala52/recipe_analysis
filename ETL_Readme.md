# ETL Process README

## Overview

This document provides a detailed explanation of the ETL (Extract, Transform, Load) process for the dataset,including the approach taken, data exploration insights, assumptions made, and instructions.

This ETL process consists of two main tasks:
1. Pre-processing raw recipe data.
2. Analyzing the pre-processed data to calculate average cooking times based on difficulty levels for recipes that include beef.


## Approach

1. **Extract**: 
   - Extracted the datasets from the provided Git repository.
   - Loaded the Json dataset as an input file on Pycharm.
   - Utilized spark concepts to read the given Json files by providing absolute path.

2. **Transform**:
   - Dropped the unwanted columns, null and duplicate rows.
   - Checked if there are any entries in prepTime or cookTime with less than a minute duration so as to convert from ISO format to numerical minutes.
   - Checked for null values or empty strings in prepTime or cookTime.
   - Parse the ISO 8601 duration strings in the `cookTime(min)` and `prepTime(min)` columns.
   - Saved preprocessed result set to parquet format
   - Extracted the recipes with 'beef' in their ingredients.
   - Calculated total_time by adding cookTime and prepTime and added it as another column.
   - Determine the difficulty level for each recipe based on the total cooking time:
     - **Easy**: Less than 30 minutes.
     - **Medium**: Between 30 and 60 minutes.
     - **Hard**: More than 60 minutes.
   - Calculate the average total cooking time for each difficulty level.

3. **Load**:
   - Saved the transformed dataset with columns `difficulty` and `avg_total_cooking_time` to a CSV file 
      in the `output` folder.

## Data Exploration

During data exploration, I examined the structure and contents of the dataset. Key observations included:
- Duration strings in `cookTime` and `prepTime` columns are in ISO 8601 format. 
- Some rows may contain missing or malformed data, which required error handling during parsing and logged steps.

## Assumptions and Considerations

- According to the output expected, there's a lot of data that could be dropped off as it was irrelevant for the requested processing.
- Removed unwanted columns like `DatePublished`, `RecipeYield`, `url`, `image`, `description`.
- Checked for data entries if there were any `cookTime` or `prepTime` values less than a minute duration so as to convert the time into minutes or seconds or hours.
- As to the above applied filter, there were no such entries found so I converted all the time values into minutes.
- For recipes that had no prepTime or cookTime, I matched them with the existing names in the dataset to pull the `prepTime` and `cookTime` values.
- Malformed duration strings are handled gracefully, and affected rows are logged for further review.
- The difficulty levels are strictly based on the total cooking time duration, without considering other potential factors such as ingredient complexity.