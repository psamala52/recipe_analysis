import os
import logging
from preprocess import preprocess_data
from analysis import analyze_data

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(filename='../logs//info.log', level=logging.INFO)
    logger.info('Started')
    input_path = "/Users/apple/PycharmProjects/recipe_analysis/data/input/"
    processed_path = "/Users/apple/PycharmProjects/recipe_analysis/data/output/processed"
    output_path = "/Users/apple/PycharmProjects/recipe_analysis/data/output/RESULTS/"

    # Preprocess data
    preprocess_data(input_path, processed_path)

    # Analyze data
    analyze_data(processed_path, output_path)
    logger.info('Finished')


if __name__ == "__main__":
    main()

