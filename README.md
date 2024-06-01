# Recipe Analysis

## Overview

This project processes and analyzes recipe data using Apache Spark. The application consists of two main tasks: preprocessing the raw data and performing analysis to extract insights about recipes containing beef and their cooking durations.

## Prerequisites

- Python 3.8+
- Apache Spark
Note: For Using Apache Spark Java Develop Kit(JDK) in mandatory to working with Apache Spark. 

## Setup

1. **Clone the repository:**
    ```sh
    git clone <project_repository_path>
    cd recipe_analysis
    ```

2. **Create a virtual environment and install dependencies:**
    ```sh
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

3. **Run the application locally:**
    ```sh
    python src/main.py
    ```

4. **Run the application using Docker:**
    ```sh
    docker build -t recipe-analysis .
    docker run recipe-analysis
    ```

## Instructions

- Ensure the input data (`recipes-000.json,recipes-001.json,recipes-002.json`) is located in the `input` folder.
- The processed data and analysis results will be written to the `output` folder.

## Testing

Unit tests are located in the `tests` directory. To run the tests:
```sh
pytest tests
