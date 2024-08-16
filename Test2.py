
import os
import pytest
from unittest.mock import patch, mock_open

# Assuming the script you shared is named `setup.py` and the functions are in the same file
from setup import get_install_requirements

# Test for get_install_requirements function
@patch("builtins.open", new_callable=mock_open, read_data="[packages]\npackage1==1.0.0\n")
@patch("os.path.abspath")
@patch("os.path.dirname")
def test_get_install_requirements(mock_dirname, mock_abspath, mock_file):
    mock_dirname.return_value = "/path/to/dir"
    mock_abspath.return_value = "/path/to/dir"
    
    expected_requirements = ["package1==1.0.0"]
    
    result = get_install_requirements()
    
    assert result == expected_requirements

# Test for the version reading part
@patch("builtins.open", new_callable=mock_open, read_data="__version__ = '0.1.0'")
def test_version_reading(mock_file):
    from setup import version
    assert version == "0.1.0"

# Add more tests as necessary for other parts of the script

if __name__ == "__main__":
    pytest.main()

####$$$$$$$$$$$

import os
from pyspark.sql import SparkSession

# Define paths
input_folder = "s3://your-bucket/path/to/folder"  # Replace with your folder path
processed_files_log = "processed_files.log"  # Path to log file

# Function to get the list of files in the folder
def list_parquet_files(folder):
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith('.parquet')]
    return sorted(files)

# Function to read processed files from the log file
def read_processed_files(log_file):
    if os.path.exists(log_file):
        with open(log_file, "r") as file:
            return set(line.strip() for line in file.readlines())
    return set()

# Function to append a processed file to the log file
def log_processed_file(log_file, file_name):
    with open(log_file, "a") as file:
        file.write(f"{file_name}\n")

# Main processing function
def process_parquet_files(folder, log_file):
    processed_files = read_processed_files(log_file)
    parquet_files = list_parquet_files(folder)

    for file_path in parquet_files:
        file_name = os.path.basename(file_path)
        if file_name not in processed_files:
            # Create a new Spark session
            spark = SparkSession.builder.appName(f"Process {file_name}").getOrCreate()

            # Read and process the Parquet file
            df = spark.read.parquet(file_path)
            df.show()  # Replace with actual processing logic

            # Log the processed file
            log_processed_file(log_file, file_name)

            # Stop the Spark session
            spark.stop()

if __name__ == "__main__":
    process_parquet_files(input_folder, processed_files_log)
