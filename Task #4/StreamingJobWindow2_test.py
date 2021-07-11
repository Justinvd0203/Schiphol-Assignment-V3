import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import StreamingJobWindow2


def test_analyse_data():
    """
    Function to test the analyse data function
    Input: None
    Output: None
    """
    # Create Mock Data structure
    routeschema = StructType().add("index", "integer").add("Airline", "string").add("Airline ID", "integer").add(
        "Source airport",
        "string").add(
        "Source airport ID", "integer").add("Destination airport", "string").add("Destination airport ID",
                                                                                 "integer").add("Codeshare",
                                                                                                "string").add(
        "Stops", "integer").add("Equipment", "string")

    # Create Mock Data
    data = [(0, "A", 123, "AA", 1, "AAA", 123, "", 0, "CR2"), (1, "A", 123, "AA", 2, "AAA", 123, "", 0, "CR2"),
            (2, "A", 123, "AA", 1, "AAA", 123, "", 0, "CR2"), (3, "A", 123, "AA", 3, "AAA", 123, "", 0, "CR2"),
            (4, "A", 123, "AA", 1, "AAA", 123, "", 0, "CR2"), (5, "A", 123, "AA", 2, "AAA", 123, "", 0, "CR2"),
            (6, "A", 123, "AA", 3, "AAA", 123, "", 0, "CR2"), (7, "A", 123, "AA", 1, "AAA", 123, "", 0, "CR2"),
            (8, "A", 123, "AA", 1, "AAA", 123, "", 0, "CR2"), (9, "A", 123, "AA", 3, "AAA", 123, "", 0, "CR2")]

    # Create Sparksession to test in
    spark = SparkSession.builder.appName('TestSession').getOrCreate()

    # Create Spark dataframe with schema and data
    df = spark.createDataFrame(data=data, schema=routeschema)

    # Fetch row count
    rows = df.count()

    # Execute function to test
    output = StreamingJobWindow2.analyse_data(df, rows, window_percentage=20, sliding_percentage=10)

    # Create schema for expected results
    expected_schema = StructType().add("Source airport ID", "integer").add("windowNumber", "integer").add("window",
                                                                                                          "string").add(
        "count", "integer")

    # Create expected results data
    expected_data = [(1, 0, "[0-2]", 2), (2, 0, "[0-2]", 1), (3, 1, "[1-3]", 1), (1, 1, "[1-3]", 1), (2, 1, "[1-3]", 1),
                     (1, 2, "[2-4]", 2), (3, 2, "[2-4]", 1), (1, 3, "[3-5]", 1), (2, 3, "[3-5]", 1), (3, 3, "[3-5]", 1),
                     (2, 4, "[4-6]", 1), (3, 4, "[4-6]", 1), (1, 4, "[4-6]", 1), (3, 5, "[5-7]", 1), (2, 5, "[5-7]", 1),
                     (1, 5, "[5-7]", 1), (1, 6, "[6-8]", 2), (3, 6, "[6-8]", 1), (1, 7, "[7-9]", 2), (3, 7, "[7-9]", 1)]

    # Create the expected results dataframe with the schema and data
    expected_df = spark.createDataFrame(data=expected_data, schema=expected_schema)

    # Test if expected output is equal to the output of the tested function
    assert sorted(output.collect()) == sorted(expected_df.collect())
