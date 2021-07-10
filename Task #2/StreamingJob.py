from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

import os

# Java Configuration
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

# Schema for the structured dataframe (input data structure)
routeschema = StructType().add("index", "string").add("Airline", "string").add("Airline ID", "integer").add(
    "Source airport",
    "string").add(
    "Source airport ID", "integer").add("Destination airport", "string").add("Destination airport ID",
                                                                             "integer").add("Codeshare",
                                                                                            "string").add(
    "Stops", "integer").add("Equipment", "string")


def createconnection():
    """
    Function to connect to the spark instance, if the application does not exist yet it will be created.
    Input: None
    Output: SparkSession
    """
    spark = SparkSession \
        .builder \
        .appName("StreamingJob") \
        .getOrCreate()
    return spark


def createstream(spark):
    """
    Function to create a spark stream
    Input: SparkSession
    Output: Results in console
    """
    # Create input stream listening to .csv files within the directory ./tmp/input
    lines = spark \
        .readStream \
        .option("sep", ",") \
        .schema(routeschema) \
        .csv("./tmp/input")

    # Perform data aggregation to get the top 10 used airports in this dataset
    data = lines.groupBy("Source airport ID").count().orderBy("count", ascending=False).limit(10)

    # Create a write stream writing the data to the console
    query = data \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()


# Start of the code
if __name__ == '__main__':
    # Create spark Session
    sparkSession = createconnection()

    # Create streaming session
    createstream(sparkSession)
