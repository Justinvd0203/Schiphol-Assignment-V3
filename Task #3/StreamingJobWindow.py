import math
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit

# Java Configuration
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

# Schema for the structured dataframe (input data structure)
routeschema = StructType().add("index", "integer").add("Airline", "string").add("Airline ID", "integer").add(
    "Source airport",
    "string").add(
    "Source airport ID", "integer").add("Destination airport", "string").add("Destination airport ID",
                                                                             "integer").add("Codeshare",
                                                                                            "string").add(
    "Stops", "integer").add("Equipment", "string")

# The percentage of the window compared to the full length of the dataset
window_percentage = 25


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


def analyse_data(df, rows):
    """
    Function to analyse the Streaming Dataframe data in set windows
    Input:  - pyspark.sql.dataframe.DataFrame
            - Dataframe row count
    Output: pyspark.sql.dataframe.DataFrame
    """
    # Create List to store window dataframes
    windowResults = []

    # Create window size
    window_size = math.ceil(window_percentage / 100 * rows)

    # Loop to create windows over the Streaming Dataframe
    for x in range(0, rows, window_size):
        # Extract dataframe rows within the window
        data = df.where(col('index').between(x, x + window_size))

        # Add a window number so it can be used for sorting
        data = data.withColumn("windowNumber", lit(x))

        # Add window buckets for a better description in the results dataset
        data = data.withColumn("window", lit("[" + str(x) + "-" + str(x + window_size) + "]"))

        # Perform data aggregation to get the top 10 used airports within this window
        data = data.groupBy("Source airport ID", "windowNumber", "window").count().orderBy(
            col("windowNumber").asc(),
            col("count").desc()).limit(10)

        # Append window result data to the list
        windowResults.append(data)

    # Append all results following the first into 1 big dataframe
    for i in windowResults[1:]:
        windowResults[0] = windowResults[0].union(i)

    # Return the big dataframe :)
    return windowResults[0]


def batch_process(df, epoch_id):
    """
    Function to process each batch
    Input:  - pyspark.sql.dataframe.DataFrame
            - epoch_id, this is the batch number if i'm correct
    Output: batch<batch_number>.csv file within the output directory
    """
    # Get general Df info (Can be used for debugging/logging/testing)
    rows = df.count()

    # Call function to analyse the data
    results = analyse_data(df, rows)

    # Save the results to a csv within ./output/batch directory with corresponding batch number
    results.toPandas().to_csv("./output/batch" + str(epoch_id) + ".csv", header=True)
    pass


def createstream(spark):
    """
    Function to create input readStream and output writeStream
    Input: Spark Session
    Output: None
    """
    # Create input stream listening to .csv files within the directory ./tmp/input
    lines = spark \
        .readStream \
        .option("sep", ",") \
        .schema(routeschema) \
        .csv("./tmp/input")

    # Create a write stream executing the foreach batch function batch_process()
    lines.writeStream \
        .outputMode("append") \
        .foreachBatch(batch_process) \
        .start() \
        .awaitTermination()


# Start of the code
if __name__ == '__main__':
    # Connect to the spark docker instance
    sparkSession = createconnection()

    # Create a spark structured stream
    createstream(sparkSession)
