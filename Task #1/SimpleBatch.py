from pyspark.sql import SparkSession

import pandas as pd
import numpy as np

pd.set_option('display.max_columns', 35)
pd.set_option('display.width', 250000)
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"


def createconnection():
    """
    Function to connect to the spark instance, if the application does not exist yet it will be created.
    Input: None
    Output: SparkSession
    """
    spark = SparkSession \
        .builder \
        .appName("SimpleBatch") \
        .getOrCreate()
    return spark


def testconnection(spark):
    """
    Function to test the spark connection
    Input: SparkSession
    Output: None
    """
    # Create fake data
    rdd = spark.sparkContext.parallelize(range(100))
    rdd = rdd.takeSample(False, 5)

    # Print fake data
    print(rdd)


def extractdata(spark):
    """
    Function to extract the source data through URL
    Input: SparkSession
    Output: pyspark.sql.dataframe.DataFrame
    """

    # Read csv from url with pandas
    df = pd.read_csv('https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat',
                     names=["Airline", "Airline ID", "Source airport", "Source airport ID",
                            "Destination airport", "Destination airport ID", "Codeshare", "Stops", "Equipment"])
    # Replace NaN values with empty strings
    df = df.replace(np.nan, "", regex=True)

    # Create spark dataframe
    data = spark.createDataFrame(df)

    # # Optional testing
    # print(df.head(5))
    # print(data)

    return data


def analysedata(data):
    """
    Function to analyse the dataframe data and retrieve top 10 used source airports
    Input: pyspark.sql.dataframe.DataFrame
    Output: sample.csv file with results
    """
    # Group by source airport ID and sort by count per value with a limit of 10
    data = data.groupBy("Source airport ID").count().orderBy("count", ascending=False).limit(10)

    # Store dataframe to csv file
    data.toPandas().to_csv("sample.csv", header=True)


# Start of the code
if __name__ == '__main__':
    # Connect to spark
    sparkSession = createconnection()

    # Test spark connection
    testconnection(sparkSession)

    # Extract source data
    sparkDataframe = extractdata(sparkSession)

    # Analyse spark dataframe
    analysedata(sparkDataframe)
