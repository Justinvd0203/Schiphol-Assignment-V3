import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

import pandas as pd
import numpy as np
import os

pd.set_option('display.max_columns', 35)
pd.set_option('display.width', 250000)

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"


def createconnection():
    spark = SparkSession \
        .builder \
        .appName("StreamingJob") \
        .getOrCreate()
    return spark


def createstream(spark):
    routeschema = StructType().add("index", "string").add("Airline", "string").add("Airline ID", "integer").add(
        "Source airport",
        "string").add(
        "Source airport ID", "integer").add("Destination airport", "string").add("Destination airport ID",
                                                                                 "integer").add("Codeshare",
                                                                                                "string").add(
        "Stops", "integer").add("Equipment", "string")
    lines = spark \
        .readStream \
        .option("sep", ",") \
        .schema(routeschema) \
        .csv("./tmp/text")

    data = lines.groupBy("Source airport ID").count().orderBy("count", ascending=False).limit(10)
    print(data)

    query = data \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    # query = data \
    #     .writeStream \
    #     .outputMode("append") \
    #     .format("csv") \
    #     .option("path", "output/") \
    #     .option("checkpointLocation", "checkpoint/") \
    #     .start()

    query.awaitTermination()


# def extractdata(spark, url):
#     df = pd.read_csv(url,
#                      names=["Airline", "Airline ID", "Source airport", "Source airport ID",
#                             "Destination airport", "Destination airport ID", "Codeshare", "Stops", "Equipment"])
#     df = df.replace(np.nan, "", regex=True)
#
#     data = spark.createDataFrame(df)
#
#     # print(df.head(5))
#     # print(data)
#
#     return data


# def analysedata(data):
#     data = data.groupBy("Source airport ID").count().orderBy("count", ascending=False).limit(10)
#     data.toPandas().to_csv("sample.csv", header=True)
#
#     # print(data.show())


if __name__ == '__main__':
    sparkSession = createconnection()
    createstream(sparkSession)

    # sparkDataframe = extractdata(sparkSession)
    # analysedata(sparkDataframe)
