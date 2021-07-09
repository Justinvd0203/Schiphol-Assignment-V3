import pyspark
from pyspark.sql import SparkSession
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
    lines = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    # Split the lines into words
    words = lines.select(
        explode(
            split(lines.value, " ")
        ).alias("word")
    )

    # Generate running word count
    wordCounts = words.groupBy("word").count()

    query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()


# def extractdata(spark):
#     df = pd.read_csv('https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat',
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
#
#
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
