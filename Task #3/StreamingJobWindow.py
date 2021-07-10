import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import window, col, lit, row_number
import math

from pyspark.sql import Window
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


def foreach_batch_function(df, epoch_id):
    windows = []
    for x in range(0, 68000, 17000):
        print(x)
        data = df.where(col('index').between(x, x + 17000))
        data = data.withColumn("windowNumber", lit(x))
        data = data.withColumn("window", lit("[" + str(x) + "-" + str(x + 17000) + "]"))
        data = data.groupBy("Source airport ID", "windowNumber", "window").count().orderBy(
            col("windowNumber").asc(),
            col("count").desc()).limit(10)
        windows.append(data)

    for i in windows[1:]:
        windows[0] = windows[0].union(i)

    windows[0].show(100)

    pass

def createstream(spark):
    routeschema = StructType().add("index", "integer").add("Airline", "string").add("Airline ID", "integer").add(
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
        .csv("./tmp/input")

    # for x in range(0, 68000, 17000):
    #     print(x)
    #     data = lines.where(col('index').between(x, x + 17000))
    #     data = data.withColumn("windowNumber", lit(x))
    #     data = data.withColumn("window", lit("[" + str(x) + "-" + str(x + 17000) + "]"))
    #     data = data.groupBy("Source airport ID", "windowNumber", "window").count().orderBy(
    #         col("windowNumber").asc(),
    #         col("count").desc()).limit(10)
    #
    #     query = data \
    #         .writeStream \
    #         .outputMode("complete") \
    #         .format("console") \
    #         .start()
    #
    # # I do know that they do not close/stop yet, I do not have an idea yet on how to fix that.
    # query.awaitTermination()

    lines.writeStream \
        .outputMode("append") \
        .foreachBatch(foreach_batch_function) \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    sparkSession = createconnection()
    createstream(sparkSession)
