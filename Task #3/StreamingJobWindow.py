import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import window, col, lit

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

    data = lines.where(col('index').between(0, 10))
    data = data.withColumn("windowNumber", lit(1))
    data = data.withColumn("window", lit("[0-10]"))


    data2 = lines.where(col('index').between(5, 15))
    data2 = data2.withColumn("windowNumber", lit(2))
    data2 = data2.withColumn("window", lit("[5-15]"))


    output = data.union(data2)
    output = output.groupBy("Source airport ID", "windowNumber", "window").count().orderBy(col("windowNumber").asc(),
                                                                           col("count").desc()).limit(10)

    query = output \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    sparkSession = createconnection()
    createstream(sparkSession)
