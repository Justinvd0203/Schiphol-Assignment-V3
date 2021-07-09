import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import pandas as pd
import numpy as np

pd.set_option('display.max_columns', 35)
pd.set_option('display.width', 250000)
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"


def createconnection():
    spark = SparkSession \
        .builder \
        .appName("SimpleBatch") \
        .getOrCreate()
    return spark


def testconnection(spark):
    # do something to prove it works
    rdd = spark.sparkContext.parallelize(range(100))
    rdd = rdd.takeSample(False, 5)
    print(rdd)


def extractdata(spark):
    df = pd.read_csv('https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat',
                     names=["Airline", "Airline ID", "Source airport", "Source airport ID",
                            "Destination airport", "Destination airport ID", "Codeshare", "Stops", "Equipment"])
    df = df.replace(np.nan, "", regex=True)

    data = spark.createDataFrame(df)

    # print(df.head(5))
    # print(data)

    return data


def analysedata(data):
    data = data.groupBy("Source airport ID").count().orderBy("count", ascending=False).limit(10)
    data.toPandas().to_csv("sample.csv", header=True)

    # print(data.show())


if __name__ == '__main__':
    sparkSession = createconnection()
    testconnection(sparkSession)
    sparkDataframe = extractdata(sparkSession)
    analysedata(sparkDataframe)
