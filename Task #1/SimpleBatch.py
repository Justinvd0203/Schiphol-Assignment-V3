from pyspark.sql import SparkSession
import pandas as pd
import os
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
# os.environ["SPARK_HOME"] = "/content/spark-2.4.3-bin-hadoop2.7"

def createconnection():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Simple_Batch") \
        .getOrCreate()
    return spark


def extractdata():
    data = pd.read_csv('https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat')
    print(data)


if __name__ == '__main__':
    sparkBuilder = createconnection()
    # extractdata()
