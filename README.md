# Schiphol-Assignment-V3

This README document will function as a way to track my train of thought and provide additional information.

## Task #1
Task 1 can be divided into multiple parts:
-   Setup Spark environment
-   Connect to Spark environment in Python
-   Load dataset into Python
-   Analyse & Return dataset

### Setup Spark environment
The first task is to set up spark with a reproducible outcome therefore there was looking into the use of docker.
For this I used the image of https://hub.docker.com/r/bitnami/spark and produced a docker-compose.yml file

This setup of spark containers 1 master node and 2 workes, this can be scaled up and down if necessary.
To test this setup the user can navigate to http://localhost:8080/ and see the output shown below.

![Image of local environment](/images/Spark-localhost-environment.png)

### Connect to Spark environment in Python
To connect to the spark environment I have chosen to create a SparkSession.
However I did manage to get some struggles connecting to the spark environment, first of all Java 8 had to be installed and an environment variable had to be added.
`os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"` to point towards the java 8 directory.

To test the connection and spark workers I created a simple function which creates a resilient distributed dataset(RDD) with a range of 100.
From there I took 5 samples and printed them giving the following results:

![Image of local environment](/images/connection-test.png)

### Load dataset into Python
To load in the data I used pandas to read the csv file by url. To transform this pandas dataframe into a pyspark dataframe I converted NaN values to empty strings.

![Image of local environment](/images/Extraction-results.png)

### Analyse and return dataset
To analyse the data it had to be grouped and counted by Source Airport ID. After it had to be sorted and limited to 10 values descending values.
Finally, these results are saved in a .csv file using pandas.

![Image of local environment](/images/Analysis%20results.png)

## Task #2


## Task #3


## Task #4
