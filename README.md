# Schiphol-Assignment-V3

This README document will function as a way to track my train of thought and provide additional information.

## Task #1

Task 1 can be divided into multiple parts:

- Setup Spark environment
- Connect to Spark environment in Python
- Load dataset into Python
- Analyse & Return dataset

### Setup Spark environment

The first task is to set up spark with a reproducible outcome therefore there was looking into the use of docker. For
this I used the image of https://hub.docker.com/r/bitnami/spark and produced a docker-compose.yml file

This setup of spark containers 1 master node and 2 workes, this can be scaled up and down if necessary. To test this
setup the user can navigate to http://localhost:8080/ and see the output shown below.

![Image of local environment](/images/Spark-localhost-environment.png)

### Connect to Spark environment in Python

To connect to the spark environment I have chosen to create a SparkSession. However I did manage to get some struggles
connecting to the spark environment, first of all Java 8 had to be installed and an environment variable had to be
added.
`os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"` to point towards the java 8 directory.

To test the connection and spark workers I created a simple function which creates a resilient distributed dataset(RDD)
with a range of 100. From there I took 5 samples and printed them giving the following results:

![Image of connection test](/images/connection-test.png)

### Load dataset into Python

To load in the data I used pandas to read the csv file by url. To transform this pandas dataframe into a pyspark
dataframe I converted NaN values to empty strings.

![Image of extraction result](/images/Extraction-results.png)

### Analyse and return dataset

To analyse the data it had to be grouped and counted by Source Airport ID. After it had to be sorted and limited to 10
descending values. Finally, these results are saved in a .csv file using pandas:

![Image of local environment](/images/Analysis%20results.png)

## Task #2

The difficult task here is to perform aggregation on a streaming dataset and return the analysis real-time.

To achieve this task 2 is divided into the following parts:

- Create input stream
- Perform aggregation
- Create output stream

### Create input stream

The question here is how do we read the data? For this I came up with these 2 options;

- Work with input from a certain port / HTTP request
- Listen to a directory for CSV files

After playing around with both options the better choice seemed to be to listen to a directory. Each time a .csv file is
added in this directory, a new batch will start and process/generate results. For production this also seems like the
better option, whilst working on a bigger server all the files that have to be processed can be placed within this
certain directory. After successfully processing it would be a good idea to remove the input files.

For this approach I made a structure/schema corresponding to the expected csv input and added this to the readstream as
a schema. This produced the following dataframe result from the readStream:

![Image of readstream input](/images/Streaming%20input.png)

### Perform aggregation

For the aggregation we can re-use the code used in the previous task on the readstream Dataframe. This produced the
following dataframe result:

![Image of data aggregation output](/images/Data_aggregation_%20output.png)

### Create output stream

At last I want to be able to see the results and check if they correspond to the results we found in the previous task.
For this I used a writestream to write the complete output of the data aggregation to the console.

To test the whole application run the code through `python3 ./Streamingjob.py`. It will then look into the tmp/input
directory and process each .csv file. This will generate a result as seen in the following image for each .csv file in
the directory:

![Image of data streaming output](/images/Streaming%20output%20results.png)

**Adding another file in the input directory will make it run a new batch and process new results**

## Task #3

### Create window + interval

The hard part here is to create a window + sliding interval whilst there is no timestamp field. When a timestamp is
available this would have been achievable with `window(col("timestamp"), "1 hour", "15 minutes)"`.

However we do not have a timestamp, therefore I went with the option to manually split it into multiple windows. I've
chosen to do this based on the index and the following code.

`data = lines.where(col('index').between(0, 10))` (Gets the first 10 rows of data)

### Analyse and return batch data

The ideal option is to split the dataframe into multiple windows, perform the aggregation and then union/join back
together within the Stream. However this caused the issue where multiple aggregations are not supported with streaming
DataFrames. To work around this I used `foreachBatch()` within the writestream where I could perform multiple
aggregations and save the data to a directory (This might not be the ideal option, but it worked).

The results from this is that each batch of data produces a csv file in the output directory, for example adding a
second .csv file in the input directory results into a "batch1.csv" file in the output directory next to the already
existing batch0.csv

It is also possible ofcourse to save every window analysis to a unique file, however I did not go for that option due to
me overloading the output directory.

I tested my method multiple times and it gave the following results:

![Image of window + interval testing](/images/Window_test.png)

This show that my approach could work, therefore I continued with this method. I do still believe there could be a
better option however without any experience with Spark this is what I came up with in the time that I had which gave
the expected result.

To be able to configure the window size and sliding interval I also added 2 field values where the user can change the
window and slider size as a percentage of the total dataframe length (amount of rows). One thing to mention here is that
the slider size should be able to fit in whole integers within the window size for it to work properly.

Eventually I ended up with the following .csv (./Task #4/output/batch0.csv) being generated with a 20% window size and
10% sliding interval. (To test use `python3 StreamingJobWindow2.py` whilst in the correct directory)

![Image of window + interval result](/images/WindowStreamResults.png)

## Task #4

Unfortunately I do not have a lot of experience writing Unit tests so far. Luckily I already divided the code into
multiple functions. Therefore, I could test the major function(s) with pytest, in this case analyse_data(). This
function splits the dataframe into windows per interval and performs data aggregation for each window.

### Create Mock data set

First a Mock dataframe had to be created, this is the data that will be running through the functions. I chose to create
a simple dataset with the same route structure(schema) containing 10 hand-picked records.

### Create Expected Results

I did the same for the expected results where I created a dataframe with the expected results. These expected results
were created by manually/visually analysing the mock data set.

### Run test

At last the unit test could be executed using `pytest StreamingJobWindow2_test.py -s -v` whilst in the correct
directory.

![Image of Unit test result](/images/Unit_test_results.png)

Ultimately I would have liked to increase the size of the Mock data set and the expected results by generating them.
However, this would require more time and perhaps experience.

## Course certificates

Because I did not have any experience with spark or pyspark I also took some courses during/after the assignment. These
courses came from datacamp and resulted into a couple certificates which can be seen in the directory ./Certificates
