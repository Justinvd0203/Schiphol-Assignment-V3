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

![Image of connection test](/images/connection-test.png)

### Load dataset into Python
To load in the data I used pandas to read the csv file by url. To transform this pandas dataframe into a pyspark dataframe I converted NaN values to empty strings.

![Image of extraction result](/images/Extraction-results.png)

### Analyse and return dataset
To analyse the data it had to be grouped and counted by Source Airport ID. After it had to be sorted and limited to 10 values descending values.
Finally, these results are saved in a .csv file using pandas.

![Image of local environment](/images/Analysis%20results.png)

## Task #2
The difficult task here is to perform aggregation on a streaming dataset and return the analysis.

To achieve this task 2 is divided into the following parts:
-   Create input stream
-   Perform aggregation
-   Create output stream

### Create input stream
The question here is how do we read the data? For this there are 2 options;
-   Work with input from a certain port or HTTP request
-   Listen to a directory for CSV files

After playing around with both options the better choice seemed to be to listen to a directory. Each time a .csv file is added in this directory,
a new batch will start and process and generate results. For production this also seems like the better option, whilst working on a bigger
server all the files that have to be processed can be placed within this certain directory and process. After successfully processing
it would be a good idea to remove the input files.

For this approach I made a structure corresponding to the expected csv input and added this to the readstream as a schema.
This produced the following dataframe result within the stream:

![Image of readstream input](/images/Streaming%20input.png)

### Perform aggregation
For the aggregation we can re-use the code used in the previous task on the readstream Dataframe.
This produced the following dataframe result:

![Image of data aggregation output](/images/Data_aggregation_%20output.png)

### Create output stream
At last I want to be able to see the results and check if they correspond to the results we found in the previous task.
For this I used a writestream to write the complete output of the data aggregation to the console.

To test the whole task run the code through `python3 ./Streamingjob.py`. It will then look into the tmp/input directory and process each .csv file.
This will generate a result as seen in the following image for each .csv file:

![Image of data streaming output](/images/Streaming%20output%20results.png)

## Task #3

### Create window + interval
The hard part here is to create a window + sliding interval whilst there is no timestamp field.
When a timestamp is available this would be achieveable with `window(col("timestamp"), "1 hour", "15 minutes)"`.

Therefore I went with the option to manually split it into multiple windows with a certain interval using:

`data = lines.where(col('index').between(0, 10))`

### Analyse and return batch data
Currently this caused the issue where multiple aggregations are not supported with streaming DataFrames. 
To solve this I used `foreachBatch()` within the writestream where I could perform multiple aggregations and save the data to a directory.
Each batch of data would give another csv file in the output directory, for example adding a .csv file in the input directory
results into batch1.csv file in the output. It is also possible ofcourse to save every window analysis to a unique file, however I did not go for that option.

I tested this concept and it gave the following results:

![Image of window + interval testing](/images/Window_test.png)

This show that my approach could work, therefore I continued with this method. I do still believe there could be a better option
however without any experience with Spark this is what I came up with in the time that I had which gave the expected result.

To be able to configure it I also added 2 field values where the user can change the window and slider size in percentages of the file size.
One thing to notice is that the slider size should be able to fit in whole integers within the window size.

## Task #4
