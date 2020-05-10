## Introduction

Driving is one of the most common yet dangerous tasks that people
perform every day. According to the NHTSA, there is an average of 6
million accidents in the U.S. per year, resulting in over 2.35 million
injuries and 37,000 deaths. [1] Additionally, road crashes cost the U.S.
$230.6 billion per year. There are various factors that can contribute
to accidents such as distracted driving, speeding, poor weather
conditions, and alcohol involvement. However, using these factors and
additional statistics, we can better predict the cause of accidents and
put laws and procedures in place to help minimize the number of
accidents. For this assignment we will use Apache Spark to analyze the
motor vehicle collisions in New York City (NYC). Our goal is to gain
additional insights into the causes of accidents in the Big Apple and
how we can help to prevent them.

## Datasets

The following datasets were used for our analysis, with the NYC Motor
Vehicle Collisions - Crashes being used for a majority of the analysis.
All of the datasets were obtained from NYC OpenData in CSV format and
contain the most up to date motor vehicle collision information
available to the public.

### NYC Motor Vehicle Collisions - Crashes: [2]

Contains information about the motor vehicle collisions in NYC from July
2012 to February 2020. The data was extracted from police reports (form
MV104-AN) that were filed at the time of the crash. A form MV104-AN is
only filed in the case where an individual is injured or fatally
injured, or when the damage caused by the accident is $1,000 or greater.
This dataset has 29 columns, 1.65 Million rows, and a size of 369 MB.
Each row includes details about a specific motor vehicle collision.

### NYC Motor Vehicle Crashes - Person: [3]

Contains information about each person that was involved in a crash in
NYC from February 2014 to May 2020 and a police report MV104-AN was
filed. This dataset has 3.92M rows, 22 columns, and a size of 641.4 MB.
Each row represents a person such as a driver, occupant, pedestrian, and
bicyclist involved in a crash, which can be tied back to a specific
crash in the NYC Motor Vehicle Collisions - Crashes dataset. Multiple
individuals can be involved in a single crash.

### NYC Motor Vehicle Crashes - Vehicle: [4]

Contains information about each vehicle that was involved in a crash in
NYC from September 2012 to May 2020 and a police report MV104-AN was
filed. This dataset has 3.35M rows, 25 columns, and a size of 566.3 MB.
Each row represents the vehicle information for a specific crash, which
can also be tied back to the NYC Motor Vehicle Collisions - Crashes
dataset. Multiple vehicles can be involved in a single crash.

### 2015 NYC Tree Census: [5]

Contains detailed information on the trees living throughout NYC
collected by the NYC parks and recreation board in 2015. This datset has
684K rows, 45 columns, and has a size of 220.4 MB. Each row represents
the information for a tree living in NYC.

## Loading Datasets

In the beforeAll() function, all four CSV files are read in as as
DataFrames, then compressed to Parquet format and persisted to external
storage. A check is made to determine if the Parquet files exist on disk
before running a test. If the files exist then they will be reused for
all subsequent tests. Through the use of Parquet, our data will be
stored in a compressed columnar format, which will allow for faster read
and write performance as compared to reading from CSV. The file size of
the datasets when converted to parquet are significantly smaller as
compared to the original CSV files. For example, the CSV file for the
NYC Motor Vehicle - Crashes was 369 MB and was reduced to 75.7 MB when
converted to Parquet. Additionally, the output Parquet files for the The
NYC Motor Vehicle - Vehicles dataset were partitioned by ZIP_CODE. By
doing this, the data is physically laid out on the filesystem in an
order that will be the most efficient for performing our queries.

Some data preparation was needed before converting to Parquet which
include the following.
- For all DataFrames, the whitespace between columns names needed to be
  replaced with underscores to avoid the invalid character errors when
  converting to Parquet. E.g. from CRASH TIME to CRASH_TIME.
- The data types of several columns in the NYC Motor Vehicle - Crashes
  dataset needed to casted from a string to an integer type because they
  will be used for numerical calculations in our query.

The below Directed acyclic graph (DAG) show the operations performed
when reading a parquet file. This operation is performed each time a
test is ran. Spark performs a parallelize operation followed by a
mapPartions operation.

![Parquet1](data/Images/Parquet1.png)
![Parquet2](data/Images/Parquet2.png)

## Analytic Questions

### 1. What is the top five most frequent contributing factors for accidents in NYC?

In order to answer this question, we will need to do a group by and
count.

### 2. What percentage of accidents had alcohol as a contributing factor?

### 3. What time of day sees the most cyclist injures or deaths caused by a motor vehicle collision?

In order to answer this question, we will need to add the number of
cyclist injured plus the number of cyclist deaths for each row and store
the result in a new column. Next, we will need to group by crash time
and get the crash time with the most amount of cyclist injured or
deaths.


In order to answer this question, We will need to do a filter and count.

### 4. Which zip code had the largest number of nonfatal and fatal accidents?

### 5. Which vehicle make, model, and year was involved in the most accidents?

One possible solution is to group by vehicle year and make.

### 6. How do the number of collisions in an area of NYC correlate to the number of trees in the area?

In order to answer this question, we will need to join the NYC Motor
Vehicle Collisions and 2015 NYC Tree Census Dataset by zip code.

### 7. What is the average number of people involved in crashes per year in NYC between the years 2012 and 2020?

## Chosen Spark API for Answering Analytical Questions

I plan to use the Spark DataFrames API for my analysis simply. The Spark
DataFrames API is similar to relational tables in SQL, however it also
provides a programmatic API allowing for more flexibility in query
expressiveness, query testing, and is extensible. Additionally, the
datasets that we will be using for our analysis are in a format that can
be easily worked with using DataFrames. For example our data is in a
tabular format with columns and rows, which is how data is represented
in DataFrames. Additionally, DataFrames inherits all of the properties
of RDDs such as read only, fault tolerance, caching, and lazy execution
but with the additional data storage optimizations, code generation,
query planner, and abstraction.

## Project Overview

- Language: [Scala](https://www.scala-lang.org/)
- Framework: [Apache Spark](https://spark.apache.org/)
- Build tool: [SBT](https://www.scala-sbt.org/)
- Testing Framework: [Scalatest](http://www.scalatest.org/)

## Running Tests

### From Intellij

Right click on `ExampleDriverTest` and choose `Run 'ExampleDriverTest'`

### From the command line

On Unix systems, test can be run:

```shell script
$ ./sbt test
```

or on Windows systems:

```shell script
C:\> ./sbt.bat test
```

## Configuring Logging

Spark uses log4j 1.2 for logging. Logging levels can be configured in
the file `src/test/resources/log4j.properties`

Spark logging can be verbose, for example, it will tell you when each
task starts and finishes as well as resource cleanup messages. This
isn't always useful or desired during regular development. To reduce the
verbosity of logs, change the line `log4j.logger.org.apache.spark=INFO`
to `log4j.logger.org.apache.spark=WARN`

## Scala Worksheets

The worksheet `src/test/scala/com/spark/example/playground.sc` is a good
place to try out Scala code. Add your code to the left pane of the
worksheet, click the 'play' button, and the result will display in the
right pane.

Note: The worksheet will not work for Spark code.

## Documentation

* RDD: https://spark.apache.org/docs/latest/rdd-programming-guide.html
* Batch Structured APIs:
  https://spark.apache.org/docs/latest/sql-programming-guide.html

## References

[1] National Highway Traffic Safety Administration. “NCSA Publications
&amp; Data Requests.” Early Estimate of Motor Vehicle Traffic Fatalities
for the First Quarter of 2019, 2019,
https://crashstats.nhtsa.dot.gov/Api/Public/ViewPublication/812783.

[2] (NYPD), Police Department. “Motor Vehicle Collisions - Crashes: NYC
Open Data.” Motor Vehicle Collisions - Crashes | NYC Open Data, 8 May
2020,
data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95.

[3] (NYPD), Police Department. “Motor Vehicle Collisions - Vehicles: NYC
Open Data.” Motor Vehicle Collisions - Vehicles | NYC Open Data, 8 May
2020,
data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Vehicles/bm4k-52h4.

[4] (NYPD), Police Department. “Motor Vehicle Collisions - Person: NYC
Open Data.” Motor Vehicle Collisions - Person | NYC Open Data, 8 May
2020,
data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Person/f55k-p6yu.

[5] Department of Parks and Recreation. “2015 Street Tree Census - Tree
Data: NYC Open Data.” 2015 Street Tree Census - Tree Data | NYC Open
Data, 4 Oct. 2017,
data.cityofnewyork.us/Environment/2015-Street-Tree-Census-Tree-Data/uvpi-gqnh.
