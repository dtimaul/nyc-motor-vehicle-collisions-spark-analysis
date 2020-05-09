## Introduction
Driving is one of the most common yet dangerous tasks that people perform every day. According to the NHTSA, there is an average of 6 million accidents in the U.S. per year, resulting in over 2.35 million injuries and 37,000 deaths.
  Additionally, road crashes cost the U.S. $230.6 billion per year.  
  There are various factors that can contribute to accidents such as distracted driving, speeding, poor weather conditions, and alcohol involvement.  
  However, using these factors and additional statistics, we can better predict the cause of accidents and put laws and procedures in place to help minimize the number of accidents. For this assignment we will use Apache Spark to analyze the motor vehicle collisions in New York City (NYC). Our goal is to gain additional insights into the causes of accidents in the Big Apple and how we can help to prevent them.

## Datasets
The following datasets were used for our analysis, with the NYC Motor Vehicle Collisions - Crashes being used for a majority of the analysis.  
All of the datasets were obtained from NYC OpenData in CSV format and contain the most up to date motor vehicle collision information available to the public.

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

Spark uses log4j 1.2 for logging. Logging levels can be configured in the file `src/test/resources/log4j.properties`

Spark logging can be verbose, for example, it will tell you when each task starts and finishes as well
as resource cleanup messages. This isn't always useful or desired during regular development. To reduce the verbosity of logs,
change the line `log4j.logger.org.apache.spark=INFO` to `log4j.logger.org.apache.spark=WARN`

## Scala Worksheets

The worksheet `src/test/scala/com/spark/example/playground.sc` is a good place to try out Scala code. Add your code
to the left pane of the worksheet, click the 'play' button, and the result will display in the right pane.

Note: The worksheet will not work for Spark code.

## Documentation

* RDD: https://spark.apache.org/docs/latest/rdd-programming-guide.html
* Batch Structured APIs: https://spark.apache.org/docs/latest/sql-programming-guide.html
