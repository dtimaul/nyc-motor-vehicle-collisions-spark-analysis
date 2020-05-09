## Introduction
Traffic accidents are a fact of life as they are constantly occuring every day, hour, and minute, leading to tremendous loss of life and causing injuries to millions of people and destruction of public and private property. According to US car accidents statistics, there were almost 6 million accidents in the country in 2018 which led to the death of nearly 37,000 people and injuries to another 3 million people. In terms of loss to private and personal property, the cost to society is around $300 billion per year. For this assignment we will use Apache Spark to analyze the motor vehicle collisions in New York City (NYC). Our goal is to gain additional insights into the causes of accidents in the Big Apple and how we can help to prevent them.

## Datasets
- **NYC Motor Vehicle Collisions - Crashes**: Contains details about the motor vehicle collisions in
NYC from July 2012 to February 2020. The data was extracted from police reports (form MV104-AN)
that were filed at the time of the crash. A form MV104-AN is only filed in the case where an individual
is injured or killed, or when the damage caused by the accident is $1,000 or greater. This dataset has
29 columns, and 1.65 Million rows. Each row includes details about a specific motor vehicle collision.
- **NYC Motor Vehicle Crashes - Individual Information**: Contains information about each person
that was involved in a crash in NYC from February 2014 to May 2020 and a police report MV104-AN
was filed. This dataset has 3.92M rows and 22 columns. Each row represents a person such as a driver,
occupant, pedestrian, and bicyclist involved in a crash, which can be tied back to a specific crash in the
NYC Motor Vehicle Collisions - Crashes dataset. Multiple individuals can be involved in a single crash.
- **NYC Motor Vehicle Crashes - Vehicle Information**: Contains information about each vehicle
that was involved in a crash in NYC from September 2012 to May 2020 and a police report MV104-AN
was filed. This dataset has 3.35M rows and 25 columns. Each row represents the vehicle information
for a specific crash, which can also be tied back to the NYC Motor Vehicle Collisions - Crashes dataset.
Multiple vehicles can be involved in a single crash.
- **2015 NYC Tree Census** Contains detailed information on the trees living throughout NYC collected
by the NYC parks and recreation board.

## Analytic Questions
### NYC Motor Vehicle Collisions and 2015 NYC Tree Census Dataset Questions
- What time of day sees the most cyclist injures or deaths caused by a motor vehicle collision? In order
to answer this question, we will need to add the number of cyclist injured plus the number of cyclist
deaths for each row and store the result in a new column. Next, we will need to group by crash time
and get the crash time with the most amount of cyclist injured or deaths.
- What percentage of accidents had alcohol as a contributing factor? In order to answer this question,
We will need to do a filter and count.
- What is the most frequent contributing factor for accidents in NYC? In order to answer this question,
we will need to do a group by and count.
- Which specific location sees the most accidents in NYC? In order to answer this question, we will most
likely need to do a group by location.
- How do the number of collisions in an area of New York City correlate to the number of trees in the
area? In order to answer this question, we will need to join the NYC Motor Vehicle Collisions and 2015
NYC Tree Census Dataset by zip code.


## Project Overview

- Language: [Scala](https://www.scala-lang.org/)
- Framework: [Apache Spark](https://spark.apache.org/)
- Build tool: [SBT](https://www.scala-sbt.org/) 
- Testing Framework: [Scalatest](http://www.scalatest.org/)

## Code Overview

### Driver

`ExampleDriver` is a Spark `Driver` (or coordinator) that will run a Spark application

It defines: 
- a 'main' class that allows the Spark appliction
to be run using `spark-submit` 
- a function `readData` to load data from a datasource
- a function `process` to apply transformations to the data

Functions `readData' and `process` take as an argument a `Spark` object. This Spark object
will be different if the `ExampleDriver` is run on a real cluster or in the unit tests in the project.

### Test

`ExampleDriverTest` is a test for the Spark driver. It contains two tests,
one to assert we can read data and the other that we can apply a transformation
to the data.

## IDE Setup

- Download [Intellij IDEA Community Edition](https://www.jetbrains.com/idea/download/#section=mac)
- Install the `Scala` plugin in intellij ([plugin install instructions](https://www.jetbrains.com/help/idea/managing-plugins.html))
- From Intellij, open the `build.sbt` file in this directory with 'File' -> 'Open'. Opening the `build.sbt` file will ensure Intellij loads the project correctly
- When prompted, choose 'Open as Project'

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
