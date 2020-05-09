## Introduction
Traffic accidents are a fact of life as they are constantly occuring every day, hour, and minute, leading to tremendous loss of life and causing injuries to millions of people and destruction of public and private property. According to US car accidents statistics, there were almost 6 million accidents in the country in 2018 which led to the death of nearly 37,000 people and injuries to another 3 million people. In terms of loss to private and personal property, the cost to society is around $300 billion per year. For this assignment, we will use Apache Spark to analyze the motor vehicle collisions in New York City (NYC). Our goal is to gain additional insights into the causes of accidents in the Big Apple and how we can help to prevent them. See SENG_5709_Assignment_2.pdf for more information on the analytical questions that were answered and how Spark was used to calculate them.

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
