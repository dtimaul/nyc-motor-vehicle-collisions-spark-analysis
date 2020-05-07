package com.spark.assignment2

import java.nio.file.{Files, Paths}

import org.apache.spark.sql._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.collection
import scala.concurrent.duration._

class Assignment2Test extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  /**
   * Set this value to 'true' to halt after execution so you can view the Spark UI at localhost:4040.
   * NOTE: If you use this, you must terminate your test manually.
   * OTHER NOTE: You should only use this if you run a test individually.
   */
  val BLOCK_ON_COMPLETION = false;

  // Paths to our data.
  val NYC_MV_COLLISIONS_CRASHES_PATH = "data/NYC_Motor_Vehicle_Collisions_Crashes.csv"
  val NYC_MV_COLLISIONS_PERSONS_PATH = "data/NYC_Motor_Vehicle_Collisions_Person.csv"
  val NYC_MV_COLLISIONS_VEHICLES_PATH = "data/NYC_Motor_Vehicle_Collisions_Vehicles.csv"
  val NYC_TREE_CENSUS_PATH = "data/NYC_2015_Street_Tree_Census_Tree_Data.csv"

  // Parquet paths
  val NYC_MV_COLLISIONS_CRASHES_PARQUET_PATH = "data/NYC_Motor_Vehicle_Collisions_Crashes.parquet"
  val NYC_MV_COLLISIONS_PERSON_PARQUET_PATH = "data/NYC_Motor_Vehicle_Collisions_Person.parquet"
  val NYC_MV_COLLISIONS_VEHICLES_PARQUET_PATH = "data/NYC_Motor_Vehicle_Collisions_Vehicles.parquet"
  val NYC_TREE_CENSUS_PARQUET_PATH = "data/NYC_2015_Street_Tree_Census_Tree_Data.parquet"

  /**
   * Create a SparkSession that runs locally on our laptop.
   */
  val spark =
    SparkSession
      .builder()
      .appName("Assignment 2")
      .master("local[*]") // Spark runs in 'local' mode using all cores
      .config("spark.executor.instances", "3")
      .config("spark.sql.parquet.filterPushdown", true)
      .getOrCreate()

  /**
   * Let Spark infer the data types. Tell Spark this CSV has a header line.
   */
  val csvReadOptions: Map[String, String] =
    Map("inferSchema" -> true.toString, "header" -> true.toString)

  // Create parquet datasets that are used in later tests
  // TODO: Add option to parse date timestamps
  override def beforeAll() {
    if (!Files.exists(Paths.get(NYC_MV_COLLISIONS_CRASHES_PARQUET_PATH))) {
      def nycMvCrashesDF: DataFrame = spark.read.options(csvReadOptions).csv(NYC_MV_COLLISIONS_CRASHES_PATH)
      // Replace whitespace between column names with underscore to avoid invalid character errors. E.g. from CRASH TIME to CRASH_TIME.
      var nycMvCrashesDFColumnsRenamed = nycMvCrashesDF
      for(col <- nycMvCrashesDF.columns){
        nycMvCrashesDFColumnsRenamed = nycMvCrashesDFColumnsRenamed.withColumnRenamed(col,col.replaceAll("\\s", "_"))
      }

      nycMvCrashesDFColumnsRenamed.write
        .mode(SaveMode.Ignore)
        .option("compression", "none")
        .parquet(NYC_MV_COLLISIONS_CRASHES_PARQUET_PATH)
    }
     if (!Files.exists(Paths.get(NYC_MV_COLLISIONS_PERSON_PARQUET_PATH))) {
       def nycMvPersonsDF: DataFrame = spark.read.options(csvReadOptions).csv(NYC_MV_COLLISIONS_PERSONS_PATH)
       var nycMvPersonsDFColumnsRenamed = nycMvPersonsDF
       for(col <- nycMvPersonsDF.columns){
         nycMvPersonsDFColumnsRenamed = nycMvPersonsDFColumnsRenamed.withColumnRenamed(col,col.replaceAll("\\s", "_"))
       }

       nycMvPersonsDFColumnsRenamed.write
         .mode(SaveMode.Ignore)
         .option("compression", "none")
         .parquet(NYC_MV_COLLISIONS_PERSON_PARQUET_PATH)
     }
     if (!Files.exists(Paths.get(NYC_MV_COLLISIONS_VEHICLES_PARQUET_PATH))) {
       def nycMvVehiclesDF: DataFrame = spark.read.options(csvReadOptions).csv(NYC_MV_COLLISIONS_VEHICLES_PATH)
       var nycMvVehiclesDFColumnsRenamed = nycMvVehiclesDF
       for(col <- nycMvVehiclesDF.columns){
         nycMvVehiclesDFColumnsRenamed = nycMvVehiclesDFColumnsRenamed.withColumnRenamed(col,col.replaceAll("\\s", "_"))
       }

       nycMvVehiclesDFColumnsRenamed.write
         .mode(SaveMode.Ignore)
         .option("compression", "none")
         .parquet(NYC_MV_COLLISIONS_VEHICLES_PARQUET_PATH)
     }
     if (!Files.exists(Paths.get(NYC_TREE_CENSUS_PARQUET_PATH))) {
       def nycTreeCensusDF:DataFrame = spark.read.options(csvReadOptions).csv(NYC_TREE_CENSUS_PATH)
       var nycTreeCensusDFColumnsRenamed = nycTreeCensusDF
       for(col <- nycTreeCensusDF.columns){
         nycTreeCensusDFColumnsRenamed = nycTreeCensusDFColumnsRenamed.withColumnRenamed(col,col.replaceAll("\\s", "_"))
       }

       nycTreeCensusDFColumnsRenamed.write
         .mode(SaveMode.Ignore)
         .option("compression", "none")
         .parquet(NYC_TREE_CENSUS_PARQUET_PATH)
     }
  }

  private def loadCrashes: DataFrame = {
    spark.read.parquet(NYC_MV_COLLISIONS_CRASHES_PARQUET_PATH).cache()
  }

  private def loadPersons: DataFrame = {
    spark.read.parquet(NYC_MV_COLLISIONS_PERSON_PARQUET_PATH).cache()
  }

  private def loadVehicles: DataFrame = {
    spark.read.parquet(NYC_MV_COLLISIONS_VEHICLES_PARQUET_PATH).cache()
  }

  private def loadTreeCensus: DataFrame = {
    spark.read.parquet(NYC_TREE_CENSUS_PARQUET_PATH).cache()
  }

  /**
   * Keep the Spark Context running so the Spark UI can be viewed after the test has completed.
   * This is enabled by setting `BLOCK_ON_COMPLETION = true` above.
   */
  override def afterEach: Unit = {
    if (BLOCK_ON_COMPLETION) {
      // open SparkUI at http://localhost:4040
      Thread.sleep(5.minutes.toMillis)
    }
  }

  /**
   * What time of day sees the most cyclist injures or deaths caused by a motor vehicle collision?
   */
  test("What time of day sees the most cyclist injures or deaths caused by a motor vehicle collision?") {
      Assignment2.problem1(loadCrashes)
  }

  /**
     * What percentage of accidents had alcohol as a contributing factor?
   */
  test("What percentage of accidents had alcohol as a contributing factor?") {
//    Assignment1.problem2(tripDataRdd) must equal(1069)
    Assignment2.problem2(loadCrashes)
  }

  /**
   * What is the top five most frequent contributing factors for accidents in NYC?
   */
  test("Top five most frequent contributing factors for accidents in NYC") {
    val expectedData = Array(
      Row("Following Too Closely", 84660),
      Row("Traffic Control Disregarded", 25342),
      Row("Driverless/Runaway Vehicle" ,849),
      Row("Accelerator Defective", 811),
      Row("Windshield Inadequate", 69)
    )

    Assignment2.problem3(loadCrashes) must equal(expectedData)
  }
//
//  /**
//   * Which specific location sees the most accidents in NYC?
//   */
//  test("Which specific location sees the most accidents in NYC?") {
//    Assignment1.problem4(tripDataRdd) must equal("94107")
//  }
//
//  /**
//   * For each individual, get the individual age and make of their car.
//   */
//  test("For each individual, get the individual age and make of their car.") {
//    Assignment1.problem5(tripDataRdd) must equal(920)
//  }
//
//  /**
//   * Which vehicle year and make had the most crashes?
//   */
//  test("Which vehicle year and make had the most crashes?") {
//    Assignment1.problem6(tripDataRdd) must equal(354152)
//  }
}
