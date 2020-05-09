package com.spark.assignment2

import java.nio.file.{Files, Paths}

import org.apache.spark.sql._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
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

  // Write DataFrames to external storage in Parquet format
  override def beforeAll() {
    if (!Files.exists(Paths.get(NYC_MV_COLLISIONS_CRASHES_PARQUET_PATH))) {
      def nycMvCrashesDF: DataFrame = spark.read.options(csvReadOptions).csv(NYC_MV_COLLISIONS_CRASHES_PATH)
      // Replace whitespace between column names with underscore to avoid invalid character errors. E.g. from CRASH TIME to CRASH_TIME.
      var nycMvCrashesDFColumnsRenamed = nycMvCrashesDF
      for (col <- nycMvCrashesDF.columns) {
        nycMvCrashesDFColumnsRenamed = nycMvCrashesDFColumnsRenamed.withColumnRenamed(col, col.replaceAll("\\s", "_"))
      }

      nycMvCrashesDFColumnsRenamed.write
        .mode(SaveMode.Ignore)
        .option("compression", "none")
        .parquet(NYC_MV_COLLISIONS_CRASHES_PARQUET_PATH)
    }
    if (!Files.exists(Paths.get(NYC_MV_COLLISIONS_PERSON_PARQUET_PATH))) {
      def nycMvPersonsDF: DataFrame = spark.read.options(csvReadOptions).csv(NYC_MV_COLLISIONS_PERSONS_PATH)
      var nycMvPersonsDFColumnsRenamed = nycMvPersonsDF
      for (col <- nycMvPersonsDF.columns) {
        nycMvPersonsDFColumnsRenamed = nycMvPersonsDFColumnsRenamed.withColumnRenamed(col, col.replaceAll("\\s", "_"))
      }

      nycMvPersonsDFColumnsRenamed.write
        .mode(SaveMode.Ignore)
        .option("compression", "none")
        .parquet(NYC_MV_COLLISIONS_PERSON_PARQUET_PATH)
    }
    if (!Files.exists(Paths.get(NYC_MV_COLLISIONS_VEHICLES_PARQUET_PATH))) {
      def nycMvVehiclesDF: DataFrame = spark.read.options(csvReadOptions).csv(NYC_MV_COLLISIONS_VEHICLES_PATH)
      var nycMvVehiclesDFColumnsRenamed = nycMvVehiclesDF
      for (col <- nycMvVehiclesDF.columns) {
        nycMvVehiclesDFColumnsRenamed = nycMvVehiclesDFColumnsRenamed.withColumnRenamed(col, col.replaceAll("\\s", "_"))
      }

      nycMvVehiclesDFColumnsRenamed.write
        .mode(SaveMode.Ignore)
        .option("compression", "none")
        .parquet(NYC_MV_COLLISIONS_VEHICLES_PARQUET_PATH)
    }
    if (!Files.exists(Paths.get(NYC_TREE_CENSUS_PARQUET_PATH))) {
      def nycTreeCensusDF: DataFrame = spark.read.options(csvReadOptions).csv(NYC_TREE_CENSUS_PATH)
      var nycTreeCensusDFColumnsRenamed = nycTreeCensusDF
      for (col <- nycTreeCensusDF.columns) {
        nycTreeCensusDFColumnsRenamed = nycTreeCensusDFColumnsRenamed.withColumnRenamed(col, col.replaceAll("\\s", "_"))
      }

      nycTreeCensusDFColumnsRenamed.write
        .mode(SaveMode.Ignore)
        .option("compression", "none")
        .parquet(NYC_TREE_CENSUS_PARQUET_PATH)
    }
  }

  private def nycMvCrashesDFParquet: DataFrame = {
    spark.read.parquet(NYC_MV_COLLISIONS_CRASHES_PARQUET_PATH).cache()
  }

  private def nycMvPersonsDFParquet: DataFrame = {
    spark.read.parquet(NYC_MV_COLLISIONS_PERSON_PARQUET_PATH).cache()
  }

  private def nycMvVehiclesDFParquet: DataFrame = {
    spark.read.parquet(NYC_MV_COLLISIONS_VEHICLES_PARQUET_PATH).cache()
  }

  private def nycTreeCensusDFParquet: DataFrame = {
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
  test("Time of day with the most cyclist injures or deaths caused by a motor vehicle collision?") {
    val expected = Array(
      Row("18:00", 476),
      Row("17:00", 420),
      Row("19:00", 411)
    )

    Assignment2.problem1(nycMvCrashesDFParquet) must equal (expected)
  }

  /**
    * What percentage of accidents had alcohol as a contributing factor?
    */
  test("Percentage of accidents where alcohol was a contributing factor") {
    Assignment2.problem2(nycMvCrashesDFParquet) must equal (1.0052881751979235 +- 0.0003)
  }

  /**
    * What is the top five most frequent contributing factors for accidents in NYC?
    */
  test("Top five most frequent contributing factors for accidents in NYC") {
    val expected = Array(
      Row("Driver Inattention/Distraction", 313879),
      Row("Failure to Yield Right-of-Way", 95574),
      Row("Following Too Closely", 84660),
      Row("Backing Unsafely", 63847),
      Row("Other Vehicular", 52700)
    )

    Assignment2.problem3(nycMvCrashesDFParquet) must equal(expected)
  }

  /**
   * Which zip code had the largest number of nonfatal and fatal accidents?
   */
  test("Zip codes with most nonfatal and fatal accidents") {
    val expected = Array(
      Row("11207", 14794),
      Row("11203", 11546),
      Row("11236", 10883)
    )

    Assignment2.problem4(nycMvCrashesDFParquet) must equal(expected)
  }

  /**
   * Which vehicle make, model, and year was involved in the most accidents?
   */
  test("Vehicle make, model, and year with the most accidents") {
    val expected = Array(
      Row("TOYT -CAR/SUV", "TOYT CAM", 2015, 565),
      Row("HOND -CAR/SUV", "van", 2004, 1)
    )

    Assignment2.problem5(nycMvVehiclesDFParquet) must equal(expected)
  }

  /**
   * How do the number of collisions in an area of NYC correlate to
   * the number of trees in the area?
   */
  test("Number of collisions compared to the number of trees by zip code") {
    val expected = Array(
      Row("11207", 20798, 8634),
      Row("11101", 16000, 3387),
      Row("10019", 15311, 1715),
      Row("10036", 14786, 894),
      Row("10016", 14780, 1872)
    )

    Assignment2.problem6(nycMvCrashesDFParquet, nycTreeCensusDFParquet) must equal(expected)
  }

  /**
   * What is the average number of crashes per year in NYC between the years 2012 and 2020?
   */
  test("Average number of crashes per year") {
    Assignment2.problem7(nycMvCrashesDFParquet)
  }
}
