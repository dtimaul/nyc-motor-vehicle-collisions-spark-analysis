package com.spark.assignment2

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object Assignment2 {
  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("M/d/yyyy H:mm")

  /**
   * What time of day sees the most cyclist injures or deaths caused by a motor vehicle collision?
   */
  def problem1(collisions: DataFrame): Seq[Row] = {
    val modifiedCollisionsDF = collisions
      .withColumn("NUMBER_OF_CYCLIST_INJURED", col("NUMBER_OF_CYCLIST_INJURED").cast(IntegerType))
      .withColumn("NUMBER_OF_CYCLIST_KILLED", col("NUMBER_OF_CYCLIST_KILLED").cast(IntegerType))

    modifiedCollisionsDF
      .filter("NUMBER_OF_CYCLIST_INJURED > 0 or NUMBER_OF_CYCLIST_KILLED > 0")
      .groupBy("CRASH_TIME")
      .count()
      .orderBy(desc("count"))
      .head(3)
  }

  /**
   * What percentage of accidents had alcohol as a contributing factor?
   */
  def problem2(collisions: DataFrame): Double = {
    val numTotalAccidents = collisions.count()
    val numAlcoholRelatedAccidents = collisions
      .filter("CONTRIBUTING_FACTOR_VEHICLE_1 == 'Alcohol Involvement' or " +
        "CONTRIBUTING_FACTOR_VEHICLE_2 == 'Alcohol Involvement' or " +
        "CONTRIBUTING_FACTOR_VEHICLE_3 == 'Alcohol Involvement' or " +
        "CONTRIBUTING_FACTOR_VEHICLE_4 == 'Alcohol Involvement' or " +
        "CONTRIBUTING_FACTOR_VEHICLE_5 == 'Alcohol Involvement'")
      .count()
    (numAlcoholRelatedAccidents * 100) / numTotalAccidents.toDouble
  }

  /**
   * What is the top five most frequent contributing factors for accidents in NYC?
   */
  def problem3(collisions: DataFrame): Seq[Row] = {
      collisions
        .groupBy("CONTRIBUTING_FACTOR_VEHICLE_1")
        .count()
        .orderBy(desc("count"))
        .head(5)
  }

  /**
   * Which zip code had the largest number of non fatal and fatal accidents?
   */
  def problem4(collisions: DataFrame): Seq[Row] = {
    val modifiedCollisionsDF = collisions
      .withColumn("NUMBER_OF_CYCLIST_INJURED", col("NUMBER_OF_CYCLIST_INJURED").cast(IntegerType))
      .withColumn("NUMBER_OF_CYCLIST_KILLED", col("NUMBER_OF_CYCLIST_KILLED").cast(IntegerType))
      .withColumn("NUMBER_OF_PERSONS_INJURED", col("NUMBER_OF_PERSONS_INJURED").cast(IntegerType))
      .withColumn("NUMBER_OF_MOTORIST_INJURED", col("NUMBER_OF_MOTORIST_INJURED").cast(IntegerType))

    val modifiedCollisionsDF1 = modifiedCollisionsDF.withColumn("TOTAL_INJURED_OR_KILLED",
      col("NUMBER_OF_PERSONS_INJURED") + col("NUMBER_OF_PERSONS_KILLED")
        + col("NUMBER_OF_PEDESTRIANS_INJURED") + col("NUMBER_OF_PEDESTRIANS_KILLED")
        + col("NUMBER_OF_CYCLIST_INJURED") + col("NUMBER_OF_CYCLIST_KILLED")
        + col("NUMBER_OF_MOTORIST_INJURED") + col("NUMBER_OF_MOTORIST_KILLED"))

    //TODO remove rows with null zip codes

    val totalInjuriesAndFatalitiesByZipCode = modifiedCollisionsDF1
      .groupBy(col("ZIP_CODE"))
      .agg(sum(col("TOTAL_INJURED_OR_KILLED")).alias("TOTAL_INJURIES_AND_FATALITIES"))

    totalInjuriesAndFatalitiesByZipCode.orderBy(desc("TOTAL_INJURIES_AND_FATALITIES")).head(5).toSeq
  }


  def problem5(collisions: DataFrame): DataFrame = {
    collisions.select("CRASH_DATE")
  }

  def problem6(collisions: DataFrame): DataFrame = {
    collisions.select("CRASH_DATE")
  }

  def problem7(collisions: DataFrame): DataFrame = {
    collisions.select("CRASH_DATE")
  }



}
