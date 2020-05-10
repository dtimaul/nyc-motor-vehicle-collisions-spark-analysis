package com.spark.assignment2

import java.time.format.DateTimeFormatter

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

object Assignment2 {
  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("M/d/yyyy H:mm")

  /**
   * What is the top five most frequent contributing factors for accidents in NYC?
   */
  def problem1(collisions: DataFrame): Seq[Row] = {
    // Filter out rows with an unspecified contributing factor
    val collisionsDFModified = collisions.filter(col("CONTRIBUTING_FACTOR_VEHICLE_1") =!= "Unspecified")

    // Perform a groupBy CONTRIBUTING_FACTOR_VEHICLE_1, then count the number of occurrences of each contributing factor.
    // Finally, order by count and get the top 5
    collisionsDFModified
      .groupBy("CONTRIBUTING_FACTOR_VEHICLE_1")
      .count()
      .orderBy(desc("count"))
      .head(5)
  }

  /**
   * What percentage of accidents had alcohol as a contributing factor?
   */
  def problem2(collisions: DataFrame): Double = {
    val numTotalAccidents = collisions.count()

    // Filter accidents where alcohol involvement was a contributing factor for any of the vehicles involved in the accident
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
   * What time of day sees the most cyclist injures or deaths caused by a motor vehicle collision?
   */
  def problem3(collisions: DataFrame): Seq[Row] = {
    // Filter accidents where there was at least one cyclist injury or fatality
    collisions
      .filter("NUMBER_OF_CYCLIST_INJURED > 0 or NUMBER_OF_CYCLIST_KILLED > 0")
      .groupBy("CRASH_TIME")
      .count()
      .orderBy(desc("count"))
      .head(3)
  }

  /**
   * Which zip code had the largest number of nonfatal and fatal accidents?
   */
  def problem4(collisions: DataFrame): Seq[Row] = {
    // Remove rows with null zip codes
    val modifiedCollisionsDF = collisions.filter(col("ZIP_CODE").isNotNull)

    // Create new column "TOTAL_INJURED_OR_KILLED", which is the sum of all nonfatal and fatal injuries for each accident
    val modifiedCollisionsDF1 = modifiedCollisionsDF.withColumn("TOTAL_INJURED_OR_KILLED",
      col("NUMBER_OF_PERSONS_INJURED") + col("NUMBER_OF_PERSONS_KILLED")
        + col("NUMBER_OF_PEDESTRIANS_INJURED") + col("NUMBER_OF_PEDESTRIANS_KILLED")
        + col("NUMBER_OF_CYCLIST_INJURED") + col("NUMBER_OF_CYCLIST_KILLED")
        + col("NUMBER_OF_MOTORIST_INJURED") + col("NUMBER_OF_MOTORIST_KILLED"))

    // Get the total number of nonfatal and fatal injuries per zip code by first performing a group by zip code.
    // then use the agg and sum functions to sum up the values in the TOTAL_INJURED_OR_KILLED column per zip code.
    // Finally, store the computed output per zip code in a column aliased as TOTAL_INJURIES_AND_FATALITIES
    val totalInjuriesAndFatalitiesByZipCode = modifiedCollisionsDF1
      .groupBy(col("ZIP_CODE"))
      .agg(sum(col("TOTAL_INJURED_OR_KILLED")).alias("TOTAL_INJURIES_AND_FATALITIES"))

    // Order the results by TOTAL_INJURIES_AND_FATALITIES in descending order
    totalInjuriesAndFatalitiesByZipCode.orderBy(desc("TOTAL_INJURIES_AND_FATALITIES")).head(3).toSeq
  }

  /**
   * Which vehicle make, model, and year was involved in the most accidents? Which had the least accidents?
   */
  def problem5(vehicles: DataFrame): Seq[Row] = {
    // Remove any rows with null values for vehicle make, model, and year
    val vehiclesDFModified = vehicles.filter(col("VEHICLE_MAKE").isNotNull && col("VEHICLE_MODEL").isNotNull)

    // Get the vehicle make, model, and year with the most accidents
    val vehicleWMostAccidents = vehiclesDFModified.groupBy("VEHICLE_MAKE", "VEHICLE_MODEL", "VEHICLE_YEAR").count().orderBy(desc("count")).first()

    // Get the vehicle make, model, and year with the least accidents
    val vehicleWLeastAccidents = vehiclesDFModified.groupBy("VEHICLE_MAKE", "VEHICLE_MODEL", "VEHICLE_YEAR").count().orderBy(asc("count")).first()

    Seq[Row] (
      vehicleWMostAccidents,
      vehicleWLeastAccidents
    )
  }

  /**
   * How do the number of collisions in an area of NYC correlate to
   * the number of trees in the area?
   */
  def problem6(collisions: DataFrame, treeCensus: DataFrame): Seq[Row] = {
    // First rename the postcode column to ZIP_CODE on the treeCensus DataFrame
    val treeCensusDFModified = treeCensus.withColumn("ZIP_CODE", col("postcode"))

    // Next, perform a groupBy on the treeCensus DataFrame to get the number of trees per zip code
    val treesPerZipCode = treeCensusDFModified.groupBy("ZIP_CODE").agg(count(col("ZIP_CODE")).alias("TOTAL_TREES"))

    // Next, perform a groupBy on the collisions DataFrame to get the number of accidents per zip code
    val accidentsPerZipCode = collisions.groupBy("ZIP_CODE").agg(count(col("ZIP_CODE")).alias("TOTAL_CRASHES"))

    // Inner equi-join the collisions DataFrame with the treeCensus DataFrame
    accidentsPerZipCode.join(treesPerZipCode, "ZIP_CODE").orderBy(desc("TOTAL_CRASHES")).head(5)
  }

  /**
   * What is the average number of people involved in crashes per year in NYC between the years 2012 and 2020?
   */
  def problem7(collisions: DataFrame): DataFrame = {
    // First create a new column with only the year for each accident
    // Next, do a group by with agg and avg function
    collisions.select("CRASH_DATE")
  }
}
