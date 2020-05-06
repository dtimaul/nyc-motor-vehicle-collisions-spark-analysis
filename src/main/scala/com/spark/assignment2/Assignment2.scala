package com.spark.assignment2

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Assignment2 {
  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("M/d/yyyy H:mm")

  def problem1(collisions: DataFrame): DataFrame = {
    collisions.select("CRASH_DATE")
  }

  def problem2(collisions: DataFrame): DataFrame = {
    collisions.select("CRASH_DATE")
  }

  /**
   * What is the top 5 most frequent contributing factor for accidents in NYC?
   */
  def problem3(collisions: DataFrame): DataFrame = {
      collisions.groupBy("CONTRIBUTING_FACTOR_VEHICLE_1").count().sort()
  }

  def problem4(collisions: DataFrame): DataFrame = {
    collisions.select("CRASH_DATE")
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