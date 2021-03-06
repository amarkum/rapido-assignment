package com.rapido.bike.assignment

import com.rapido.bike.sparkudaf.{DailyAvg, HourlyAvg, MonthlyAvg, WeeklyAvg, YearlyAvg}
import org.apache.spark.sql.SparkSession

/**
 * Problem Statement : Rapido
 *
 * 1. Calculate hourly, daily, weekly and monthly averages of aggregated counts of each
 * customer and make this accessible for querying purposes
 */
object Assignment {

  def main(args: Array[String]): Unit = {

    val YearlyAvg = new YearlyAvg()
    val MonthlyAvg = new MonthlyAvg()
    val WeeklyAvg = new WeeklyAvg()
    val DailyAvg = new DailyAvg()
    val HourlyAvg = new HourlyAvg()

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    val dataFrame = sparkSession.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/rapido/dataset/ct_rr.csv")

    /**
     * As dataset is already in sorted order by ts(timestamp) we can invoke UDAF which compares deviation from previous
     *
     * The average is calculated based on the usage. i.e relative average calculation.
     * If a user takes only 4 rides in a year 2 in one hour & 3 in any other hour.
     *
     * Hourly Avg is Total Rides/Distinct Hours = 2+3/2 = 2.5
     * and not 2+3/Total Hours in entire Day, Week, Month, Year etc.
     *
     */
    val averagedDataframe = dataFrame
      .groupBy("number")
      .agg(
        YearlyAvg(dataFrame.col("ts")).alias("yearly_avg"),
        MonthlyAvg(dataFrame.col("ts")).alias("monthly_avg"),
        WeeklyAvg(dataFrame.col("ts")).alias("weekly_avg"),
        DailyAvg(dataFrame.col("ts")).alias("daily_avg"),
        HourlyAvg(dataFrame.col("ts")).alias("hourly_avg")
      )

    averagedDataframe
      // Flush into a single file for easy viewing
      .repartition(1)
      .write
      .option("header", "true")
      .option("compression", "none")
      .mode("overwrite")
      .csv("src/main/resources/rapido/average/")

    // View sample results in console.
    averagedDataframe.show(20, false)
  }
}
