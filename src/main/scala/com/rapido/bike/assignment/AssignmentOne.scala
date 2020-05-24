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

    val df = sparkSession.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/rapido/dataset/ct_rr.csv")

    val filtered_df = df
      .groupBy("number")
      .agg(
        YearlyAvg(df.col("ts")).alias("yearly_avg"),
        MonthlyAvg(df.col("ts")).alias("monthly_avg"),
        WeeklyAvg(df.col("ts")).alias("weekly_avg"),
        DailyAvg(df.col("ts")).alias("daily_avg"),
        HourlyAvg(df.col("ts")).alias("hourly_avg")
      )

    filtered_df.printSchema()

    filtered_df
      .repartition(1)
      .write
      .option("header", "true")
      .option("compression", "none")
      .mode("overwrite")
      .csv("src/main/resources/rapido/average/")

    filtered_df.show(20, false)
  }
}
