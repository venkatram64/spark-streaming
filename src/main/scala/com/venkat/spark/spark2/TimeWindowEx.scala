package com.venkat.spark.spark2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by VenkatramR on 7/21/2017.
  */
object TimeWindowEx extends App{

  def printWindow(windowDF: DataFrame, aggCol:String) = {
    windowDF.sort("window.start").select("window.start", "window.end", s"$aggCol")
      .show(truncate = false)
  }

  val sparkSession = SparkSession
    .builder()
    .master("local")
    .appName("TimeWindowEx")
    .getOrCreate()

  val stockDF = sparkSession.read.option("header","true")
    .option("inferSchema", "true")
    .csv("applestock.csv")

  //weekly average of 2016
  val stocks2016 = stockDF.filter("year(Date) == 2016")

  val tumblingWindowDS = stocks2016.groupBy(window(stocks2016.col("Date"), "1 week"))
    .agg(avg("Close").as("weekly_average"))
  println("weekly average in 2016 using tumbling window is")
  printWindow(tumblingWindowDS, "weekly_average")

  val windowWithStartTime = stocks2016.groupBy(window(stocks2016.col("Date"), "1 week", "1 week", "4 days"))
    .agg(avg("Close").as("weekly_average"))
  println("weekly average in 2016 using sliding window is")
  printWindow(windowWithStartTime, "weekly_average")

  val filteredWindow = windowWithStartTime.filter("year(window.start)=2016")
  println("weekly average in 2016 after filtering is")
  printWindow(filteredWindow, "weekly_average")

}
