package com.venkat.spark.spark2

import org.apache.spark.sql.SparkSession

/**
  * Created by VenkatramR on 7/21/2017.
  */
object WordCount extends App{

  val sparkSession = SparkSession
    .builder
    .master("local")
    .appName("WordCount")
    .getOrCreate()

  import sparkSession.implicits._
  val data = sparkSession.read.text("WordCount.csv").as[String]

  val words = data.flatMap(value => value.split(","))
  val groupedWords = words.groupByKey(_.toLowerCase())
  val counts = groupedWords.count()

  counts.show()

}
