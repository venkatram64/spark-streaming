package com.venkat.spark.spark2

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by VenkatramR on 7/21/2017.
  */
object RDDToDataSet extends App{

  val sparkSession = SparkSession
    .builder()
    .master("local")
    .appName("RDDToDataSet")
    .getOrCreate()

  val sparkContext = sparkSession.sparkContext

  import sparkSession.implicits._

  val rdd = sparkContext.textFile("Names.txt")
  val ds = sparkSession.read.text("Names.txt").as[String]

  println("Count: ")

  println(rdd.count())
  println(ds.count())

  println("Word Count")

  val wordsRDD = rdd.flatMap(value => value.split(","))
  val wordsPair = wordsRDD.map(word => (word, 1))
  val wordCount = wordsPair.reduceByKey(_+_)
  println(wordCount.collect.toList)

  val wordsDS = ds.flatMap(value => value.split(","))
  val wordsPairDS = wordsDS.groupByKey(value => value)
  val wordCountDS = wordsPairDS.count()
  wordCountDS.show()

  //cache
  rdd.cache()
  ds.cache()

  //filter
  val filteredRDD = wordsRDD.filter(value => value != "Reta")
  println(filteredRDD.collect().toList)
  val filteredDS = wordsDS.filter(value => value != "Reta")
  filteredDS.show()

  //map partitions
  val mapPartitionsRDD = rdd.mapPartitions(iterator => List(iterator.count(value => true)).iterator)
  println(s" the count of each partition is ${mapPartitionsRDD.collect().toList}")

  val mapPartitionsDS = ds.mapPartitions(iterator =>List(iterator.count(value => true)).iterator)
  mapPartitionsDS.show()

  //converting to each other
  val dsToRDD = ds.rdd
  println(dsToRDD.collect().toList)

  val rddStringToRowRDD = rdd.map(value => Row(value))
  val dfSchema = StructType(Array(StructField("value",StringType)))
  val rddToDF = sparkSession.createDataFrame(rddStringToRowRDD, dfSchema)
  val rddToDataSet = rddToDF.as[String]
  rddToDataSet.show()
}
