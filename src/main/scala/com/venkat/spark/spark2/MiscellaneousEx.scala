package com.venkat.spark.spark2

import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by VenkatramR on 7/21/2017.
  */
object MiscellaneousEx extends App{

  val sparkSession = SparkSession
    .builder()
    .master("local")
    .appName("RDDToDataSet")
    .getOrCreate()

  val sparkContext = sparkSession.sparkContext


  val doubleRDD = sparkContext.makeRDD(List(2.0,4.0,8.2,11.9,17.0))
  val rddSum = doubleRDD.sum()
  val rddMean = doubleRDD.mean()

  println(s"sum is $rddSum")
  println(s"maen is $rddMean")

  val rowRDD = doubleRDD.map(value => Row.fromSeq(List(value)))
  val schema = StructType(Array(StructField("value", DoubleType)))
  val doubleDS = sparkSession.createDataFrame(rowRDD, schema)

  import org.apache.spark.sql.functions._
  doubleDS.agg(sum("value")).show()
  doubleDS.agg(mean("value")).show()

  //reduce function
  val rddReduce = doubleRDD.reduce((a,b) => a + b)
  val dsReduce = doubleDS.reduce((row1, row2) => Row(row1.getDouble(0) + row2.getDouble(0)))

  println(s"rdd reduce is $rddReduce dataset reduce $dsReduce")

}
