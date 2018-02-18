package com.venkat.spark.spark2

import com.typesafe.config.ConfigFactory
import com.venkat.spark.spark2.WordCount.sparkSession
import org.apache.spark.sql.SparkSession

object ParallelEx extends App{

  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession
    .builder
    .master(appConf.getConfig("dev").getString("deploymentMaster"))
    .appName("WordCount")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val data = Array(1,2,3,4,5,6)
  val rddData = sc.parallelize(data)
  println(rddData.reduce((acc, value) => acc + value))

  import sparkSession.implicits._
  val data2 = sparkSession.read.text("numberData.txt").as[String]

  val total = data2.map(rec => rec.toInt).reduce((acc,value) => acc + value)

  println(total)

}
