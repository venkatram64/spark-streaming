package com.venkat.spark.spark2.ex

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object MyRDD extends App{

  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession
    .builder
    .master(appConf.getConfig("dev").getString("deploymentMaster"))
    .appName("MyRDD")
    .getOrCreate()

  val productRaw = scala.io.Source.fromFile("products.txt").getLines().toList
  //productRaw.foreach(println)

  val sc = sparkSession.sparkContext

  val productRDD = sc.parallelize(productRaw)

  //productRDD.take(10).foreach(println)

  productRDD.takeSample(true,100,1234).foreach(println)



}
