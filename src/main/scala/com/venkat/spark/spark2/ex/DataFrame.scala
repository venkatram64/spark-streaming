package com.venkat.spark.spark2.ex

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object DataFrame extends App{

  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession
    .builder
    .master(appConf.getConfig("dev").getString("deploymentMaster"))
    .appName("DataFrame")
    .getOrCreate()

  val ordersDF = sparkSession.read.format("json").load("orders.json") //data frame is distributed collection + structure
  //ordersDF.printSchema()
  ordersDF.show()


}
