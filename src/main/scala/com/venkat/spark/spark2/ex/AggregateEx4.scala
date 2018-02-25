package com.venkat.spark.spark2.ex

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object AggregateEx4 extends App{

  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession
    .builder
    .master(appConf.getConfig("dev").getString("deploymentMaster"))
    .appName("AggregateEx4")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val products = sc.textFile("products.txt")

  val productsMap = products.map(product => (product.split(",")(1).toInt, product))

  //productsMap.take(10).foreach(println)
  val productsSortedByCategoryId = productsMap.sortByKey(false)
  //productsSortedByCategoryId.take(100).foreach(println)

  //composit key

  val productsMap2 = products
      .filter(product => product.split(",")(4) != "")
      .map(product => (
        (product.split(",")(1).toInt, -product.split(",")(4).toFloat), product)
      )

  val productsSortedByCategoryId2 = productsMap2.sortByKey()
              .map(rec => rec._2) //showing only category and product
  productsSortedByCategoryId2.take(100).foreach(println)

}
