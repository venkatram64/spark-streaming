package com.venkat.spark.spark2.ex

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object AggregateEx5 extends App{

  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession
    .builder
    .master(appConf.getConfig("dev").getString("deploymentMaster"))
    .appName("AggregateEx5")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val products = sc.textFile("products.txt")

  //Ranking -Global (top 5 products)

  val productsMap = products
    .filter(product => product.split(",")(4) != "")
    .map(product => (
      (product.split(",")(4).toFloat), product)
    )

  val productsSortedByPrice = productsMap.sortByKey(false)

  productsSortedByPrice.take(10).foreach(println)
  println("***TakeOrdered Example***")
  val productsMap2 = products
      .filter(product => product.split(",")(4) != "")
      .takeOrdered(10)(Ordering[Float].reverse.on(
        product => product.split(",")(4).toFloat)
      )

  productsMap2.take(10).foreach(println)


}
