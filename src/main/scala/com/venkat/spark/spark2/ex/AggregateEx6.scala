package com.venkat.spark.spark2.ex

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

//https://www.youtube.com/watch?v=95s8fXET8sw&list=PLf0swTFhTI8rDQXfH8afWtGgpOTnhebDx&index=82

class AggregateEx6 {
  def getTopNPricedProducts(productsIterable: Iterable[String], topN: Int): Iterable[String] = {

    val productPrices = productsIterable.map(p => p.split(",")(4).toFloat).toSet //elminate duplicates
    val topNPrices = productPrices.toList.sortBy(p => -p).take(topN) //to sort

    val productsSorted = productsIterable.toList.sortBy(product => -product.split(",")(4).toFloat)
    val minOfTopNprices = topNPrices.min

    val topNPricedProducts = productsSorted
      .takeWhile(product => product.split(",")(4).toFloat >= minOfTopNprices)

    topNPricedProducts
  }
}

object AggregateEx6 extends App{

  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession
    .builder
    .master(appConf.getConfig("dev").getString("deploymentMaster"))
    .appName("AggregateEx6")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val products = sc.textFile("products.txt")

  //Ranking Get top N priced products with in each product category

  val productsMap = products
    .filter(product => product.split(",")(4) != "")
    .map(product => (
      (product.split(",")(1).toFloat), product)
    )

  val productsGroupByCategory = productsMap.groupByKey()

  //productsGBK.take(10).foreach(println)

  val ex6 = new AggregateEx6()

  val top3PricedProductsByCat = productsGroupByCategory.flatMap(
    rec => ex6.getTopNPricedProducts(rec._2,3)
  )

  top3PricedProductsByCat.collect.foreach(println)

}

