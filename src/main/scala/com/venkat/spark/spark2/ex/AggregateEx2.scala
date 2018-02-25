package com.venkat.spark.spark2.ex

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object AggregateEx2 extends App{

  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession
    .builder
    .master(appConf.getConfig("dev").getString("deploymentMaster"))
    .appName("AggregateEx2")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val ordersRDD = sc.textFile("orders.txt")
  val orderItemsRDD = sc.textFile("order_items.txt")

  //aggregations - groupByKey (single threaded approach)
  //1. (1 to 1000) - sum(1 to 1000) => 1 = 2 + 3 + 1000

  //agregations with out groupByKey => divide and concur
  //1. (1 to 1000) - sum(1 to 25) sum(26 to 50) sum(51 to 75) ... sum(951 to 1000) => 1 = 2 + 3 + 1000

  //get revenue per order id, get data in descending by order item sub total, for each order it

  val orderItemsMap = orderItemsRDD.map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))
  //orderItemsMap.foreach(println)
  val orderItemsGroupByKey = orderItemsMap.groupByKey()
  //orderItemsGroupByKey.foreach(println)
  //(22506,CompactBuffer(149.94, 199.98, 31.98))
  val orderItemsSumBGroupByKey = orderItemsGroupByKey.map(rec => (rec._1,rec._2.toList.sum))
  //orderItemsSumBGroupByKey.foreach(println)

  //get data in descending order by order item subtotal for each order id
  //(22506,CompactBuffer(149.94, 199.98, 31.98))
  val ordersSortedByRevenue = orderItemsGroupByKey.flatMap(rec => {
    rec._2.toList.sortBy(o => -o).map(k => (rec._1, k))
  })
  ordersSortedByRevenue.foreach(println)

}
