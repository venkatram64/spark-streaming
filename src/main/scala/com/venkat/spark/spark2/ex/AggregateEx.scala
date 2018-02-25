package com.venkat.spark.spark2.ex

import com.typesafe.config.ConfigFactory
import com.venkat.spark.spark2.ex.JoinEx.sc
import com.venkat.spark.spark2.ex.LRJoin.sc
import org.apache.spark.sql.SparkSession

object AggregateEx extends App{

  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession
    .builder
    .master(appConf.getConfig("dev").getString("deploymentMaster"))
    .appName("AggregateEx")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val ordersRDD = sc.textFile("orders.txt")
  val orderItemssRDD = sc.textFile("order_items.txt")
  //Aggregations - using actions
  val orderStatusCountByKey = ordersRDD.map(order => (order.split(",")(3),1)).countByKey()
  orderStatusCountByKey.foreach(println)

  //Aggregations - Global using actions

  val orderItemsRevenue = orderItemssRDD.map(oi => oi.split(",")(4).toFloat)
  val orderItemsRevenueTotal = orderItemsRevenue.reduce((total,revenue) => total + revenue)
  println("Total Revenue: "+ orderItemsRevenueTotal)
  val orderItemsRevenueMaxTotal = orderItemsRevenue.reduce((max,revenue) => {
    if(max < revenue) revenue else max
  })
  println("Total Max Revenue: "+ orderItemsRevenueMaxTotal)

}
