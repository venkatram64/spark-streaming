package com.venkat.spark.spark2.ex

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object JoinEx extends App{

  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession
    .builder
    .master(appConf.getConfig("dev").getString("deploymentMaster"))
    .appName("JoinEx")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val ordersRDD = sc.textFile("orders.txt")

  val orderItemssRDD = sc.textFile("order_items.txt")

  val ordersMap = ordersRDD.map(order => {
    (order.split(",")(0).toInt, order.split(",")(1).substring(0, 10))
  })

  //ordersMap.take(10).foreach(println)

  val orderItemsMap = orderItemssRDD.map(orderItem =>{
    val oi = orderItem.split(",")
    (oi(1).toInt, oi(4).toFloat)
  })

  //orderItemsMap.take(10).foreach(println)

  val ordersJoin = ordersMap.join(orderItemsMap)

  ordersJoin.take(10).foreach(println)





}
