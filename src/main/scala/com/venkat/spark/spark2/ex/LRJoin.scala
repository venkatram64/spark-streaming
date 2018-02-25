package com.venkat.spark.spark2.ex

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object LRJoin extends App{

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
    (order.split(",")(0).toInt, order)
  })

  //ordersMap.take(10).foreach(println)

  val orderItemsMap = orderItemssRDD.map(orderItem =>{
    val oi = orderItem.split(",")
    (oi(1).toInt, orderItem)
  })

  //orderItemsMap.take(10).foreach(println)

  //Get all the orders which do not have corresponding entries in order items
  println("left outer join")
  val leftOuterJoin = ordersMap.leftOuterJoin(orderItemsMap)
  //leftOuterJoin.take(10).foreach(println)
  //(31037,(31037,2014-02-02 00:00:00.0,8301,COMPLETE,None))
  val leftOuterJoinFilter = leftOuterJoin.filter(order => order._2._2 == None)
  //leftOuterJoinFilter.foreach(println)

  val ordersWithNoOrderItems = leftOuterJoinFilter.map(order => order._2._1)

  ordersWithNoOrderItems.foreach(println)

  println("right outer join")

  val rightOuterJoin = orderItemsMap.rightOuterJoin(ordersMap)
  //rightOuterJoin.take(10).foreach(println)
  //(19021,(None,19021,2013-11-20 00:00:00.0,9532,PENDING_PAYMENT))
  val rightOuterJoinFilter = rightOuterJoin.filter(order => order._2._1 == None)
  //rightOuterJoin.foreach(println)

  val ordersWithNoOrderItems2 = rightOuterJoinFilter.map(order => order._2._2)

  ordersWithNoOrderItems2.foreach(println)

}
