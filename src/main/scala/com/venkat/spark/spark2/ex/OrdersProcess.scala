package com.venkat.spark.spark2.ex

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object OrdersProcess extends App{

  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession
    .builder
    .master(appConf.getConfig("dev").getString("deploymentMaster"))
    .appName("OrdersProcess")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val ordersRDD = sc.textFile("orders.txt")

  val orderItemssRDD = sc.textFile("order_items.txt")

  val orderDates = ordersRDD.map(rec =>{
    rec.split(",")(1).substring(0,10).replace("-","").toInt
  })

  //orderDates.take(10).foreach(println)

  val ordersPairRDD = ordersRDD.map(rec =>{
    val o = rec.split(",")
    (o(0).toInt, o(1).substring(0,10).replace("-","").toInt)
  })

  //ordersPairRDD.take(10).foreach(println)

  val orderItemsPairRDD = orderItemssRDD.map(rec => {
    (rec.split(",")(1).toInt, rec)
  })

  orderItemsPairRDD.take(10).foreach(println)

  //flat map example

  val list = collection.immutable.List("Hello", "How are you doing", "Let us perform word count",
    "As part of the word count program", "we will see how many times each word repeat")

  val listRDD = sc.parallelize(list)

  val wcFM = listRDD.flatMap(rec => rec.split(" "))
                    .map(rec => (rec,1))
                    .countByKey()
  wcFM.foreach(println)
  println("************")
  val wcM = listRDD.flatMap(rec => rec.split(" "))
  val wcMap = wcM.map(rec => (rec,1))
  val reducByKey = wcMap.reduceByKey((acc,value)=> acc + value)
  reducByKey.foreach(println)

  println("**** filtered orders ****** ")
  val completedOrders = ordersRDD.filter(order => order.split(",")(3) == "COMPLETE")
  completedOrders.foreach(println)

  //order status
  println("**** order status ****** ")
  val orderStatus = ordersRDD.map(order => order.split(",")(3)).distinct()
  orderStatus.foreach(println)

  println("**** orders form 2013-09 ****** ")

  val ordersFrom = ordersRDD.filter(order =>{
    val o = order.split(",")
    (o(3) == "COMPLETE" || o(3) == "CLOSED") && (o(1).contains("2013-09"))
  })

  ordersFrom.foreach(println)

}
