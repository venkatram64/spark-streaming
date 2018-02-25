package com.venkat.spark.spark2.ex

import com.typesafe.config.ConfigFactory

import org.apache.spark.sql.SparkSession

object AggregateEx3 extends App{

  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession
    .builder
    .master(appConf.getConfig("dev").getString("deploymentMaster"))
    .appName("AggregateEx3")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val orderItemsRDD = sc.textFile("order_items.txt")

  val orderItemsMap = orderItemsRDD.map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))
  //orderItemsMap.foreach(println)


  //(22506,CompactBuffer(149.94, 199.98, 31.98))
  val revenuePerOrderId = orderItemsMap.reduceByKey((total, revenue) => total + revenue)
  println("Revenue per order id")
  revenuePerOrderId.foreach(println)

  val minRevenuePerOrderId = orderItemsMap.reduceByKey((min, revenue) => {
    if(min > revenue) revenue else min
  })
  println("Min Revenue")
  minRevenuePerOrderId.foreach(println)

  //same thing we can achieve in one iteration

  println("Aggregation AGGREGATE BY KEY")

  //(order_id, order_item_subtotal)
  val revenueAndMaxPerOrderId = orderItemsMap.aggregateByKey((0.0f, 0.0f))(
    (inter, subTotal) => (inter._1 + subTotal, if(subTotal > inter._2) subTotal else inter._2),//intermediate value
    (total, inter) => (total._1 + inter._1, if(total._2 > inter._2) total._2 else inter._2)
  )
  //(order_id, (order_revenue, max_order_item_subtotal))

  revenueAndMaxPerOrderId.sortByKey().take(10).foreach(println)

}
