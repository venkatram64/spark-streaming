package com.venkat.spark.spark2.ex

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object AggregateEx7 extends App{

  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession
    .builder
    .master(appConf.getConfig("dev").getString("deploymentMaster"))
    .appName("AggregateEx7")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val orders = sc.textFile("orders.txt")

  //set operations

  val customers_201308 = orders
                .filter(order => order.split(",")(1).contains("2013-08"))
                .map(order => order.split(",")(2).toInt)

  val customers_201309 = orders
    .filter(order => order.split(",")(1).contains("2013-08"))
    .map(order => order.split(",")(2).toInt)

  //Get all the customers who placed orders in 2013 August and 2013 September
  val customers_201308_and__201309 = customers_201308.intersection(customers_201309)

  //customers_201308_and__201309.collect().foreach(println)

  //Get all unique customers who placed orders in 2013 August or 2013 September

  val customers_201308_or__201309 = customers_201308.union(customers_201309)

  customers_201308_or__201309.collect().distinct.foreach(println)

  //Get all customers who placed orders in 2013 August but not in 2013

/*  val customers_201308_minus__201309 = customers_201308
                     .map(c => (c,1))
                     .leftOuterJoin(customers_201309.map(c =>(c,1)))
                     .filter(rec => rec._2._2 == None)*/


}
