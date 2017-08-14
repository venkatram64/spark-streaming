package com.venkat.spark.spark2

import org.apache.spark.sql.SparkSession

/**
  * Created by VenkatramR on 7/21/2017.
  */
object CatalogEx extends App{

  val sparkSession = SparkSession
    .builder()
    .master("local")
    .appName("CatelogEx")
    .getOrCreate()

  val df = sparkSession.read.csv("sales.csv")
  df.createTempView("Sales")

  val catalog =sparkSession.catalog

  //print the databases
  catalog.listDatabases().select("name").show()

  //list all tables
  catalog.listTables().select("name").show()

  //is cached
  println(catalog.isCached("Sales"))
  df.cache()
  println(catalog.isCached("Sales"))

  //drop the table
  catalog.dropTempView("Sales")
  catalog.listTables().select("name").show()

  //list functions
  catalog.listFunctions().select("name","description","className","isTemporary").show(100)

}
