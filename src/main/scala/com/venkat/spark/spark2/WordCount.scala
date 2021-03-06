package com.venkat.spark.spark2

import com.typesafe.config.ConfigFactory
import org.apache.commons.configuration.tree.xpath.ConfigurationNodePointerFactory
import org.apache.spark.sql.SparkSession


/**
  * Created by VenkatramR on 7/21/2017.
  */
object WordCount extends App{

  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession
    .builder
    .master(appConf.getConfig("dev").getString("deploymentMaster"))
    .appName("WordCount")
    .getOrCreate()

  import sparkSession.implicits._
  val data = sparkSession.read.text("WordCount.csv").as[String]

  val words = data.flatMap(rec => rec.split(","))
  //words.collect().foreach(println)
  words.take(5).foreach(println)
  println("*************")
  val groupedWords = words.groupByKey(_.toLowerCase())
  val counts = groupedWords.count()
  counts.show()

  //val wcMap = words.map(rec => (rec,1))
  //wcMap.take(5).foreach(println)
  //val reducByKey = wcMap.reduceBykey((acc, value) => acc + value)



}
