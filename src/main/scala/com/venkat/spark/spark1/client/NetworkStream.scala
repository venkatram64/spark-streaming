package com.venkat.spark.spark1.client

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Srijan on 16-07-2017.
  */
object NetworkStream extends App{

  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkStream").set("spark.driver.memory","2g")
  val sc = new StreamingContext(conf, Seconds(12))
  val lines = sc.socketTextStream("localhost",9999, StorageLevel.MEMORY_AND_DISK_SER_2)
  val words = lines.flatMap(_.split(" "))
  val pairs = words.map(word => (word,1))
  val wordCounts = pairs.reduceByKey(_+_)
  wordCounts.print()
  sc.start()
  sc.awaitTermination()

}
