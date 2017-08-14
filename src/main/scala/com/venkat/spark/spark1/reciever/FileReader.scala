package com.venkat.spark.spark1.reciever

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by venkatram.veerareddy on 8/14/2017.
  */

object FlatfileReader extends App{

  val conf = new SparkConf().setMaster("local[2]").setAppName("FlatfileReader").set("spark.executor.memory","2g")
  val sc = new StreamingContext(conf, Seconds(10))
  val personStream = sc.receiverStream(new DataReceiver())
  personStream.print()
  sc.start()
  sc.awaitTermination()
}
