package com.venkat.spark.spark1.kafka

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by venkatram.veerareddy on 8/14/2017.
  */
object KafkaReader {

  System.setProperty("hadoop.home.dir", "C:/srijan")

  val ssc = new StreamingContext("local[2]","KafkaProcessor",Seconds(1))

  val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")

  /*C:\Venkatram\kafka\kafka_2.10-0.10.2.1\bin\windows>kafka-console-producer.bat --broker-list localhost:9092 --topic test-topic1 < C:\Venkatram\Names.txt*/

  val topics = List("test-topic1").toSet

  val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
  //lines.print()

  val words = lines.flatMap(_.split(","))
  val pairs = words.map(word => (word, 1)).reduceByKeyAndWindow(_ + _, _-_,Seconds(300), Seconds(1))
  val wordCounts = pairs.reduceByKey(_ + _)
  wordCounts.print()

  ssc.checkpoint("C:/venkat/checkpoint/")
  ssc.start()
  ssc.awaitTermination()

}
