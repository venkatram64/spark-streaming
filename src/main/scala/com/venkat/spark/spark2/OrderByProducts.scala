package com.venkat.spark.spark2

import org.apache.spark.sql.SparkSession

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
//https://www.youtube.com/watch?v=uXoFED4IBos
object OrderByProducts {

  def main(args: Array[String]): Unit = {

    val appConf = ConfigFactory.load()

    val sparkSession = SparkSession
      .builder
      .master(appConf.getConfig("dev").getString("deploymentMaster"))
      .appName("OrderByProducts")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val inputPath = "D:/freelance-work/spark/spark-streaming/order_items.txt" //args(0)
    val outputPath = "D:/venn/output" //args(1)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputPathExists = fs.exists(new Path(inputPath))
    val outPathExists = fs.exists(new Path(outputPath))

    if (!inputPathExists) {
      println("Invalid input path")
      return
    }

    if (outPathExists) {
      fs.delete(new Path(outputPath),true)
    }

    val orderItemsRDD = sc.textFile(inputPath)

    //orderItemsRDD.take(10).foreach(println)

    val orderItems = orderItemsRDD.
            map(orderItem => (orderItem.split(",")(1).toInt,
                        orderItem.split(",")(4).toFloat))

    //orderItems.take(10).foreach(println)

    val orderRev = orderItems.reduceByKey((total, value) => total + value)
    //orderRev.filter(rec => rec._1 == 4).collect().foreach(println)
    orderRev.take(10).foreach(println)

    val orderAggregate = orderItems.aggregateByKey((0.0, 0))(
      (total, element) => (total._1 + element, total._2 +1),
      (finalTotal, interTotal) => (finalTotal._1 + interTotal._1,
            finalTotal._2 + interTotal._2)
    )


    orderAggregate.take(10).foreach(println)
    println("******with aggregate key*****")
    //https://www.youtube.com/watch?v=2aoEaGhyCQw

    val orderAggregate3 = orderItems.aggregateByKey((0.0, 0.0))(
      (total, element) => (total._1 + element, if(element > total._1) element else total._1),
      (finalTotal, interTotal) => (finalTotal._1 + interTotal._1,
        finalTotal._2 + interTotal._2)
    )


    orderAggregate3.take(10).foreach(println)



    println("******with reduce key*****")
    val orderAggregate2 = orderItemsRDD
          .map(orderItem =>(orderItem.split(",")(1).toInt,
            (orderItem.split(",")(4).toFloat,1)))
            .reduceByKey((total, element) => (total._1 + element._1 ,
            total._2 + element._2));


    orderAggregate2.take(10).foreach(println)
  }

}
