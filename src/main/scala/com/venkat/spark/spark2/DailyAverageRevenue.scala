package com.venkat.spark.spark2

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object DailyAverageRevenue {

  def main(args: Array[String]): Unit = {

    val appConf = ConfigFactory.load()

    val sparkSession = SparkSession
      .builder
      .master(appConf.getConfig("dev").getString("deploymentMaster"))
      .appName("DailyAverageRevenue")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val inputPath = "D:/freelance-work/spark/spark-streaming/orders.txt" //args(0)
    val inputPath2 = "D:/freelance-work/spark/spark-streaming/order_items.txt" //args(0)
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

    val ordersRDD = sc.textFile(inputPath)

    //ordersRDD.take(10).foreach(println)

    val orderItemsRDD = sc.textFile(inputPath2 )

    val ordersCompleted = ordersRDD
                .filter(rec => (rec.split(",")(3) == "COMPLETE"))

    //ordersCompleted.foreach(println)

    val orders = ordersCompleted
                  .map(rec => (rec.split(",")(0).toInt, rec.split(",")(1)))

    //orders.foreach(println)
    val orderItemsMap = orderItemsRDD
                      .map(rec => (rec.split(",")(1).toInt, rec.split(",")(4).toFloat))
    //orderItemsMap.foreach(println)

    val orderItems = orderItemsMap
                    .reduceByKey((acc, value) => acc + value)

    //orderItems.foreach(println)
    //orderItems.filter(rec => (rec._1 == 55207)).foreach(println)

    val ordersJoin = orders.join(orderItems)
    //ordersJoin.foreach(rec => (println(rec._1 + " : " + rec._2._1 + " : " + rec._2._2)))

    val ordersJoinMap = ordersJoin.map(rec => (rec._2._1, rec._2._2))
    //ordersJoinMap.foreach(rec => println(rec._1 + ": " + rec._2))

    val revenuePerDay = ordersJoinMap.aggregateByKey((0.0,0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),//total orders per day
      (total1, total2) => (total1._1 + total2._1, total1._2 + total2._2)
    )
    //revenuePerDay.foreach(rec => println(rec._1 +": " + rec._2._2 + ": " + rec._2._1))

    //revenue average per day
    val avgPerDay = revenuePerDay.map(rec => (rec._1,
          BigDecimal(rec._2._1/rec._2._2).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat))
    val avgPerDaySortedByDate = avgPerDay.sortByKey(false)

    avgPerDaySortedByDate.foreach(rec => println(rec._1 + ": " + rec._2))

    avgPerDaySortedByDate
        .map(rec => rec._1 +", " + rec._2)
        .saveAsTextFile(outputPath)
  }

}
