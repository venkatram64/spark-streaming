package com.venkat.spark.spark2.df

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
//https://www.youtube.com/watch?v=-iLJQVbRe90&list=PLf0swTFhTI8pYx_X_fYpWkCfHozmMsyij&index=52
case class Orders(
  order_id: Int,
  order_date: String,
  order_customer_id: Int,
  order_status: String
)

case class OrderItems(
   order_item_id: Int,
   order_item_order_id: Int,
   order_item_product_id: Int,
   order_item_quantity: Int,
   order_item_subtotal: Float,
   order_item_price: Float
 )

object DailyAverageRevenue {

  def main(args: Array[String]): Unit = {

    val appConf = ConfigFactory.load()

    val sparkSession = SparkSession
      .builder
      .master(appConf.getConfig("dev").getString("deploymentMaster"))
      .appName("DailyAverageRevenue")
      .getOrCreate()

    sparkSession.sqlContext.setConf("spark.sql.shuffle.partitions","2")

    val sc = sparkSession.sparkContext

    import sparkSession.implicits._

    val inputPath = "D:/freelance-work/spark/spark-streaming" //args(0)
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

    val ordersDF = sc.textFile(inputPath +"/orders.txt")
          .map(rec =>{
            val a = rec.split(",")
            Orders(a(0).toInt, a(1).toString, a(2).toInt,a(3).toString)
          }).toDF()

    //ordersDF.take(10).foreach(println)

    val orderItemsDF = sc.textFile(inputPath +"/order_items.txt")
      .map(rec =>{
        val a = rec.split(",")
        OrderItems(a(0).toInt, a(1).toInt, a(2).toInt,a(3).toInt,
          a(4).toFloat,a(5).toFloat)
      }).toDF()

    //orderItemsDF.take(10).foreach(println)

    val ordersFiltered = ordersDF
          .filter(ordersDF("order_status") === "COMPLETE")

    val ordersJoin = ordersFiltered.join(orderItemsDF,
      ordersFiltered("order_id") === orderItemsDF("order_item_order_id"))

    val avgPerDaySortedByDate = ordersJoin
            .groupBy("order_date")
            .agg(sum("order_item_subtotal"))
            .sort("order_date")

    //avgPerDaySortedByDate.collect().foreach(println)
    avgPerDaySortedByDate.show()

    avgPerDaySortedByDate.rdd.saveAsTextFile(outputPath)

  }

}
