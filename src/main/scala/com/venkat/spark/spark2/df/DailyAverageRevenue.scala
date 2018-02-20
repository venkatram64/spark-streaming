package com.venkat.spark.spark2.df

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
//https://www.youtube.com/watch?v=-iLJQVbRe90&list=PLf0swTFhTI8pYx_X_fYpWkCfHozmMsyij&index=52
/*
vi build.sbt
mkdir -p src/main/scala
mkdir -p src/main/resources
ls -altr
sbt package

find project -name "*"

sbt eclipse

ls -ltr sprark*
tar xzf file.tgz
x-> extraction
z-> unzip
f-> file

ls -ltr | grep spark

ln -s spark-1.6.2-bin-hadoop2.6 spark  (soft link)

spark-shell (to run the spark)

ls -altr
sbt package
sbt .classpath

Spartk is distributed computing engine
It works on many file systems - typically distributed ones
It uses HDFS API's for reading files from file system
Works seamlessly on HDFS, AWS S3 and Azure Blob etc

to start the spark shell
spark-shell --master yarn --conf spark.ui.port=45162

cd ~;mkdir -p sparkDemo/scala/data; cd sparkDemo/scala/data

sudo -u hdfs hadoop fs -mkdir /user/root; sudo -u hdfs hadoop fs -chown root /user/root

hadoop fs -ls /user

hadoop fs -mkdir -p /user/root/spark_demo/scala

hadoop fs -put /root/spark_demo/scala/data /user/root/spark_demo/scala

find /usr/htp -name "*spark*core*.jar"

set -o vi   to see the history
 */
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
