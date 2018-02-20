package com.venkat.spark.spark2

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
//https://www.youtube.com/watch?v=l07tS62bQ1E
object GroupByProducts2{

  def main(args: Array[String]): Unit = {

    val appConf = ConfigFactory.load()

    val sparkSession = SparkSession
      .builder
      .master(appConf.getConfig("dev").getString("deploymentMaster"))
      .appName("GroupByProducts2")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val inputPath = "D:/freelance-work/spark/spark-streaming/products.txt" //args(0)
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

    val productsRDD = sc.textFile(inputPath)

    //productsRDD.take(10).foreach(println)

    val productsMap = productsRDD
        .filter(prod => prod.split(",")(4) != "")
        .map(prod =>{
          val p = prod.split(",")
          (p(1).toInt, prod)
        })

    //productsMap.take(5).foreach(println)

    val productsGroupByCategory = productsMap.groupByKey

    //productsGroupByCategory.take(5).foreach(println)

   /* val prodsGroupBy = productsGroupByCategory.map(rec => (rec._1, rec._2.size))

    prodsGroupBy.take(5).foreach(println)*/

    //dense rank
    productsGroupByCategory
        .flatMap(rec => {
          val topNprices = rec._2.toList.map(rec => rec.split(",")(4).toFloat).
            sortBy(k => -k).
            distinct.
            take(5)
          rec._2.toList.sortBy(r => -r.split(",")(4).toFloat).
            takeWhile(rec => topNprices.contains(rec.split(",")(4).toFloat))
        }).take(100).foreach(println)

  }

}
