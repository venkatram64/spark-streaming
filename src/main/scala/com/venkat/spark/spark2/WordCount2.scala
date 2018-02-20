package com.venkat.spark.spark2

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._


object WordCount2{

  def main(args: Array[String]): Unit ={
    val appConf = ConfigFactory.load()

    val sparkSession = SparkSession
      .builder
      .master(appConf.getConfig("dev").getString("deploymentMaster"))
      .appName("WordCount")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val inputPath =  "D:/freelance-work/spark/spark-streaming/WordCount.csv"      //args(0)
    val outputPath = "D:/venn/output"             //args(1)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputPathExists = fs.exists(new Path(inputPath))
    val outPathExists = fs.exists(new Path(outputPath))

    if(!inputPathExists){
      println("Invalid input path")
      return
    }

    if(outPathExists){
      fs.delete(new Path(outputPath),true)
    }

    val wc = sc.textFile(inputPath)
                .flatMap(rec => rec.split(","))
                .map(rec => (rec,1))
                .reduceByKey((acc,value) => acc + value)

    wc.foreach(println)

/*    wc.map(rec => rec.productIterator.mkString(("\t")))
          .saveAsObjectFile(outputPath)*/

    wc.map(rec => rec._1 + "\t" + rec._2)
      .saveAsTextFile(outputPath)




  }

}
