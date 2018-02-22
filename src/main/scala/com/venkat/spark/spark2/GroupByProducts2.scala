package com.venkat.spark.spark2

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
//https://www.youtube.com/watch?v=l07tS62bQ1E
/*
reduceByKey, aggregateByKey, groupByKey
Using these API's we can perform aggregations such as total revenue per day, minimum salaried employee
per department, average salary per department etc.  Standard aggregations
are count, sum, average, standard deviation, mode, min, max etc

reduceByKey                aggregateByKey              groupByKey

Uses Combiner              Uses Combiner                no combiner
Take one parameter         Take 2 parameters as         No parameters as functions
as function- for seqOp     functions -one for           Generally followed by map or
and combOp                 seqOp and other for combOp   flatMp

Implicit Combiner          Explicit Combiner            No Combinar

seqOp or combiner logic    seqOp or combiner logic      No Combiner
are same as combOp or      are different from combOp
final reduce logic         or final reduce logic

Input and output value     Input and output value       No parameters
type need to be same       type can be different

Performance is high for    Performance is high for      Relatively slow for
aggregations               aggregations                 aggregations

Only aggregations         Only aggregations             Any by key transformations
                                                        aggregations, sorting, ranking
                                                        etc

eg sum, min, max,etc     eg average                     eg any aggregation is possible
                                                        but not performed for
                                                        performance reasons
 */
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
