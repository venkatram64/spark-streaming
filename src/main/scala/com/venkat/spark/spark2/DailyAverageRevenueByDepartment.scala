package com.venkat.spark.spark2

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object DailyAverageRevenueByDepartment {

  def main(args: Array[String]): Unit = {

    val appConf = ConfigFactory.load()

    val sparkSession = SparkSession
      .builder
      .master(appConf.getConfig("dev").getString("deploymentMaster"))
      .appName("DailyAverageRevenueByDepartment")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val inputPath = "D:/freelance-work/spark/spark-streaming"
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

    val departments = sc.textFile(inputPath +"/departments.txt")
    val categories = sc.textFile(inputPath +"/categories.txt")
    val products = sc.textFile(inputPath +"/products.txt")

    val departmentsMap = departments
       .map(rec => (rec.split(",")(0).toInt, rec.split(",")(1)))

    //departmentsMap.collect().foreach(println)

    val categoriesMap = categories
      .map(rec => (rec.split(",")(0).toInt, rec.split(",")(1).toInt))

    val productsMap = products
      .map(rec => (rec.split(",")(1).toInt, rec.split(",")(0).toInt))

    val productCategories = productsMap.join(categoriesMap)

    val productCategoriesMap = productCategories
          .map(rec => (rec._2._2, rec._2._1))

    //productCategoriesMap.collect().foreach(println)

    val productDepartments = productCategoriesMap.join(departmentsMap)

    //productDepartments.collect().foreach(println)
    val productDepartmentsMap = productDepartments
                  .map(rec => (rec._2._1, rec._2._2))
                  .distinct()

    //productDepartmentsMap.collect().foreach(println)

    val bv = sc.broadcast(productDepartmentsMap.collectAsMap())


    val orders = sc.textFile(inputPath +"/orders.txt")


    val orderItems = sc.textFile(inputPath + "/order_items.txt" )

    val ordersCompleted = orders
                .filter(rec => (rec.split(",")(3) == "COMPLETE"))
                .map(rec => (rec.split(",")(0).toInt, rec.split(",")(1)))

    val orderItemsMap = orderItems
      .map(rec => (rec.split(",")(1).toInt,
        (bv.value.get(rec.split(",")(2).toInt).get,rec.split(",")(4).toFloat)))


    //orderItemsMap.collect().foreach(println)
    val ordersJoin = ordersCompleted.join(orderItemsMap)

    val revenuePerDayDepartment = ordersJoin
              .map(rec => ((rec._2._1, rec._2._2._1), rec._2._2._2))
              .reduceByKey((acc, value) => acc + value)

    //revenuePerDayDepartment.collect().foreach(println)

    val revenuePerDayDepartmentSortedByDate = revenuePerDayDepartment.sortByKey()
    revenuePerDayDepartmentSortedByDate.collect().foreach(println)

    revenuePerDayDepartmentSortedByDate.saveAsTextFile(outputPath)

  }

}
