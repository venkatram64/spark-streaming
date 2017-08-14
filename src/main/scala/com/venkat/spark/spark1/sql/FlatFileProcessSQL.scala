package com.venkat.spark.spark1.sql

/**
  * Created by Venkatram on 7/21/2017.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.rdd._
import org.apache.spark.util.IntParam


/**
  * Created by VenkatramR on 7/18/2017.
  */
object FlatFileProcessSQL extends App{

  val conf = new SparkConf().setMaster("local[*]").setAppName("LogSQL").set("spark.sql.warehouse.dir", "file:///C:/tmp")

  val sc = new StreamingContext(conf, Seconds(5))

  val lines = sc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

  /*val requests = lines.map(_.split(",")).map(row =>{
    Record(row(0), row(1), row(3))
  })
  requests.print
  */

  val requests = lines.map(_.split(",")).map(row =>{
    (row(0), row(1), row(3))
  })


  requests.foreachRDD((rdd:RDD[(String,String,String)], time:Time) =>{

    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    import sqlContext.implicits._
    val requestDataFrame = rdd.map(w => Record(w._1, w._2, w._3)).toDF()

    requestDataFrame.createOrReplaceTempView("requests")

    //val xxDataFrame = sqlContext.sql("select name, count(*) from requests group by name")
    val xxDataFrame = sqlContext.sql("select * from requests")
    println(s"=========$time==========")
    xxDataFrame.show()
  })


  sc.checkpoint("D:/freelance-work/spark-CheckPoint/")
  sc.start()
  sc.awaitTermination()
}

case class Record(state:String, gender:String,name:String)

object SQLContextSingleton {

  @transient
  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext ={
    if(instance == null){
      instance = new org.apache.spark.sql.SQLContext(sparkContext)
    }
    instance
  }

}
