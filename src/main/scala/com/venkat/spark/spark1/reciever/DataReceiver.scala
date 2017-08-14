package com.venkat.spark.spark1.reciever

import java.io.{FileNotFoundException, IOException}


import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.util.{Failure, Success}

/**
  * Created by venkatram.veerareddy on 8/14/2017.
  */
class DataReceiver extends Receiver[Person](StorageLevel.MEMORY_AND_DISK_SER_2){

  override def onStart: Unit = {
    new Thread("Percons Process"){
      override def run(): Unit ={
        recievePerson
      }
    }.start()
  }
  override def onStop: Unit = {

  }

  private def recievePerson : Unit = {
    val names = new Names();
    val namesData = names.readFile("NY.TXT")

    while(!isStopped() && namesData != null){
      namesData match{
        case Success(lines)=>{
          lines.foreach(nd =>{
            store(Person(nd._1,nd._2, nd._3, nd._4, nd._5))
          })
        }
        case Failure(f) => f match{
          case ex: FileNotFoundException => {
            println(s"Couldn't find the file ${ex.getMessage()}")
          }
          case ex: IOException => {
            println(s"Read failed. ${ex.getMessage()}")
          }
        }
      }

    }
  }


}
