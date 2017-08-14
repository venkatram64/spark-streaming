package com.venkat.spark.spark1.reciever

import java.io.{FileNotFoundException, IOException}

import scala.io.Source.fromFile
import scala.util.{Failure, Random, Success, Try}

/**
  * Created by venkatram.veerareddy on 8/14/2017.
  */


class Names {

  private type NameData = (String,String,Int,String,String)

  private def parseLine(line:String):NameData = {
    val parts = line.split(",")
    (parts(0), parts(1),parts(2).toInt,parts(3),genPhoneNumber)
  }

  def readFile(fileName:String): Try[Array[NameData]] = {
    Try{
      val source = fromFile(fileName)
      val content = source.getLines().toArray
      val nameData = content.map(parseLine)
      nameData
    }
  }

  private def genPhoneNumber: String = {
    val NUMBERS = Array[Int](1,2,3,4,5,6,7,8,9,0)
    val sb:StringBuilder = new StringBuilder
    for(i <- 1 to 10){
      sb.append(NUMBERS((getRandomNumber(10))))
      if(i % 3 == 0 && i % 9 != 0) sb.append("-")
    }
    sb.toString()
  }

  private def getRandomNumber(length: Int) = {
    var randomInt = 0
    val randomGenerator = new Random()
    randomInt = randomGenerator.nextInt(length)
    if (randomInt - 1 == -1) randomInt
    else randomInt - 1
  }
}

object Names extends App{

  var names = new Names
  val nameData = names.readFile("NY.TXT")
  val list = nameData.get(0)

  println(list._1 +", " + list._2)
  /*nameData match {
    case Success(lines) => {
      lines.foreach(println)
    }
    case Failure(f) => f match {
      case ex: FileNotFoundException => {
        println(s"Couldn't find the file ${ex.getMessage()}")
      }
      case ex: IOException => {
        println(s"Read failed. ${ex.getMessage()}")
      }
    }
  }*/

}
