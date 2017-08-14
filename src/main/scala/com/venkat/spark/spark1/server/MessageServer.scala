package com.venkat.spark.spark1.server

import java.io._
import java.net.ServerSocket


/**
  * Created by VenkatramR on 7/17/2017.
  */
object MessageServer extends App{

  if(args.length == 1){
    println(s"File is, ${args(0)}")
  }

  val serverSocket = new ServerSocket(9999)
  //val socket = serverSocket.accept()
  //val outputStream = socket.getOutputStream
  var flag = true

  val myFile = new File(args(0))
  if(myFile != null && !myFile.exists()){
    println("File not exists, re run by supplying the file")
    flag = false
  }

  /*val source = fromFile(args(0))
  val content = source.getLines()
  while(true){
    val socket = serverSocket.accept()
    val outputStream = socket.getOutputStream
    val oos = new ObjectOutputStream(outputStream)

    content.foreach(line =>{
      outputStream.write(line.getBytes)
      println(line)
    })
    outputStream.flush()
    println("Done....")

  }
*/

  while(true){
    val socket = serverSocket.accept()
    val bis = new BufferedInputStream(new FileInputStream(args(0)))
    val length = myFile.length().toInt
    val outputStream = socket.getOutputStream
    val oos = new ObjectOutputStream(outputStream)
    val buffer = new Array[Byte](length)
    var bytesRead = 0
    while((bytesRead =  bis.read(buffer, 0, buffer.length)) != -1){
      outputStream.write(buffer, 0, buffer.length)
      outputStream.flush()
    }
    outputStream.flush()
    println("Sending " + args(0) + "  " + buffer.length + " bytes")
    println("Done....")

  }



  /*while(true){
    val bis = new BufferedInputStream(new FileInputStream(args(0)))
    val length = myFile.length().toInt

    val socket = serverSocket.accept()
    val outputStream = socket.getOutputStream
    val oos = new ObjectOutputStream(outputStream)
    val buffer = new Array[Byte](length)
    bis.read(buffer, 0, buffer.length)
    println("Sending " + args(0) + "  " + buffer.length + " bytes")
    outputStream.write(buffer, 0, length)
    outputStream.flush()
    println("Done....")

  }*/
}
