package com.venkat.spark.spark1.server

import java.net.ServerSocket
import java.util.Scanner

/**
  * Created by Srijan on 16-07-2017.
  */
object MyServer extends App{

  val serverSocket = new ServerSocket(9999)
  val socket = serverSocket.accept()
  val outputStream = socket.getOutputStream


  while(true){
    println("Enter a text..")
    val scanner = new Scanner(System.in)
    val line = scanner.nextLine() + "\n"
    outputStream.write(line.getBytes)

  }
}
