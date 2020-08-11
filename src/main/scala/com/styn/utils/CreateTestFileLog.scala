package com.styn.utils

import java.io.{File, PrintWriter}

/**
 * Created by STYN on 05-08-2020
 */
object CreateTestFileLog {
  val out_path = "D:\\SWs\\spark\\DataTest\\movieLens\\testLogData"
  def main(args: Array[String]): Unit = {
    val lines = List("ERROR")//, "WARN", "INFO", "DEBUG", "FATAL")
    createFile(s"${out_path}\\testLog", lines, 37800000)
  }

  def createFile(name: String, lines: List[String], sets: Int) = {
    val writer = new PrintWriter(new File(name))
    try {
      (1 to sets).foreach(x => lines.foreach(s => writer.write(s + "\n")))
    } catch {
      case e: Exception => e.printStackTrace()
    }finally {
      writer.close()
    }
  }

}
