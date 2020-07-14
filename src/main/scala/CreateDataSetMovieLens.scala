import java.io.File
import java.io.PrintWriter

import scala.io.Source

/**
 * Created by STYN on 27-06-2020
 * This is to create dataset of desired size from existing dataset - to be precise 1MB file will be copied many times
 * to achieve desired size
 */
object CreateDataSetMovieLens {
  def main(args: Array[String]): Unit = {
    val ip_file = "D:\\SWs\\spark\\DataTest\\movieLens\\ml-100k\\u.data"
    val out_path = "D:\\SWs\\spark\\DataTest\\movieLens\\testData"

    val lines = Source.fromFile(ip_file).getLines().toList

    println("**********Started creating files**********")

    val isDir = new File(out_path)
    if (!isDir.exists()) {
      isDir.mkdir()
    }else{
      println("**********Output directory exists and it will be overwritten**********")
    }


    createNfiles(s"${out_path}\\ip", lines, 10000)
    println("**********Ended creating files**********")

  }

  def createNfiles(name: String, lines: List[String], count: Int) = {
    (1 to count).foreach{n =>
      val filename = s"${name}_$n.txt"
      createFile(filename, lines)
      println(s"created file $filename")
    }
  }

  def createFile(name: String, lines: List[String]) = {
    val writer = new PrintWriter(new File(name))
    try {
      lines.foreach(writer.write(_))
    } catch {
      case e: Exception => e.printStackTrace()
    }finally {
      writer.close()
    }
  }
}
