/**
 * Created by STYN on 01-10-2020
 */
import com.styn.utils.SparkConfig._
object DataSavvyReq1 {
  def main(args: Array[String]): Unit = {
    val data = "C:\\Users\\Premium\\Downloads\\us-500.csv"
    val usrdd = spark.sparkContext.textFile(data)
    val head = usrdd.first()
    val reg = "(\"[a-zA-Z0-9 ]+,)([a-zA-Z0-9 ]+\")".r
    val res = usrdd.filter(x=>x!=head).map{x=>
      val m = reg.findFirstIn(x)
      if (m != None)
        x.replaceAll(m.get,m.get.replace(",", ""))
      else
        x
    }
    res.take(10).foreach(println)
  }
}
