import com.styn.utils.SparkConfig._
import java.util.Calendar

import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
/**
 * Created by STYN on 25-06-2020
 * This is to sum values inside a Array[Struct] in a DataFrame
 */
object ArraySumDF {
  case class inner(pC: Int, qty: Int, pid: Long, peid: String)
  case class outer(timestamp: String, cur: String, ordNum: String, items: Array[inner])
  case class result(timestamp: String, cur: String, ordNum: String, items: Array[inner], sales: Int)

  def main(args: Array[String]): Unit = {
    val ipList = Seq(outer(Calendar.getInstance.getTime.toString, "$", "12345", Array(inner(1111, 1, 123456789L,"123"),inner(2222, 1, 123456789L,"123"),inner(3333, 1, 123456789L,"123"),inner(4444, 1, 123456789L,"123"),inner(5555, 1, 123456789L,"123"))))

    import spark.implicits._
    //val enc = Encoders.product[result]
    // The DS way
    val ds = ipList.toDS
    ds.map(r => result(r.timestamp, r.cur, r.ordNum, r.items, r.items.map(s => s.qty * s.pC).sum)).show(false)
    //The DF way
    val df = ipList.toDF
    df.withColumn("ordSum", arraySum($"items")).show(false)
  }

  val arraySum: UserDefinedFunction = udf(arrSum _)
  def arrSum(ar: Seq[Row]): Int = {
    ar.map(sales => sales.getInt(1) * sales.getInt(0)).sum
  }
}
