/**
 * Created by STYN on 02-08-2020
 */

import com.styn.utils.SparkConfig._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.Try

object ColChain {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val df = Seq(("t1", "t2"), ("t9", "t3"), ("t3", "t4"), ("t4", "t5"), ("t5", "t6")).toDF("c1", "c2")
    val df1 = df.select($"c1".as("c2"), $"c2".as("c1"))
    val df2 = df.join(df1, Seq("c1")).select(df1("c2").as("c1"), df("c1").as("c2"), monotonically_increasing_id().as("rid"))
    val df3 = df2.where($"c2" === "t4")
    val df4 = df2.join(df3.select($"rid".as("rid1")), $"rid" <= $"rid1").select(df1.columns.map(col(_)):_*)
    df4.show()

    colChain(df, "t4").show
  }

  def colChain(df: DataFrame, startValue: String): DataFrame = {
    def inner(df1: DataFrame, value: String, c: String): DataFrame = {
      val c11 = df.where(col("c") === value)
      if (c11.count == 0)
        df1
      else
        inner(c11.union(df1), c11.take(1)(0).getString(0), if (c == "c2") "c1" else "c2")
    }

    val start = df.filter(col("c2") === startValue)
    val c1 = Try(start.take(1)(0).getString(0)).toOption
    if (c1 == None)
      start
    else
      inner(start, c1.get, "c2")
  }
}
