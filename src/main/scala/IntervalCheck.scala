/**
 * Created by STYN on 17-12-2020
 * Given a table containing 4 columns user_id, API name, session_id and timestamp count numer of calles per user_id and session_id(condition: one call should be counted for each user per session within 3 sec interval)
 * If diff between two interval is > 3 sec pre value should be used for next operation else current column value should be used
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object IntervalCheck {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val df = Seq((1,1,"a1", 1),(1,1,"a1", 3000),(1,1,"a1", 6001),(1,1,"a1", 9004),(1,1,"a1", 12001),(1,1,"a1", 12005))
      .toDF("uid", "sid", "api", "timestamp")

    df.show()

    val wind = Window.partitionBy("uid", "sid").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val df1 = df.withColumn("res",when((lit(-1) * sum("timestamp").over(wind)
      + (lit(2) * first("timestamp").over())) * lit(-1) > 3000, last($"timestamp").over(wind)).otherwise($"timestamp"))

    val df2 = df.withColumn("t", when(first("timestamp").over(wind).isNull , last("timestamp").over(wind)).otherwise(first("timestamp").over(wind)))
    //val df2 = df.withColumn("t",first("timestamp").over(wind))
    //df1.show()

    val df3 = df.withColumn("res", when(last("timestamp").over(wind)
      - first("timestamp").over(wind) > 3000,
      last("timestamp").over(wind))
      .otherwise(first("timestamp").over(wind)))

    val df4 = df.withColumn("rt", max("timestamp").over(wind))

    val wind1 = Window.partitionBy("uid", "sid").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val findDiff = udf(udf1 _)
    val df5 = df.withColumn("res", findDiff(collect_list("timestamp").over(wind1)))
    df5.show
  }

  def udf1(sq: Seq[Int]): Int = {
    sq.tail.foldLeft(sq.head)((a:Int,b:Int) => if ((b - a) > 3000) b else a)
  }
}
