import com.styn.utils.SparkConfig._
import org.apache.spark.sql.functions._

/**
 * Created by STYN on 30-06-2020
 * https://stackoverflow.com/questions/62651743/cannot-split-an-array-by-a-special-char-in-spark
 */
object ArraySplit {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val df = Seq(("azerty", Array("10", "8", "11", "", "1", "5", "4", "1", "9", "7", "1"))).toDF("runner", "positions")
    df.show(false)

    val df1 = df.withColumn("pos", split(array_join($"positions", "#"), "#1#|#1$"))

    df1.show(false)
  }
}
