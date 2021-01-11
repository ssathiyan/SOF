/**
 * Created by STYN on 09-11-2020
 * https://stackoverflow.com/questions/64755497/how-do-i-groupby-and-divide-each-value-of-the-group-by-the-number-of-rows-in-tha
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object GroupByZeroCount {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val df = Seq(("Steve",1),("Steve",0),("Steve",0),("Michael",3),("Michael",2),("Michael",0),("Katherine",4),("Katherine",0),("Devin",0)).toDF("name","score")
    df.show

    val df1 = df.withColumn("zero", when($"score" > 0, 0).otherwise(1))
      .groupBy("name")
      .agg((sum("zero") / count("name")).as("zero_avg") )
    df1.show()
  }
}
