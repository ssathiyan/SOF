/**
 * Created by STYN on 09-12-2020
 * Find min 3 consecutive record where people strength is greater than 100
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
object ThreeConsecutive {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val df = Seq((1, "2020-09-01", 101),
      (2, "2020-09-02", 102),
      (3, "2020-09-03", 97),
      (4, "2020-09-04", 153),
      (5, "2020-09-05", 164),
      (6, "2020-09-06", 187),
      (7, "2020-09-07", 186),
      (8, "2020-09-08", 197),
      (9, "2020-09-09", 54),
      (10, "2020-09-10", 105)
    ).toDF("id", "date", "people")

    df.show()
    df.createOrReplaceTempView("temp")
    //spark.sql("select date, date_sub(date, row_number() over (order by date)) from temp").show
    df.printSchema()
    val wind = Window.orderBy("date")
    val df1 = df.filter($"people" > 100).withColumn("temp", when(lag("date",1)
      .over(wind) === date_sub($"date", 1),1).otherwise(0))
        .withColumn("temp1", sum("temp").over(wind))
        .withColumn("comm", dayofmonth($"date") - $"temp1")
        .withColumn("count", count("date").over(Window.partitionBy("comm")))
        .filter($"count" >= 3).drop("temp", "temp1", "comm", "count")
    df1.show()
  }
}
