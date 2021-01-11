/**
 * Created by STYN on 10-11-2020
 * https://stackoverflow.com/questions/64756074/check-start-middle-and-end-of-groups-in-spark
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object MinMaxZeroGroup {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val df = Seq((1,2,1,"fileA"),
      (2,2,1,"fileA"),
      (3,2,1,"fileA"),
      (4,2,0,"fileA"),
      (5,2,0,"fileA"),
      (6,2,1,"fileA"),
      (11,2,1,"fileB"),
      (12,2,1,"fileB"),
      (13,2,0,"fileB"),
      (14,2,0,"fileB"),
      (15,2,1,"fileB"),
      (16,2,1,"fileB"),
      (21,4,1,"fileB"),
      (22,4,1,"fileB"),
      (23,4,1,"fileB"),
      (24,4,1,"fileB"),
      (25,4,1,"fileB"),
      (26,4,0,"fileB"),
      (31,1,0,"fileC"),
      (32,1,0,"fileC"),
      (33,1,0,"fileC"),
      (34,1,0,"fileC"),
      (35,1,0,"fileC"),
      (36,1,0,"fileC")).toDF("id","Phase","Switch","InputFileName")

    df.show

    val wind = Window.partitionBy("InputFileName", "Phase").orderBy("id")
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    val df1 = df.withColumn("Valid",
      when(first("Switch").over(wind) === 1
        && last("Switch").over(wind) === 1
        && min("Switch").over(wind) === 0, true)
        .otherwise(false))
    df1.orderBy("id").show() //Ordering for display purpose
  }
}
