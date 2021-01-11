/**
 * Created by STYN on 21-10-2020
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object ConsecutiveRowGrouping {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val df = Seq(("2020-10-02 10:00:00",211.39,"Max"),
      ("2020-10-02 10:10:00",210.94,"Min"),
      ("2020-10-02 10:30:00",209.21,"Max"),
      ("2020-10-02 11:20:00",207.22,"Min"),
      ("2020-10-02 11:50:00",207.48,"Min"),
      ("2020-10-02 12:10:00",207.58,"Max"),
      ("2020-10-02 12:40:00",207.45,"Min"),
      ("2020-10-02 13:10:00",207.45,"Min"),
      ("2020-10-02 13:40:00",208.7,"Max"),
      ("2020-10-02 14:10:00",208.31,"Max"),
      ("2020-10-02 14:20:00",208.16,"Min"),
      ("2020-10-02 14:30:00",208.3,"Max"),
      ("2020-10-02 14:50:00",208.25,"Min"),
      ("2020-10-02 15:10:00",208.7,"Max"),
      ("2020-10-02 15:30:00",208.08,"Min"),
      ("2020-10-02 16:00:00",208.0,"Min"),
      ("2020-10-02 16:30:00",208.35,"Max"),
      ("2020-10-02 16:40:00",208.26,"Min"),
      ("2020-10-02 16:50:00",208.27,"Max"),
      ("2020-10-02 17:30:00",208.06,"Min")).toDF("Date","Val","Condition")
    df.printSchema()
    df.show(false)
    val wind = Window.orderBy("Date")
    val df1 = df.withColumn("val1", when($"Condition" === lead($"Condition", 1).over(wind),
      when($"Condition" === "Min", min($"val").over(wind.rowsBetween(0,1))).otherwise(max($"val").over(wind.rowsBetween(0,1))))
        .when($"Condition" === lag($"Condition", 1).over(wind),
          when($"Condition" === "Min", min($"val").over(wind.rowsBetween(-1,0))).otherwise(max($"val").over(wind.rowsBetween(-1,0))))
      .otherwise($"val"))

    val df2 = df1.withColumn("rn", when($"Condition" === lead($"Condition", 1).over(wind),1)
      .when($"Condition" === lag($"Condition", 1).over(wind), 2)
      .otherwise(1)).withColumn("Val", $"val1").filter($"rn" === 1).drop("rn", "val1")

    df2.show(false)
  }
}
