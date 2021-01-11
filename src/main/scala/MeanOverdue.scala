/**
 * Created by STYN on 22-12-2020
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object MeanOverdue {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val jsonDF = spark.read.option("mode","DROPMALFORMED").json("D:\\SWs\\spark\\DataTest\\aaron_mean\\data.json")
    jsonDF.show

    val csvDF = spark.read.option("header", true).option("inferSchema", true).csv("D:\\SWs\\spark\\DataTest\\aaron_mean\\data.csv")
    csvDF.show

    val joinDF = jsonDF.join(csvDF, "id")
      .withColumn("day", date_format(from_unixtime($"created"/1000), "E"))

    val joinDF1 = joinDF.groupBy("day").agg(mean("overdue").as("mean_overdue"))
    joinDF1.show()

    val res1 = joinDF1.select($"day", $"mean_overdue",min("mean_overdue").over(Window.orderBy("day")).as("minOver")).filter($"minOver" === $"mean_overdue").select("day")
    //res1.show()

    val custDF = spark.read.option("header", true).option("inferSchema", true).csv("D:\\SWs\\spark\\DataTest\\aaron_mean\\data1.csv")
    val resultDF = custDF.groupBy("CITY").agg(count("ID").as("COUNT")).orderBy($"CITY".asc)
    resultDF.show
  }
}
