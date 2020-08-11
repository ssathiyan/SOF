/**
 * Created by STYN on 30-07-2020
 * https://stackoverflow.com/questions/63158608/reading-a-txt-file-with-colon-in-spark-2-4
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.functions._
object PivotCase {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val df = Seq("Manager_21: Employee_575,Employee_2703", "Manager_11: Employee_454,Employee_158", "Manager_4: Employee_1545,Employee_1312").toDF
    df.show(false)

    val df1 = df.withColumn("mid",monotonically_increasing_id)
    .withColumn("col1",split(col("value"),":")(0)).
      withColumn("col2",split(split(col("value"),":")(1),","))

      //df1.show(false)

      .groupBy("mid").
      pivot(col("col1")).
      agg(min(col("col2"))).drop("mid")
        .select(max("Manager_11").alias("Manager_11"),max("Manager_21").alias("Manager_21") ,max("Manager_4").alias("Manager_4"))
      .selectExpr("explode(arrays_zip(Manager_11,Manager_21,Manager_4))")
      .select("col.*")

     //df1.select(df1.columns.map(c => max(col(c)).as(c)):_*)
    .show(false)

  }
}
