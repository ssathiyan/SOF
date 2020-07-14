import com.styn.utils.SparkConfig._
/**
 * Created by STYN on 03-07-2020
 */
object PrevRow {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val df = Seq((2020, 4000), (2019, 3000), (2018, 2000)).toDF("y","r")

    df.createOrReplaceTempView("table")
    spark.table("table").show(false)
    val res = spark.sql("select table2.y, table2.r, table1.y as p_y, table1.r as p_r  from table as table1 inner join table as table2 on table1.y = table2.y - 1")
    res.show(false)
  }
}
