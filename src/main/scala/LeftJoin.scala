import com.styn.utils.SparkConfig._
import org.apache.spark.sql.functions._
/**
 * Created by STYN on 26-06-2020
 * https://stackoverflow.com/questions/62587596/create-dataset-from-2-other-datasets-spark
 */
object LeftJoin {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val df1 = Seq((1, "10.", "key1", "date1"), (2, "12.", "key2", "date2")).toDF("_c0", "_c1", "_c2", "_c3")
    val df2 = Seq((1, "2.", "key1", "date1"), (2, "3", "key5", "date2")).toDF("_c0", "_c4", "_c2", "_c3")
    df1.show()
    df2.show()

    val result = df1.join(df2, df1("_c2") === df2("_c2") && df1("_c3") === df2("_c3"), "left")
    result.show()
    df1.write.saveAsTable("t1")
    spark.catalog.listTables().show(false)
  }
}
