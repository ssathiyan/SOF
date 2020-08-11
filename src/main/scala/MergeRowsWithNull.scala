/**
 * Created by STYN on 16-07-2020
 * https://stackoverflow.com/questions/62928059/how-to-merge-two-rows-in-spark-dataframe-to-get-null-values-in-output
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.functions._
object MergeRowsWithNull {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val df = Seq(("1","abc",null,"pqr","man","2020-03-1200:00:0"),("1","abc","def", null,"man","2020-03-1200:00:0"))
      .toDF("id","name","senior1","senior2","senior3","date")

    df.show(false)

    val cols = df.columns.diff(List("id")).toList

    //val res = df.groupBy("id").agg( first(cols.head, ignoreNulls = true).as(cols.head),cols.tail.map(c => first(c, ignoreNulls = true).as(c)):_*)

    //Null as string
    val res1 = df.groupBy("id").agg( when(size(array_intersect(array(lit("null")), collect_list(col(cols.head)))) > 0, null).otherwise(first(cols.head)).as(cols.head),
      cols.tail.map(c => when(size(array_intersect(array(lit("null")),collect_list(col(c)))) > 0, null).otherwise(first(col(c))).as(c)):_*)

    //Null as object
    val res = df.groupBy("id").agg( when(size(array_intersect(array(lit(null)), flatten(collect_list(array(col(cols.head)))))) > 0, null).otherwise(first(cols.head)).as(cols.head),
      cols.tail.map(c => when(size(array_intersect(array(lit(null)),flatten(collect_list(array(col(c)))))) > 0, null).otherwise(first(col(c))).as(c)):_*)


    res.show(false)


  }
}
