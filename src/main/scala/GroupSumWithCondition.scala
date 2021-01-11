/**
 * Created by STYN on 29-12-2020
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object GroupSumWithCondition {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val df = Seq(("a","b","c",-50,"N"),
      ("a","b","c",50,"N"),
      ("a","b","c",0,"Y"),
      ("d","e","f",0,"Y"),
      ("d","e","f",-0,"Y"),
      ("q","r","s",1,"N"),
      ("q","r","s",1,"N"),
      ("x","y","z",5,"N"),
      ("x","y","z",0,"Y")).toDF("c1","c2","c3","c4","c5")
    df.show
    val wind = Window.partitionBy("c1","c2", "c3")
    val df1 = df.withColumn("tmp", array_join(collect_set("c5").over(wind), "|"))
        .filter("tmp <> 'Y'")

    val df1a = df.withColumn("tmp", array_join(collect_set("c5").over(wind), "|"))
      .filter("(tmp = 'N|Y' and c5 = 'N') or (tmp = c5)")

    val df2 = df1.groupBy("c1","c2", "c3", "c5").agg(sum("c4").as("sum"))
    val df2a = df1a.groupBy("c1","c2", "c3", "c5").agg(sum("c4").as("sum"))
    df2a.show
  }
}
