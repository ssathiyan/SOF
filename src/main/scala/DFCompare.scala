/**
 * Created by STYN on 28-10-2020
 * https://stackoverflow.com/questions/64576139/how-to-filter-a-dataframe-based-on-multiple-column-matches-in-other-dataframes-i
 */
import com.styn.utils.SparkConfig._
object DFCompare {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val df1 = Seq(("Run","Run"),("Swim","Swim"),("Fish","Fish")).toDF("sport1","sport2")
    val df2 = Seq(("Run","Swim"),("Run","Fish"),("Swim","Fish")).toDF("sport1","sport2")
    val df3 = Seq(("Bike","Bike"),("Bike","Fish"),("Run","Run"),("Fish","Fish"),("Swim","Fish")).toDF("sport1","sport2")

    val res = df3.intersect(df1).union(df3.intersect(df2))
    df1.show
    df2.show
    df3.show
    res.show
  }
}
