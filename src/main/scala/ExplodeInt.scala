import com.styn.utils.SparkConfig._
import org.apache.spark.sql.functions._
/**
 * Created by STYN on 14-09-2020
 */
object ExplodeInt {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val df = Seq(("A",2,1),("B",0,2),("C",2,0)).toDF("class", "male", "female")
    df.show()
    val df1 = df.select($"class",
      explode(concat(typedLit(array_repeat(lit("M"), $"male")), typedLit(array_repeat(lit("F"), $"female")))).as("gender"))
    df1.show(false)
  }
}
