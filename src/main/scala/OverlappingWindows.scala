/**
 * Created by STYN on 29-07-2020
 * https://stackoverflow.com/questions/63144844/spark-aggregating-a-single-column-based-on-several-overlapping-windows
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object OverlappingWindows {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val df = Seq((0,0),(1,0),(2,1),(3,4),(4,0),(5,0),(6,1),(7,3)).toDF("timestamp","spent")

    val wind = Window.orderBy("timestamp").rangeBetween(-4, Window.currentRow)
    df.withColumn("spend-5d", sum("spent") over wind).show(false)
  }
}
