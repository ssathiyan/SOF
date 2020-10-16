/**
 * Created by STYN on 04-10-2020
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object BatchIdInc {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val df = Seq((1,"BATCH"),(2,"APPLE"),(3,"CHICKU"),(4,"ORANGES"),(5,"BATCH"),(6,"GRAPES"),(7,"PINEAPPLE"),(8,"MANGO"),(9,"BATCH"),(10,"BANANA")).toDF("sr_no","entries")
    df.show(false)
    val wind = Window.orderBy("sr_no")
    val df1 = df.withColumn("batch_id", sum(when($"entries" ==="BATCH", 1).otherwise(0)).over(wind))
    df1.show(false)
  }
}
