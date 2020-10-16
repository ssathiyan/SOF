/**
 * Created by STYN on 01-10-2020
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object WindFlag {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val df = Seq((1,"2020-01-01",0),(1,"2020-02-01",0),(1,"2020-03-01",1),(1,"2020-04-01",0),(1,"2020-05-01",0),(1,"2020-06-01",1),(1,"2020-07-01",0)).toDF("id","date","flag")
    df.createOrReplaceTempView("t1")
    //spark.sql("select *,sum(flag) over (partition by id order by date),if(sum(flag) over (partition by id order by date) >= 1, 1,0) as flag_out from t1").show(false)
    val wind = Window.partitionBy("id").orderBy("date")//.rangeBetween(Window.unboundedPreceding, Window.currentRow)
    //val wind1 = Window.partitionBy("id").rangeBetween(Window.unboundedPreceding, Window.currentRow)
    val df1 = df.withColumn("flag_out_rb", sum("flag").over(wind))
      //.withColumn("flag_out_ord", max("flag").over(wind1))
    df1.show()
  }
}
