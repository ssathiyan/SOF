/**
 * Created by STYN on 19-08-2020
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.functions._
object ColumnNameMapMessage {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val rdd = spark.sparkContext.parallelize(Seq(("2","MSG_NONE[0]","MSG_TERMINATION[3]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]"," MSG_NONE[0]"),
      ("4","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]","MSG_TERMINATION[3]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]"," MSG_NONE[0]"),
      ("2","MSG_NONE[0]","MSG_TERMINATION[3]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]"," MSG_NONE[0]"),
      ("7","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]","MSG_NONE[0]","MSG_TERMINATION[3]","MSG_NONE[0]","MSG_NONE[0]"," MSG_NONE[0]")))

    val df1 = spark.createDataFrame(rdd).toDF("text_count","MESSAGE_1_CANNED_MSG_ID","MESSAGE_2_CANNED_MSG_ID","MESSAGE_3_CANNED_MSG_ID","MESSAGE_4_CANNED_MSG_ID","MESSAGE_5_CANNED_MSG_ID","MESSAGE_6_CANNED_MSG_ID","MESSAGE_7_CANNED_MSG_ID","MESSAGE_8_CANNED_MSG_ID","MESSAGE_9_CANNED_MSG_ID","MESSAGE_10_CANNED_MSG_ID")

    df1.show(false)

    val msgcols = df1.columns.filter(_.contains("MESSAGE")).flatMap(c => Seq(lit(c),col(c)))
    val df2 = df1.withColumn("msg_map",map(msgcols:_*))
      .withColumn("msg_col", concat(lit("MESSAGE_"), col("text_count"), lit("_CANNED_MSG_ID")))
      .withColumn("message", $"msg_map"($"msg_col")).drop("msg_map", "msg_col")

    df2.show(false)
  }
}
