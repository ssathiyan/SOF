import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.json.JSONObject

import scala.util.parsing.json.JSONObject
/**
 * Created by STYN on 22-06-2020
 * https://stackoverflow.com/questions/62461634/udf-with-multiple-argument-failes-when-called-in-dataframel
 */
object JsonObj {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = Seq(("{\"a\":1, \"b\":2}", "str1", "str2")).toDF("A","B","C")
    df.show(false)
    for(field <- df.columns){
      df.withColumn(field, parseJsonUdf(col("A"), lit(field),lit(field))).show()
    }

  }

  private def parseJsonUdf: UserDefinedFunction = udf(parseJson _)
  def parseJson (json: String, arg1: String, arg2: String): String = {
    new org.json.JSONObject(json).get("a").toString
  }
}
