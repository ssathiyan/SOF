/**
 * Created by STYN on 16-07-2020
 * https://stackoverflow.com/questions/62937586/how-to-write-a-generic-function-to-evaluate-column-values-inside-withcolumn-of-s
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object GenericReplace {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val df  = Seq("RFRA","BRES","EAST", "RUSS").toDF("countries")

    val replaceMap = typedLit(Map("RFRA" -> "FRA",
      "BRES" -> "BRA",
      "RESP" -> "ESP",
      "RBEL" -> "BEL",
      "RGRB" -> "GBR",
      "RALL" -> "DEU",
      "MARO" -> "MAR",
      "RPOR" -> "PRT"))

    def replace(countries: Column): Column = {
      when(replaceMap($"$countries").isNotNull,replaceMap($"$countries"))
        .otherwise(lit("unknown"))
    }

    val res = df.withColumn("modified_countries", replace($"countries"))
    res.show(false)


  }
}
