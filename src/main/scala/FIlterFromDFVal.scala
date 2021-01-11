/**
 * Created by STYN on 06-01-2021
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.functions._
object FIlterFromDFVal {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val ausDF = Seq(
      ("australia", "Steve Smith", "batter"),
      ("australia", "David Warner", "batter"),
      ("australia", "Pat Cummins", "bowler")
    ).toDF("country", "player", "speciality")

    val indDF = Seq(
      ("india", "Rohit Sharma", "batsman"),
      ("india", "Virat Kohli", "batsman"),
      ("india", "Jaspreet Bumrah", "bowler")
    ).toDF("country", "player", "speciality")

    val engDF = Seq(
      ("england", "Jos Buttler", "bat"),
      ("england", "Joe Root", "bat"),
      ("england", "James Anderson", "bowl")
    ).toDF("country", "player", "speciality")

    val cricketersDF = ausDF.union(indDF).union(engDF)

    val batsmanFilter = Seq(
      ("australia", "speciality == \"batter\""),
      ("india", "speciality == \"batsman\""),
      ("england", "speciality == \"bat\"")
    ).toDF("country", "filter")

    val batsmanFilterDF = cricketersDF.join(batsmanFilter, "country")

    batsmanFilterDF.show()

    batsmanFilterDF.withColumn("tmp", split($"filter", " == ")(0)).show()
  }


}
