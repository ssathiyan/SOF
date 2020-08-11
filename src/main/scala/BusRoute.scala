/**
 * Created by STYN on 15-07-2020
 * 1) Given a hive table containing bus route details, write a hive query to find all possible route between two station and time between them.
 *
 * Input :
 *
 * Busid   Station Arrival_Time
 * 1       st1     4:20
 * 1       st2     5:30
 * 1       st3     7:30
 *
 * Output:
 *
 * Busid  Route   Duration
 * 1      st1-st2  70
 * 1      st2-st3  120
 * 1      st1-st3  190
 */

import com.styn.utils.SparkConfig._
import org.apache.spark.sql.functions._

object BusRoute {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val df = Seq(("1", "st1", "4:20"), ("1", "st2", "5:30"), ("1", "st3", "7:30"))
      .toDF("Bid", "stn", "ArrTime")

    df.show()
    df.createOrReplaceTempView("table")
    val df1 = df.as("t1").join(df.as("t2"), $"t1.Bid" === $"t2.Bid" && $"t1.stn" =!= $"t2.stn")
      .where($"t1.ArrTime" < $"t2.ArrTime")
      .withColumn("duration", (unix_timestamp($"t2.ArrTime", "hh:mm") - unix_timestamp($"t1.ArrTime", "hh:mm")) / 60)
      .select($"t1.bid", concat_ws("-", $"t1.stn", $"t2.stn").as("Route"), $"duration")
    df1.show(false)

    val query = "select t1.bid, concat_ws('-', t1.stn, t2.stn) as route, (unix_timestamp(t2.arrtime, 'hh:mm') - unix_timestamp(t1.arrtime, 'hh:mm'))/60 as duration from table t1 join table t2 " +
      "on t1.bid = t2.bid and t1.stn <> t2.stn where t1.arrtime < t2.arrtime"

    spark.sql(query).show(false)

  }
}
