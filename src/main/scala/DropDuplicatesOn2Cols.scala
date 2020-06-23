import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
 * Created by STYN on 23-06-2020
 * Dropduplicates based on two columns in different order
 */
object DropDuplicatesOn2Cols {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").config("spark.driver.host","localhost").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    println("**********The DF way**********")
    val df = Seq(("ban","hyd", 500),("del","ban",2000),("hyd","ban",500),("ban","del",2000)).toDF("c1","c2","d")
    val df1 = df.withColumn("key", when($"c1" <= $"c2", concat($"c1",$"c2")).otherwise(concat($"c2",$"c1")))
    df1.show(false)
    val res = df1.dropDuplicates("key").drop("key")
    res.show(false)

    println("**********The SQL way**********")
    df.createOrReplaceTempView("table")
    val query = "select * from table where c1 in (select distinct min(c1) as c1 from (select *, case when(c1 < c2) then concat(c1, c2) else concat(c2, c1) end as condition from table) group by condition)"
    val qDF1 = spark.sql(query)
    qDF1.show(false)
  }
}
