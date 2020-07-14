/**
 * Created on 08-07-2020
 * https://stackoverflow.com/questions/62708493/quantity-redistribution-logic-mapgroups-with-external-dataset
 */

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object DistributionQuantity {

  val spark = SparkSession.builder()
    .appName("Sample app")
    .master("local")
    .config("spark.sql.shuffle.partitions", "200") //Change to a more reasonable default number of partitions for our data
    .getOrCreate()

  val sc = spark.sparkContext

  final case class Owner(car: String, pcode: String, o_qtty: Double)

  final case class Invoice(car: String, pcode: String, i_qtty: Double)

  def main(args: Array[String]): Unit = {

    val data = Seq(
      Owner("A", "666", 80),
      Owner("B", "555", 20),
      Owner("A", "444", 50),
      Owner("A", "222", 20),
      Owner("C", "444", 20),
      Owner("C", "666", 80),
      Owner("C", "555", 120),
      Owner("A", "888", 100)
    )

    val fleet = Seq(
      Invoice("A", "666", 15),
      Invoice("C", "666", 10),
      Invoice("A", "888", 12),
      Invoice("B", "555", 200)
    )

    val expected = Seq(
      Owner("A", "666", 65),
      Owner("B", "555", 20), // not redistributed because produce a negative value
      Owner("A", "444", 69.29),
      Owner("A", "222", 27.71),
      Owner("C", "444", 21.43),
      Owner("C", "666", 70),
      Owner("C", "555", 128.57),
      Owner("A", "888", 88)
    )

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      import spark.implicits._

      val owners = spark.createDataset(data)
        .as[Owner]
        .cache()

      val invoices = spark.createDataset(fleet)
        .as[Invoice]
        .cache()

      val p1 = owners
        .join(invoices, Seq("car", "pcode"), "inner")
        .selectExpr("car", "pcode", "IF(o_qtty-i_qtty < 0,o_qtty,o_qtty - i_qtty) AS qtty", "IF(o_qtty-i_qtty < 0,0,i_qtty) AS to_distribute")
        .persist(StorageLevel.MEMORY_ONLY)

      p1.show()

      val p2 = owners
        .join(invoices, Seq("car", "pcode"), "left_outer")
        .filter(row => row.anyNull == true)
        .drop(col("i_qtty"))
        .withColumnRenamed("o_qtty", "qtty")
        .persist(StorageLevel.MEMORY_ONLY)

      p2.show

      val distribute = p1
        .groupBy(col("car"))
        .agg(sum(col("to_distribute")).as("to_distribute"))
        .persist(StorageLevel.MEMORY_ONLY)

      distribute.show()

      val proportion = p2
        .groupBy(col("car"))
        .agg(sum(col("qtty")).as("proportion"))
        .persist(StorageLevel.MEMORY_ONLY)

      proportion.show()

      val result = p2
        .join(distribute, "car")
        .join(proportion, "car")
        .withColumn("qtty", round(((col("to_distribute") / col("proportion")) * col("qtty")) + col("qtty"), 2))
        .drop("to_distribute", "proportion")
        .union(p1.drop("to_distribute"))

      result.show()
      /*
      +---+-----+------+
      |car|pcode|  qtty|
      +---+-----+------+
      |  A|  444| 69.29|
      |  A|  222| 27.71|
      |  C|  444| 21.43|
      |  C|  555|128.57|
      |  A|  666|  65.0|
      |  B|  555|  20.0|
      |  C|  666|  70.0|
      |  A|  888|  88.0|
      +---+-----+------+
      */

      expected
        .toDF("car", "pcode", "qtty")
        .show(truncate = false)
      /*
      +---+-----+------+
      |car|pcode|qtty  |
      +---+-----+------+
      |A  |666  |65.0  |
      |B  |555  |20.0  |
      |A  |444  |69.29 |
      |A  |222  |27.71 |
      |C  |444  |21.43 |
      |C  |666  |70.0  |
      |C  |555  |128.57|
      |A  |888  |88.0  |
      +---+-----+------+
      */

      //SQL version

      owners.createOrReplaceTempView("owners")
      invoices.createOrReplaceTempView("invoices")

      /**
       * this part fetch car and pcode from owner with the substracted quantity from invoice
       */
      val p1SQL = spark.sql(
        """SELECT i.car,i.pcode,
          |CASE WHEN (o.qtty - i.qtty) < 0 THEN o.qtty ELSE (o.qtty - i.qtty) END AS qtty,
          |CASE WHEN (o.qtty - i.qtty) < 0 THEN 0 ELSE i.qtty END AS to_distribute
          |FROM owners o
          |INNER JOIN invoices i  ON(i.car = o.car AND i.pcode = o.pcode)
          |""".stripMargin)
        .cache()
      p1SQL.createOrReplaceTempView("p1")

      /**
       * this part fetch all the car and pcode that we have to redistribute their quantity
       */
      val p2SQL = spark.sql(
        """SELECT o.car, o.pcode, o.qtty
          |FROM owners o
          |LEFT OUTER JOIN invoices i  ON(i.car = o.car AND i.pcode = o.pcode)
          |WHERE i.car IS NULL
          |""".stripMargin)
        .cache()
      p2SQL.createOrReplaceTempView("p2")

      /**
       * this part fetch the quantity to distribute
       */
      val distributeSQL = spark.sql(
        """
          |SELECT car, SUM(to_distribute) AS to_distribute
          |FROM p1
          |GROUP BY car
          |""".stripMargin)
        .cache()
      distributeSQL.createOrReplaceTempView("distribute")

      /**
       * this part fetch the proportion to distribute proportionally
       */
      val proportionSQL = spark.sql(
        """
          |SELECT car, SUM(qtty) AS proportion
          |FROM p2
          |GROUP BY car
          |""".stripMargin)
        .cache()
      proportionSQL.createOrReplaceTempView("proportion")


      /**
       * this part join p1 and p2 with the distribution calculated
       */
      val resultSQL = spark.sql(
        """
          |SELECT p2.car, p2.pcode, ROUND(((to_distribute / proportion) * qtty) + qtty, 2) AS qtty
          |FROM p2
          |JOIN distribute d ON(p2.car = d.car)
          |JOIN proportion p ON(d.car = p.car)
          |UNION ALL
          |SELECT car, pcode, qtty
          |FROM p1
          |""".stripMargin)

      resultSQL.show(truncate = false)
      /*
      +---+-----+------+
      |car|pcode|qtty  |
      +---+-----+------+
      |A  |444  |69.29 |
      |A  |222  |27.71 |
      |C  |444  |21.43 |
      |C  |555  |128.57|
      |A  |666  |65.0  |
      |B  |555  |20.0  |
      |C  |666  |70.0  |
      |A  |888  |88.0  |
      +---+-----+------+
      */

      expected
        .toDF("car", "pcode", "qtty")
        .show(truncate = false)
      /*
      +---+-----+------+
      |car|pcode|qtty  |
      +---+-----+------+
      |A  |666  |65.0  |
      |B  |555  |20.0  |
      |A  |444  |69.29 |
      |A  |222  |27.71 |
      |C  |444  |21.43 |
      |C  |666  |70.0  |
      |C  |555  |128.57|
      |A  |888  |88.0  |
      +---+-----+------+
      */

    } finally {
      sc.stop()
      println("SparkContext stopped")
      spark.stop()
      println("SparkSession stopped")
    }
  }
}

