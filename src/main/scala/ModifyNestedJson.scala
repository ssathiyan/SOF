/**
 * Created by STYN on 08-10-2020
 */
import com.styn.utils.SparkConfig._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object ModifyNestedJson {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val js =
      """
        |{
        |	"id": "0001",
        |	"type": "donut",
        |	"name": "Cake",
        |	"ppu": 0.55,
        |	"batters":
        |		{
        |			"batter":
        |				[
        |					{ "id": "1001", "type": "Regular","created_at":1246 },
        |					{ "id": "1002", "type": "Chocolate","created_at":1246 },
        |					{ "id": "1003", "type": "Blueberry","created_at":1246 },
        |					{ "id": "1004", "type": "Devil's Food","created_at":1246 }
        |				],
        |			"topping":
        |				[
        |					{ "id": "5001", "type": "None","created_at":1246 },
        |					{ "id": "5002", "type": "Glazed","created_at":1246 },
        |					{ "id": "5005", "type": "Sugar","created_at":1246 },
        |					{ "id": "5007", "type": "Powdered Sugar","created_at":1246 },
        |					{ "id": "5006", "type": "Chocolate with Sprinkles","created_at":1246 },
        |					{ "id": "5003", "type": "Chocolate","created_at":1246 },
        |					{ "id": "5004", "type": "Maple","created_at":1246 }
        |				]
        |		}
        |}
        |""".stripMargin
val js1 =
  """
    |{
    |	"id": "0001",
    |	"type": "donut",
    |	"name": "Cake",
    |	"ppu": 0.55,
    |	"created_at":1246,
    |	"batters":
    |		{
    |			"batter":
    |				[
    |					{ "id": "1001", "type": "Regular" },
    |					{ "id": "1002", "type": "Chocolate" },
    |					{ "id": "1003", "type": "Blueberry" },
    |					{ "id": "1004", "type": "Devil's Food" }
    |				],
    |			"topping":
    |				[
    |					{ "id": "5001", "type": "None" },
    |					{ "id": "5002", "type": "Glazed" },
    |					{ "id": "5005", "type": "Sugar" },
    |					{ "id": "5007", "type": "Powdered Sugar" },
    |					{ "id": "5006", "type": "Chocolate with Sprinkles" },
    |					{ "id": "5003", "type": "Chocolate" },
    |					{ "id": "5004", "type": "Maple" }
    |				]
    |		}
    |}
    |""".stripMargin
    val df = spark.read.json(Seq(js1).toDS)
    df.printSchema()
    val wind = Window.partitionBy("id")
    val df1 = df.withColumn("batter", explode($"batters.batter")).withColumn("batter", struct($"batter.*", $"created_at"))
      .withColumn("batter", collect_list($"batter").over(wind).as("batter"))
      .withColumn("topping", explode($"batters.topping")).withColumn("topping", struct($"topping.*", $"created_at"))
      .withColumn("topping", collect_list($"topping").over(wind).as("topping"))
      .withColumn("batters", struct("batter", "topping")).drop("batter","topping").dropDuplicates

    df1.printSchema()
    df1.show(false)
  }
}
