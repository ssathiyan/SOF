import com.styn.utils.SparkConfig._
import org.apache.spark.sql.functions._
/**
 * Created by STYN on 25-06-2020
 * https://stackoverflow.com/questions/62575379/how-to-replace-spark-dataframe-column-values-using-a-hashmap
 */
object ReplaceColValDF {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val myMap = Map("Simpson" -> "Surname")
    val df = Seq(("Homer","Simpson","BeerDrinker"),("Marge","Simpson","Housewife"),("Bart","Simpson","Son"),
      ("Lisa","Simpson","Daughter"),("TheSimpsons","Simpsons","Family")).toDF("Noun","Pronoun","Adjective")
    df.show(false)

    //Approach-1: Adding map as an additional column to the DF and get values from the map
    val df1 = df.withColumn("map", typedLit(myMap))
    val df2 = df1.withColumn("Pronoun", when($"map"($"Pronoun").isNotNull, $"map"($"Pronoun")).otherwise($"Pronoun") ).drop("map")
    df2.show(false)

    //Approach-2: Declaring the typedLit(map):Column instead of adding column
    val colMap = typedLit(myMap)
    val df3 = df.withColumn("Pronoun", when(colMap($"Pronoun").isNotNull, colMap($"Pronoun")).otherwise($"Pronoun") )
    df3.show(false)

    ////Approach-3: UDF way
    val getVal = udf((x: String) => myMap.getOrElse(x, x))
    val resDF = df.withColumn("Pronoun", getVal($"Pronoun"))
    resDF.show(false)

  }
}
