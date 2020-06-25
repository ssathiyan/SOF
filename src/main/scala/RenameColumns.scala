import com.styn.utils.SparkConfig._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
/**
 * Created by STYN on 23-06-2020
 * https://stackoverflow.com/questions/62521402/how-to-dynamically-rename-a-column-in-spark-df-based-on-the-case-class
 */
object RenameColumns {
  def main(args: Array[String]): Unit = {
    // Case class mapped to the JSON config
    case class ObjMapping (colName:String,
                           renameCol:Option[String],
                           colOrder:Option[Integer]
                          )
    // JSON config mapped to case class
    val newOb = List(ObjMapping("DEPTID",Some("DEPT_ID"),Some(1)), ObjMapping("EMPID",Some("EMP_ID"),Some(4)), ObjMapping("DEPT_NAME",None,Some(2)), ObjMapping("EMPNAME",Some("EMP_NAME"),Some(3)))

    println(ObjMapping("DEPTID",Some("DEPT_ID"),Some(1)))

    import spark.implicits._

    val empDf = Seq(
      (1,10,"IT","John"),
      (2,20,"DEV","Ed"),
      (2,30,"OPS","Brian")
    ).toDF("DEPTID","EMPID","DEPT_NAME","EMPNAME")

    val selectExpr : Seq[Column] = newOb
      .sortBy(_.colOrder)
      .map(om => col(om.colName).as(om.renameCol.getOrElse(om.colName)))

    empDf
      .select(selectExpr:_*)
      .show()

  }
}
