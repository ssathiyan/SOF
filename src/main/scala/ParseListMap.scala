/**
 * Created by STYN on 12-06-2020
 * https://stackoverflow.com/questions/62330665/how-to-parse-a-list-of-map-in-scala
 */
object ParseListMap {
  def main(args: Array[String]): Unit = {
    val tabColMapping = List( Map("table" ->"Emp") -> Map("keyCol" -> "EmpId", "orderingCol" -> "dob"),
      Map("table" -> "Dept") -> Map("keyCol" -> "deptId", "orderingCol" -> "branch"))

    println(getNestedConfig(tabColMapping, "Dept", "keyCol"))
  }

  def getNestedConfig(ip: List[(Map[String, String], Map[String, String])], parentKey: String, childKey: String): Option[String] = {
    val pk = ip.filter(i => i._1.toList.count(i1 => i1._2 == parentKey) > 0)

    if (pk.isEmpty)
      None
    else
      pk.head._2.get(childKey)
  }
}