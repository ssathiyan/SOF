import scala.collection.mutable

/**
 * Created by STYN on 14-07-2020
 */
object Possibletime {
  def main(args: Array[String]): Unit = {
    println(solution(6,2,4,7))
  }

  def solution(a: Int, b: Int, c: Int, d: Int): Int = {
    val comb = combinations(s"$a$b$c$d")
    comb.filter(s => s.take(2).toInt <=24 && s.drop(2).toInt <= 59).size
  }

  def combinations(str: String): mutable.HashSet[String] = {
      val resSet = new mutable.HashSet[String]()
      def inner(ip:String, res: String): Unit = {
        if(ip.length == 0)
          resSet.add(res)
        else {
          for(c <- ip){
            inner(ip.diff(c.toString), res + c.toString)
          }
        }
      }
      inner(str, "")
      resSet
    }
}
