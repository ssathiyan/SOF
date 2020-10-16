/**
 * Created by STYN on 28-09-2020
 */
object SortandKeepNegative {
  def main(args: Array[String]): Unit = {
    val ls = List(1,-2,-3,9,6,5,-7,8)
    println(ls)
    val positive_ls = ls.filter(_ > 0).sorted
    val res = ls.foldLeft(List[Int](), positive_ls)((a,b) => if (b < 0) (a._1 :+ b, a._2) else (a._1 :+ a._2.head, a._2.tail))
    println(res._1)
  }


}
