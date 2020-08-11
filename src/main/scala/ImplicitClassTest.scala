/**
 * Created by STYN on 22-07-2020
 */

case class comm(c: Int, d: Int)
abstract class com[T](c: comm) {
  def comFunction(c: comm): String = {
    c.c.toString
  }
}
case class a(i: Int, j:Int, c1:comm) extends com[String](c1)
case class b(k:Int, l: Int, c1:comm) extends com[Int](c1)
case class bb(k:Int, l: Int, c1:comm)

trait comFun[T]{
  def dropDuP(t:T): String
}

object imps {
  /*implicit class ext(c: com[String]) {
    def call[U >: comm](cc: U) = {

      print(cc.getClass)
      println(cc)
    }
  }*/

  implicit class typ1(c: com[String])
  implicit class type2(c: com[Int])
}
object ImplicitClassTest {
  import imps._
  def main(args: Array[String]): Unit = {
    val aa = a(1, 2, comm(3,4))
    val bb1 = bb(1, 2, comm(3,4))

    println(aa.comFunction(aa.c1))
  }
}
