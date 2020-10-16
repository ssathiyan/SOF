/**
 * Created by STYN on 27-09-2020
 */

trait Add[A] {
  def apply(a: A, b: A): A
}

object Add {
  implicit val booleanAdd: Add[Boolean] = new Add[Boolean] {
    def apply(a: Boolean, b: Boolean): Boolean = !a
  }

  implicit def numericAdd[A: Numeric]: Add[A] = new Add[A] {
    def apply(a: A, b: A): A = implicitly[Numeric[A]].plus(a, b)
  }
}

object GenericExample1 {
  def main(args: Array[String]): Unit = {

    println {
      fun(1, 1)
    }
  }

  def fun[T: Add](x: T, y: T) = implicitly[Add[T]].apply(x, y)
}
