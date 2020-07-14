/**
 * Created by STYN on 14-07-2020
 */
object BalloonCdlty {
  final val bln = "BALLOON"

  def main(args: Array[String]): Unit = {
    println(formBalloon("BAONXXOLL"), formBalloon1("BAONXXOLL"))
    println(formBalloon("BAOOLLNNOLOLGBAX"), formBalloon1("BAOOLLNNOLOLGBAX"))
    println(formBalloon("QAWABAWONL"), formBalloon1("QAWABAWONL"))
    println(formBalloon("ONLABLABLOON"), formBalloon1("ONLABLABLOON"))
  }

  def formBalloon(ip: String): Int = {
    if (ip.count(_ == 'B') >= 1 && ip.count(_ == 'A') >= 1 && ip.count(_ == 'L') >= 2 && ip.count(_ == 'O') >= 2 && ip.count(_ == 'N') >= 1) {
      val diff = ip.diff(bln).size
      diff / 7 + (if (diff % 7 == 0) 0 else 1)
    }
    else
      0
  }

  def formBalloon1(ip: String): Int = {
    def inner(s: String, rs: Map[Char, Int]): Map[Char, Int] = {
      if (s.isEmpty)
        rs
      else
        inner(s.tail, rs + (s.head.toChar -> (rs.getOrElse(s.head, 0) + 1)))
    }

    val res = inner(ip, Map())
    val res1 = (if (res.getOrElse('B', 0) >= 1 && res.getOrElse('A', 0) >= 1 && res.getOrElse('L', 0) >= 2 &&
      res.getOrElse('O', 0) >= 2 && res.getOrElse('N', 0) >= 1) {
      res.map { x =>
        x._1 match {
          case 'B' => x._2 - 1
          case 'A' => x._2 - 1
          case 'L' => x._2 - 2
          case 'O' => x._2 - 2
          case 'N' => x._2 - 1
          case _ => x._2
        }
      }.toList
    } else {
      List(0)
    }).sum
    val fRes = if (res1 > 0) if (res1 % 7 == 0) res1 / 7 else (res1 / 7) + 1 else res1
    fRes
  }


}
