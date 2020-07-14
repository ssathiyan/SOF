/**
 * Created by STYN on 03-07-2020
 * https://stackoverflow.com/questions/62706206/matching-a-string-of-delimited-optional-and-required-keywords
 */
object StringMatch {
  def main(args: Array[String]): Unit = {
    println(firstIndexOf("foo;bar|baz;fox", "the quick fox foo bar baz"))
    println(firstIndexOf("foo;bar|baz;lorem", "the quick fox foo bar baz"))
    println(firstIndexOf("foo;bacr|baz;phone", "the quick fox foo bar baz"))
  }

  def firstIndexOf(keyword: String, summary: String): Option[(String, Int)] = {
    val match_part = keyword.split("\\|")
    val matches = match_part.flatMap(_.split(";"))
    val ipList = summary.split(" ")
    val matched_list = ipList.map(i => (if (matches.contains(i)) i else "" , match_part.filter(_.split(";")
      .map(summary.contains(_)).reduce(_ && _)).toList))
      .filter(x => x._1 != "" && !x._2.isEmpty).toList
    val matched_list1 = matched_list.map(x => (x._1, x._2.filter(_.contains(x._1))))

    if (matched_list1.isEmpty)
      None
    else
      Some((matched_list1.head._2.head,summary.indexOf(matched_list.head._1)))
  }
}
