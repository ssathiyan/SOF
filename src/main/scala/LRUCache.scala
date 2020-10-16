import scala.collection._

/**
 * Created by STYN on 12-08-2020
 */
class LRUCache[X, Y] (number: Int){
  private var cache = new mutable.LinkedHashMap[X,Y]()

  def set(key: X, value: Y): Unit = {
    cache.put(key, value)
    cache = if (cache.size > number){
      cache.takeRight(number)
    } else cache
  }

  def get(key: X): Option[Y] = {
    val v = if (cache.contains(key)){
      val v1 = cache.get(key)
      cache.remove(key)
      cache.put(key, v1.get)
      v1
    } else None
    v
  }

  def getCache() = {
    println(cache)
  }
}

object LRUCacheTest {
  def main(args: Array[String]): Unit = {
    val cache = new LRUCache[Int, Int](3)
    cache.set(1,1)
    cache.set(2,2)
    cache.set(3,3)
    cache.set(4,4)
    println(cache.get(2))
    cache.set(5,5)
    cache.getCache()
  }
}
