package tds.util

object CommonUtils {
  def newMap[A,B](kv: Iterable[(A, B)]): java.util.HashMap[A, B] = {
      val map = new java.util.HashMap[A, B]
      kv.foreach(i => map.put(i._1, i._2))
      map
  }
}
