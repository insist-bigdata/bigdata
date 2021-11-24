package top.damoncai.scala.chapter10

import scala.collection.mutable

object Demo_06_MergeMap {

  def main(args: Array[String]): Unit = {
    var map1 = Map("a" -> 1, "b" -> 2)
    var map2 = mutable.Map("a" -> 1, "b" -> 2,"c" -> 3)

    var map3 = map1.foldLeft(map2)(
      (mergeMap,kv) => {
        var key = kv._1
        var value = kv._2
        mergeMap(key) = mergeMap.getOrElse(key,0) + value
        mergeMap
      }
    )
    println(map3)
  }
}
