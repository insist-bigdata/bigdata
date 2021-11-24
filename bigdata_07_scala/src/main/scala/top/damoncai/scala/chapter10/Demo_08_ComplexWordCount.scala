package top.damoncai.scala.chapter10

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Demo_08_ComplexWordCount {

  def main(args: Array[String]): Unit = {
    var list:List[(String,Int)] = List(
      ("hello",1),
      ("hello world",2),
      ("hello scala",3),
      ("hello spark from scala",1),
      ("hello flink from scala",2)
    )

    // 分开
    var listFlatMap = list.flatMap(kv => {
      var keyStrArray = kv._1.split(" ")
      keyStrArray.map((_,kv._2))
    })

    //分组
    var group = listFlatMap.groupBy(kv => kv._1)

    //统计个数
    var static = group.toList.map(kv => {
      var sum = kv._2.map(_._2).sum
      (kv._1,sum)
    })

    // 排序
    var sort = static.sortWith(_._2 > _._2)
    println(sort)

    // 获取前三
    var take3 = static.take(3)
    println(take3)
  }

}
