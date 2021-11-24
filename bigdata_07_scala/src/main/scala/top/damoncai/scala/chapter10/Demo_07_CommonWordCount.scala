package top.damoncai.scala.chapter10

object Demo_07_CommonWordCount {

  def main(args: Array[String]): Unit = {
    var list:List[String] = List(
      "hello java",
      "hello spark",
      "hello flink",
      "hello spark from flink",
    )

    val flatMap:List[String] = list.flatMap(_.split(" "))
    println(flatMap)

    val functionGroup:Map[String,List[String]] = flatMap.groupBy(word => word)
    println(functionGroup)

    val functionMap:Map[String,Int] = functionGroup.map(kv => (kv._1 -> kv._2.size))
    println(functionMap)

    val functionSortTake = functionMap.toList.sortWith(_._2 > _._2).take(2)
    println(functionSortTake)
  }

}
